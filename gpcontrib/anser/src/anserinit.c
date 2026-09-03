/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * anserinit.c
 *	  Module entry point: GUCs, shared memory, background services, and the
 *	  core hooks the Anser subsystem hangs off.
 *
 * Anser is a shared_preload_libraries extension.  Everything it needs from the
 * server is reached through an existing extensibility point:
 *
 *	 shmem_request_hook / shmem_startup_hook  the channel map and its LWLocks
 *	 RegisterBackgroundWorker                 the gather and send services
 *	 planner_hook                             runtime-filter injection
 *	 RegisterCustomScanMethods                the injected plan nodes
 *	 CustomAuth*_hook                         segment -> QD token connections
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserinit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anser.h"
#include "anserplan.h"
#include "cdb/cdbvars.h"
#include "libpq/auth.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static void anser_define_gucs(void);
static void anser_register_services(void);
static void anser_shmem_request(void);
static void anser_shmem_startup(void);
static PlannedStmt *anser_planner(Query *parse, const char *query_string,
								  int cursorOptions, ParamListInfo boundParams,
								  OptimizerOptions *optimizer_options);

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;

void
_PG_init(void)
{
	anser_define_gucs();

	/*
	 * Only a preloaded library can request shared memory, register background
	 * workers, or be relied on to have installed its hooks in every backend.
	 * Loaded any other way, Anser stays inert: the GUCs exist (so a stray
	 * setting is not an error) but nothing is wired up.
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = anser_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = anser_shmem_startup;

	prev_planner_hook = planner_hook;
	planner_hook = anser_planner;

	/*
	 * The producer and consumer nodes travel to the segments inside dispatched
	 * plans, so every backend must be able to resolve their CustomScan methods
	 * by name.  Registering here covers QD and QE alike.
	 */
	AnserRegisterRuntimeFilterMethods();

	if (gp_anser_enable)
	{
		/*
		 * Own the authentication of incoming segment -> QD connections.  With
		 * Anser disabled the hooks stay unset and such a connection is simply
		 * authenticated the ordinary way, through pg_hba.
		 */
		CustomAuthClaims_hook = AnserConnClaims;
		CustomAuthCheckPassword_hook = AnserConnCheckPassword;

		anser_register_services();
	}
}

/*
 * The subsystem's GUCs.  All are "anser.*"-qualified because they belong to a
 * loadable module; the C variables keep their gp_anser_ names.
 */
static void
anser_define_gucs(void)
{
	DefineCustomBoolVariable("anser.enable",
							 "Enables the Anser adaptive information sharing subsystem.",
							 "When disabled, Anser does not allocate shared memory and its background services are not started.",
							 &gp_anser_enable,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("anser.runtime_filter",
							 "Enables injection of Anser runtime bloom filters into plans.",
							 "Requires anser.enable; the plan pass is a no-op otherwise.",
							 &gp_anser_runtime_filter,
							 false,
							 PGC_USERSET,
							 GUC_EXPLAIN,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("anser.conn",
							 "Specify this is a connection for the Anser runtime filter transport.",
							 NULL,
							 &gp_anser_conn,
							 false,
							 PGC_BACKEND,
							 GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_DISALLOW_IN_FILE,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("anser.max_channels",
							"Sets the maximum number of Anser channels.",
							"This value sizes the fixed Anser shared-memory channel map at postmaster start. "
							"0 (the default) auto-sizes it to max_connections * gp_max_slices, "
							"falling back to a fixed per-connection budget when gp_max_slices is unbounded.",
							&gp_anser_max_channels,
							0, 0, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("anser.max_info_size",
							"Sets the maximum byte size of one Anser information record.",
							"Per-record DSM payload cap for Anser information.  The default holds a full 64 MB bloom-filter bitset plus its serialized-part header.",
							&gp_anser_max_info_size,
							64 * 1024 * 1024 + 1024 * 1024, 1, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("anser.timeout_ms",
							"Sets how long Anser consumers wait for producer registration.",
							"After producer registration, consumers wait for data without this timeout and rely on query cancellation or channel cancellation.",
							&gp_anser_timeout_ms,
							1000, 0, INT_MAX,
							PGC_USERSET,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("anser.max_consumers_per_channel",
							"Sets the maximum number of waiting Anser consumers per channel.",
							"This value sizes the fixed Anser consumer wait table at postmaster start "
							"(anser.max_channels * this). Each channel has one consumer per segment, "
							"so it should be set to the number of primary segments; the plan pass injects "
							"at most one consumer per channel. It cannot be auto-derived because the "
							"segment count is a catalog value unavailable at postmaster start. Over-sizing "
							"only wastes shared memory; under-sizing makes surplus consumers fail open "
							"(unfiltered), never wrong results.",
							&gp_anser_max_consumers_per_channel,
							64, 1, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("anser");
}

/*
 * Register the gather and send services.
 *
 * Both live on the coordinator only.  The decision is made here rather than in
 * a bgw_start_rule because the postmaster consults that field only for its own
 * auxiliary process list, not for workers an extension registers.  Gp_role is
 * already settled at this point: the configuration files (which carry
 * gp_contentid) are processed before shared_preload_libraries.
 */
static void
anser_register_services(void)
{
	BackgroundWorker worker;
	int			i;

	static const struct
	{
		const char *name;
		const char *main_func;
	}			services[] =
	{
		{"anser gather service", "AnserGatherServiceMain"},
		{"anser send service", "AnserSendServiceMain"}
	};

	if (!AnserStartRule((Datum) 0))
		return;

	for (i = 0; i < lengthof(services); i++)
	{
		MemSet(&worker, 0, sizeof(worker));
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = 1;
		worker.bgw_notify_pid = 0;
		snprintf(worker.bgw_name, BGW_MAXLEN, "%s", services[i].name);
		snprintf(worker.bgw_type, BGW_MAXLEN, "%s", services[i].name);
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "anser");
		snprintf(worker.bgw_function_name, BGW_MAXLEN, "%s",
				 services[i].main_func);

		RegisterBackgroundWorker(&worker);
	}
}

static void
anser_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	if (!gp_anser_enable)
		return;

	RequestAddinShmemSpace(AnserShmemSize());
	RequestNamedLWLockTranche(ANSER_LWLOCK_TRANCHE, ANSER_NUM_LWLOCKS);
}

static void
anser_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	AnserShmemInit();
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Plan the query as usual, then hand the finished tree to the runtime-filter
 * pass.  Wrapping the hook this way covers both optimizers, because ORCA is
 * dispatched from inside standard_planner().
 */
static PlannedStmt *
anser_planner(Query *parse, const char *query_string, int cursorOptions,
			  ParamListInfo boundParams, OptimizerOptions *optimizer_options)
{
	PlannedStmt *result;

	if (prev_planner_hook)
		result = prev_planner_hook(parse, query_string, cursorOptions,
								   boundParams, optimizer_options);
	else
		result = standard_planner(parse, query_string, cursorOptions,
								  boundParams, optimizer_options);

	AnserApplyRuntimeFilters(result);

	return result;
}
