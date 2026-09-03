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
 * anserauth.c
 *	  Authentication of the segment -> coordinator Anser connections.
 *
 * Two halves of one mechanism.  The QD side owns a shared-memory hash of
 * per-session tokens, handed out at plan time (AnserGetOrCreateSessionToken)
 * and verified when a segment presents one (AnserSessionTokenIsValid).  The
 * backend side supplies the two functions _PG_init installs as the server's
 * custom-authentication hooks: AnserConnClaims recognizes an Anser connection
 * from its startup marker, and AnserConnCheckPassword accepts or rejects the
 * token it sends as password.  The wire exchange itself stays in
 * libpq/auth.c -- see the CustomAuth*_hook comments there.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserauth.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "anser.h"
#include "cdb/cdbvars.h"
#include "common/hashfn.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

#define ANSER_TOKEN_HASH_NAME		"Anser Session Token Hash"

/*
 * Session token hash.
 *
 * Remote (segment) producers/consumers authenticate their libpq connection to
 * the QD with a per-session random token instead of relying on pg_hba entries
 * covering the segment hosts -- the parallel-retrieve-cursor model (see
 * retrieve_conn_authentication in libpq/auth.c).  The QD registers one token
 * per (gp_session_id, session user) when a plan gets its first injected
 * runtime filter and embeds the token in the dispatched plan; the segment
 * connects with anser.conn=true and presents the token as the password,
 * and AnserSessionTokenIsValid() verifies it here.  Entries are removed when
 * the owning QD session exits.
 */
#define ANSER_TOKEN_BYTES		16	/* 128 bits, as ENDPOINT_TOKEN_ARR_LEN */
#define ANSER_TOKEN_HEX_LEN		(ANSER_TOKEN_BYTES * 2)

/* Token hash key: one token per (gp_session_id, session user). */
typedef struct AnserTokenTag
{
	int			session_id;
	Oid			user_id;
} AnserTokenTag;

/* Token hash entry: the hex-encoded random token registered by a session. */
typedef struct AnserTokenEntry
{
	AnserTokenTag tag;
	char		token_hex[ANSER_TOKEN_HEX_LEN + 1];
} AnserTokenEntry;

static HTAB *AnserTokenHash = NULL;

/* Set once this backend has registered its session-token cleanup hook. */
static bool anser_token_exit_registered = false;

static bool AnserAuthInitialized(void);
static void AnserInitializeTokenHash(void);
static void AnserTokenSessionCleanup(int code, Datum arg);

/*
 * Client authentication for incoming segment -> QD connections.
 */
static bool AnserConnMarkedInCmdOptions(char *cmd_options);
static bool AnserConnMarkedInGucOptions(List *guc_options);

/*
 * Shared-memory sizing and setup for the session-token hash, called from
 * AnserShmemSize() / AnserShmemInit() so all Anser shared state is requested
 * and created in one place.
 */
Size
AnserAuthShmemSize(void)
{
	return hash_estimate_size(MaxConnections, sizeof(AnserTokenEntry));
}

void
AnserAuthShmemInit(void)
{
	AnserInitializeTokenHash();
}

/*
 * Is the token hash usable?  Mirrors AnserInitialized() in anser.c for the
 * state this file owns; AnserChannelLock guards the hash and is resolved in
 * AnserShmemInit().
 */
static bool
AnserAuthInitialized(void)
{
	return gp_anser_enable && AnserChannelLock != NULL &&
		AnserTokenHash != NULL;
}

static void
AnserInitializeTokenHash(void)
{
	HASHCTL		hctl;

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(AnserTokenTag);
	hctl.entrysize = sizeof(AnserTokenEntry);
	hctl.hash = tag_hash;

	/* One entry per concurrent session; removed when the session exits. */
	AnserTokenHash = ShmemInitHash(ANSER_TOKEN_HASH_NAME,
								   MaxConnections,
								   MaxConnections,
								   &hctl,
								   HASH_ELEM | HASH_FUNCTION);
}

/*
 * Drop this session's token entry at backend exit.  Registered once by the
 * first AnserGetOrCreateSessionToken() call in the backend.
 */
static void
AnserTokenSessionCleanup(int code, Datum arg)
{
	AnserTokenTag tag;

	if (AnserTokenHash == NULL)
		return;

	tag.session_id = gp_session_id;
	tag.user_id = DatumGetObjectId(arg);

	LWLockAcquire(AnserChannelLock, LW_EXCLUSIVE);
	(void) hash_search(AnserTokenHash, &tag, HASH_REMOVE, NULL);
	LWLockRelease(AnserChannelLock);
}

/*
 * AnserGetOrCreateSessionToken
 *
 * Return this session's token (palloc'd hex string), generating and
 * registering it on first use.  NULL when the subsystem is off, the user id
 * is invalid, or the token hash is full -- callers fail open (connect without
 * the token, i.e. fall back to pg_hba-driven authentication).
 *
 * user_id must be the *session* user: segment executors connect back to the
 * QD as the session user (cdbconn passes MyProcPort->user_name), regardless
 * of any SET ROLE in effect on the QD.
 */
char *
AnserGetOrCreateSessionToken(Oid user_id)
{
	AnserTokenTag tag;
	AnserTokenEntry *entry;
	bool		found;
	char		token_hex[ANSER_TOKEN_HEX_LEN + 1];
	bool		have_token = false;

	if (!AnserAuthInitialized() || !OidIsValid(user_id))
		return NULL;

	tag.session_id = gp_session_id;
	tag.user_id = user_id;

	/* Copy the token into a stack buffer: no palloc while holding the lock. */
	LWLockAcquire(AnserChannelLock, LW_EXCLUSIVE);
	entry = (AnserTokenEntry *) hash_search(AnserTokenHash, &tag,
											HASH_ENTER, &found);
	if (entry != NULL)
	{
		if (!found)
		{
			uint8		token[ANSER_TOKEN_BYTES];

			if (!pg_strong_random(token, ANSER_TOKEN_BYTES))
			{
				(void) hash_search(AnserTokenHash, &tag, HASH_REMOVE, NULL);
				entry = NULL;
			}
			else
			{
				hex_encode((const char *) token, ANSER_TOKEN_BYTES,
						   entry->token_hex);
				entry->token_hex[ANSER_TOKEN_HEX_LEN] = '\0';
			}
		}
		if (entry != NULL)
		{
			strlcpy(token_hex, entry->token_hex, sizeof(token_hex));
			have_token = true;
		}
	}
	LWLockRelease(AnserChannelLock);

	if (!have_token)
		return NULL;

	if (!anser_token_exit_registered)
	{
		anser_token_exit_registered = true;
		before_shmem_exit(AnserTokenSessionCleanup, ObjectIdGetDatum(user_id));
	}

	return pstrdup(token_hex);
}

/*
 * AnserSessionTokenIsValid
 *
 * Token check behind AnserConnCheckPassword(): true iff some live session of
 * this exact user registered this token.  Runs before InitPostgres in the
 * accepting backend; shared-memory pointers are inherited from the postmaster,
 * so no attach is needed.
 */
bool
AnserSessionTokenIsValid(Oid user_id, const char *token_hex)
{
	HASH_SEQ_STATUS status;
	AnserTokenEntry *entry;
	bool		valid = false;

	if (!AnserAuthInitialized() || !OidIsValid(user_id) || token_hex == NULL ||
		strlen(token_hex) != ANSER_TOKEN_HEX_LEN)
		return false;

	LWLockAcquire(AnserChannelLock, LW_SHARED);
	hash_seq_init(&status, AnserTokenHash);
	while ((entry = (AnserTokenEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->tag.user_id == user_id &&
			strcmp(entry->token_hex, token_hex) == 0)
		{
			valid = true;
			hash_seq_term(&status);
			break;
		}
	}
	LWLockRelease(AnserChannelLock);

	return valid;
}

/*
 * CustomAuthClaims_hook: is this an Anser backward (segment -> QD) connection?
 *
 * The client marks the connection with anser.conn=true in its startup
 * packet, either as a command-line option or as a GUC option, so both sources
 * are checked -- the same pair of tests the parallel-retrieve-cursor path in
 * libpq/auth.c makes for gp_retrieve_conn.
 */
bool
AnserConnClaims(Port *port)
{
	if (port == NULL)
		return false;

	return AnserConnMarkedInCmdOptions(port->cmdline_options) ||
		AnserConnMarkedInGucOptions(port->guc_options);
}

/*
 * CustomAuthCheckPassword_hook: the password of an Anser connection is the
 * per-session token the QD handed to the segment in the plan.  The connecting
 * user must be the session user that registered it.
 */
bool
AnserConnCheckPassword(Port *port, const char *passwd)
{
	Oid			owner_uid;

	if (port == NULL || passwd == NULL)
		return false;

	owner_uid = get_role_oid(port->user_name, false);

	return AnserSessionTokenIsValid(owner_uid, passwd);
}

/*
 * Return true if the command line contains anser.conn=true.  Mirrors
 * cmd_options_include_retrieve_conn() in libpq/auth.c.
 */
static bool
AnserConnMarkedInCmdOptions(char *cmd_options)
{
	char	  **av;
	int			maxac;
	int			ac;
	int			flag;
	bool		ret = false;

	if (!cmd_options)
		return false;

	maxac = 2 + (strlen(cmd_options) + 1) / 2;

	av = (char **) palloc(maxac * sizeof(char *));
	ac = 0;

	av[ac++] = "dummy";

	pg_split_opts(av, &ac, cmd_options);

	av[ac] = NULL;

#ifdef HAVE_INT_OPTERR
	opterr = 0;
#endif

	while ((flag = getopt(ac, av, "c:-:")) != -1)
	{
		switch (flag)
		{
			case 'c':
			case '-':
				{
					char	   *name,
							   *value;

					ParseLongOption(optarg, &name, &value);
					if (!value)
					{
						if (flag == '-')
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("--%s requires a value",
											optarg)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("-c %s requires a value",
											optarg)));
					}

					if ((guc_name_compare(name, "anser.conn") == 0) &&
						!parse_bool(value, &ret))
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("invalid value for guc anser.conn: \"%s\"",
										value)));
					}

					pfree(name);
					pfree(value);
					break;
				}

			default:
				break;
		}
	}

	/*
	 * Reset getopt(3) library so that it will work correctly in subprocesses
	 * or when this function is called a second time with another array.
	 */
	optind = 1;
#ifdef HAVE_INT_OPTRESET
	optreset = 1;	/* some systems need this too */
#endif

	return ret;
}

/*
 * Return true if startup GUC options contain anser.conn=true.  Mirrors
 * guc_options_include_retrieve_conn() in libpq/auth.c.
 */
static bool
AnserConnMarkedInGucOptions(List *guc_options)
{
	ListCell   *gucopts;
	bool		ret = false;

	gucopts = list_head(guc_options);
	while (gucopts)
	{
		char	   *name;
		char	   *value;

		name = lfirst(gucopts);
		gucopts = lnext(guc_options, gucopts);

		value = lfirst(gucopts);
		gucopts = lnext(guc_options, gucopts);

		if (guc_name_compare(name, "anser.conn") == 0)
		{
			/* Do not break in case there are more than one such option. */
			if (!parse_bool(value, &ret))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid value for guc anser.conn: \"%s\"",
								value)));
		}
	}

	return ret;
}
