/* gpcontrib/anser/anser--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION anser" to load this file. \quit

/*
 * The coordinator-side edges of the Anser network transport.  Segment
 * executors call these over libpq while running a plan that carries Anser
 * runtime-filter nodes; they are not a user-facing API.
 *
 * Execution location is left at the default (EXECUTE ON ANY), even though the
 * channel map only exists in coordinator shared memory: CREATE FUNCTION
 * accepts EXECUTE ON COORDINATOR only for set-returning functions (see
 * validate_sql_exec_location() in commands/functioncmds.c).  It costs nothing
 * here -- the transport calls these as "SELECT anser.publish(...)" with no
 * FROM clause, which a coordinator backend evaluates locally -- and a call that
 * did somehow reach a segment would find no channel map and return false, i.e.
 * fail open.
 *
 * The default EXECUTE grant to PUBLIC -- and the USAGE grant on the schema
 * below -- are intentional and must not be revoked: segments connect back as
 * the query's own role, so restricting these functions would silently disable
 * runtime filtering for every non-superuser query.  Callers are confined to
 * the channels their own role created (see anserfuncs.c).
 */

GRANT USAGE ON SCHEMA anser TO PUBLIC;

CREATE FUNCTION anser.producer_begin(
    gp_session_id int4,
    gp_command_count int4,
    condition_id int4,
    condition_key text,
    expected_producers int4)
RETURNS bool
AS 'MODULE_PATHNAME', 'anser_producer_begin'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION anser.publish(
    gp_session_id int4,
    gp_command_count int4,
    condition_id int4,
    condition_key text,
    payload bytea,
    cancelled bool)
RETURNS bool
AS 'MODULE_PATHNAME', 'anser_publish'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION anser.consume_wait(
    gp_session_id int4,
    gp_command_count int4,
    condition_id int4,
    condition_key text)
RETURNS bytea
AS 'MODULE_PATHNAME', 'anser_consume_wait'
LANGUAGE C STRICT VOLATILE;
