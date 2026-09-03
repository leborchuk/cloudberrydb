/*-------------------------------------------------------------------------
 *
 * auth.h
 *	  Definitions for network authentication routines
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/auth.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTH_H
#define AUTH_H

#include "libpq/libpq-be.h"
#include "utils/date.h"

extern PGDLLIMPORT char *pg_krb_server_keyfile;
extern PGDLLIMPORT bool pg_krb_caseins_users;
extern PGDLLIMPORT bool pg_gss_accept_delegation;

extern void ClientAuthentication(Port *port);
extern void FakeClientAuthentication(Port *port);  /* GPDB only */
extern void sendAuthRequest(Port *port, AuthRequest areq, const char *extradata,
							int extralen);

/* Hook for plugins to get control in ClientAuthentication() */
typedef void (*ClientAuthentication_hook_type) (Port *, int);
extern PGDLLIMPORT ClientAuthentication_hook_type ClientAuthentication_hook;

/*
 * Hooks for an extension that maintains its own internal connections, such as
 * a segment -> coordinator connection carrying a per-session token.  The claims
 * hook is consulted before pg_hba.conf and answers whether this connection
 * belongs to the extension, normally by looking for a marker option in
 * port->cmdline_options / port->guc_options.  When it claims the connection,
 * the backend asks the client for a password and hands it to the check hook,
 * which returns true if the connection may proceed as port->user_name.  The
 * wire exchange stays in auth.c; the extension only supplies the two answers.
 */
typedef bool (*CustomAuthClaims_hook_type) (Port *port);
extern PGDLLIMPORT CustomAuthClaims_hook_type CustomAuthClaims_hook;
typedef bool (*CustomAuthCheckPassword_hook_type) (Port *port,
												   const char *passwd);
extern PGDLLIMPORT CustomAuthCheckPassword_hook_type CustomAuthCheckPassword_hook;

/*
 * Support for time-based authentication
 *  
 * Used by auth.c for comparing current time to the contents of 
 * pg_auth_time_constraint for acl enforcement
 * Used by user.c for comparing incoming changes to the contents of
 * pg_auth_time_constraint for acl modification
 */
typedef struct authPoint
{
    int16 day;
    TimeADT time;
} authPoint;

typedef struct authInterval
{
    authPoint start;
    authPoint end;
} authInterval;

extern void timestamptz_to_point(TimestampTz in, authPoint *out);
extern int point_cmp(const authPoint *a, const authPoint *b);
extern bool interval_overlap(const authInterval *a, const authInterval *b);
extern bool interval_contains(const authInterval *interval, const authPoint *point);
extern int CheckAuthTimeConstraints(char *rolname);
extern int check_auth_time_constraints_internal(char *rolname, TimestampTz timestamp);
/* hook type for password manglers */
typedef char *(*auth_password_hook_typ) (char *input);

/* Default LDAP password mutator hook, can be overridden by a shared library */
extern PGDLLIMPORT auth_password_hook_typ ldap_password_hook;

#endif							/* AUTH_H */
