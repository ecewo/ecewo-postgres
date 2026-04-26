#ifndef ECEWO_PG_H
#define ECEWO_PG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include "ecewo.h"
#include "ecewo-pg-export.h"

/** Opaque connection pool. Create with ecewo_pg_pool_create(), destroy with ecewo_pg_pool_destroy(). */
typedef struct ecewo_pg_pool_s ecewo_pg_pool_t;

/** Opaque pool configuration. Build with ecewo_pg_pool_config_new() + setters, pass to
 *  ecewo_pg_pool_create(), then free with ecewo_pg_pool_config_free(). */
typedef struct ecewo_pg_pool_config_s ecewo_pg_pool_config_t;

/** Opaque query handle. Owns an arena, a connection, and a queue of SQL statements.
 *  Create with ecewo_pg_query_create(), populate with ecewo_pg_query_queue(),
 *  then finalize with ecewo_pg_query_exec() or ecewo_pg_query_exec_trans(). */
typedef struct ecewo_pg_query_s ecewo_pg_query_t;

/** Opaque parallel execution context. Holds N independent query streams that run concurrently. */
typedef struct ecewo_pg_parallel_s ecewo_pg_parallel_t;

/** Opaque query result. Handed to every result callback; valid only for the duration of that callback.
 *  Access rows and columns via the ecewo_pg_result_*() accessors. */
typedef struct ecewo_pg_result_s ecewo_pg_result_t;

/** Called once per query result as it arrives. `result` is valid only during this call. */
typedef void (*ecewo_pg_result_cb_t)(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data);

/** Called after all queries on a query handle have finished (success or failure). */
typedef void (*ecewo_pg_complete_cb_t)(ecewo_pg_query_t *pg, void *data);

/** Called after every stream in a parallel context completes.
 *  `success` is 1 when every stream succeeded, 0 if any failed. */
typedef void (*ecewo_pg_parallel_cb_t)(ecewo_pg_parallel_t *parallel, int success, void *data);

// ---------------------------------------------------------------------------
// POOL CONFIGURATION
// ---------------------------------------------------------------------------

/** Allocate a new, zeroed pool config. Returns NULL on allocation failure. */
ECEWO_PG_EXPORT ecewo_pg_pool_config_t *ecewo_pg_pool_config_new(void);

/** Free a config created by ecewo_pg_pool_config_new(). Safe to call after ecewo_pg_pool_create(). */
ECEWO_PG_EXPORT void ecewo_pg_pool_config_free(ecewo_pg_pool_config_t *config);

/** Set the ecewo application this pool is attached to. Required. */
ECEWO_PG_EXPORT void ecewo_pg_pool_config_set_app(ecewo_pg_pool_config_t *config, ecewo_app_t *app);

/** Set the PostgreSQL host (e.g. "localhost"). String is copied on ecewo_pg_pool_create(). */
ECEWO_PG_EXPORT void ecewo_pg_pool_config_set_host(ecewo_pg_pool_config_t *config, const char *host);

/** Set the PostgreSQL port as a string (e.g. "5432"). */
ECEWO_PG_EXPORT void ecewo_pg_pool_config_set_port(ecewo_pg_pool_config_t *config, const char *port);

/** Set the database name. */
ECEWO_PG_EXPORT void ecewo_pg_pool_config_set_dbname(ecewo_pg_pool_config_t *config, const char *dbname);

/** Set the authenticating user. */
ECEWO_PG_EXPORT void ecewo_pg_pool_config_set_user(ecewo_pg_pool_config_t *config, const char *user);

/** Set the user password. */
ECEWO_PG_EXPORT void ecewo_pg_pool_config_set_password(ecewo_pg_pool_config_t *config, const char *password);

/** Set the number of physical connections maintained by the pool. Must be in [1, 1024]. */
ECEWO_PG_EXPORT void ecewo_pg_pool_config_set_pool_size(ecewo_pg_pool_config_t *config, int pool_size);

/** Set how long a requester may wait for a free connection, in milliseconds.
 *  0 = do not wait, -1 = wait indefinitely. */
ECEWO_PG_EXPORT void ecewo_pg_pool_config_set_timeout_ms(ecewo_pg_pool_config_t *config, int timeout_ms);

// ---------------------------------------------------------------------------
// POOL LIFECYCLE
// ---------------------------------------------------------------------------

/** Create a connection pool from a populated config. Opens `pool_size` physical connections
 *  eagerly; returns NULL if none can be established. The config can be freed immediately after. */
ECEWO_PG_EXPORT ecewo_pg_pool_t *ecewo_pg_pool_create(const ecewo_pg_pool_config_t *config);

/** Close all connections and free the pool. Pending waiters receive a NULL connection. */
ECEWO_PG_EXPORT void ecewo_pg_pool_destroy(ecewo_pg_pool_t *pool);

/** Return the total number of connection slots configured on the pool. */
ECEWO_PG_EXPORT int ecewo_pg_pool_total(ecewo_pg_pool_t *pool);

/** Return the number of connection slots currently idle and ready to borrow. */
ECEWO_PG_EXPORT int ecewo_pg_pool_available(ecewo_pg_pool_t *pool);

/** Return the number of connections currently checked out by running queries. */
ECEWO_PG_EXPORT int ecewo_pg_pool_in_use(ecewo_pg_pool_t *pool);

/** Close any connection that has been idle longer than `max_idle_ms`.
 *  Returns the number of connections closed. Connections are lazily re-created on demand. */
ECEWO_PG_EXPORT int ecewo_pg_pool_cleanup_idle(ecewo_pg_pool_t *pool, uint64_t max_idle_ms);

// ---------------------------------------------------------------------------
// QUERY BUILDER
// ---------------------------------------------------------------------------

/** Start building a query attached to the given response. `res` may be NULL for background work.
 *  The handle is freed automatically once the query chain completes. */
ECEWO_PG_EXPORT ecewo_pg_query_t *ecewo_pg_query_create(ecewo_pg_pool_t *pool, ecewo_response_t *res);

/** Register a callback to run after the entire query chain has settled (success or failure).
 *  Typical place to send the HTTP response. */
ECEWO_PG_EXPORT void ecewo_pg_query_on_complete(ecewo_pg_query_t *pg,
                                                ecewo_pg_complete_cb_t callback,
                                                void *data);

/** Append a SQL statement to the query queue.
 *  `params` may be NULL when `param_count` is 0. Strings are copied into the query arena.
 *  `result_cb` is invoked once per returned result (valid only inside the callback).
 *  Returns 0 on success, -1 on failure. */
ECEWO_PG_EXPORT int ecewo_pg_query_queue(ecewo_pg_query_t *pg,
                                         const char *sql,
                                         int param_count,
                                         const char **params,
                                         ecewo_pg_result_cb_t result_cb,
                                         void *query_data);

/** Execute all queued statements sequentially on a single connection.
 *  Returns 0 when execution has been scheduled, -1 on immediate error. */
ECEWO_PG_EXPORT int ecewo_pg_query_exec(ecewo_pg_query_t *pg);

/** Execute all queued statements inside an implicit transaction.
 *  Wraps the chain in BEGIN/COMMIT and rolls back if any statement fails. */
ECEWO_PG_EXPORT int ecewo_pg_query_exec_trans(ecewo_pg_query_t *pg);

// ---------------------------------------------------------------------------
// PARALLEL EXECUTION
// ---------------------------------------------------------------------------

/** Create a parallel context with `count` independent streams, each running on its own connection. */
ECEWO_PG_EXPORT ecewo_pg_parallel_t *ecewo_pg_parallel_create(ecewo_pg_pool_t *pool,
                                                              int count,
                                                              ecewo_response_t *res);

/** Return the query handle for stream `index`. Populate it with ecewo_pg_query_queue(). */
ECEWO_PG_EXPORT ecewo_pg_query_t *ecewo_pg_parallel_get(ecewo_pg_parallel_t *parallel, int index);

/** Register a callback invoked once every stream has completed. */
ECEWO_PG_EXPORT void ecewo_pg_parallel_on_complete(ecewo_pg_parallel_t *parallel,
                                                   ecewo_pg_parallel_cb_t callback,
                                                   void *data);

/** Start all streams. Each acquires its own pool connection and runs concurrently. */
ECEWO_PG_EXPORT int ecewo_pg_parallel_exec(ecewo_pg_parallel_t *parallel);

/** Return the number of streams configured on the parallel context. */
ECEWO_PG_EXPORT int ecewo_pg_parallel_count(ecewo_pg_parallel_t *parallel);

// ---------------------------------------------------------------------------
// RESULT ACCESSORS
// ---------------------------------------------------------------------------

/** Return true when the result represents a successful command or row set. */
ECEWO_PG_EXPORT bool ecewo_pg_result_ok(const ecewo_pg_result_t *result);

/** Return the number of rows in the result, or 0 when the command produced none. */
ECEWO_PG_EXPORT int ecewo_pg_result_ntuples(const ecewo_pg_result_t *result);

/** Return the number of columns in the result. */
ECEWO_PG_EXPORT int ecewo_pg_result_nfields(const ecewo_pg_result_t *result);

/** Return the column name at `field`, or NULL if the index is out of range.
 *  The returned pointer is owned by the result and invalidated when the callback returns. */
ECEWO_PG_EXPORT const char *ecewo_pg_result_field_name(const ecewo_pg_result_t *result, int field);

/** Return the text value at (row, field). NULL cells are reported as empty strings;
 *  use ecewo_pg_result_is_null() to distinguish. */
ECEWO_PG_EXPORT const char *ecewo_pg_result_get_value(const ecewo_pg_result_t *result, int row, int field);

/** Return the byte length of the value at (row, field). */
ECEWO_PG_EXPORT int ecewo_pg_result_get_length(const ecewo_pg_result_t *result, int row, int field);

/** Return true when the cell at (row, field) is SQL NULL. */
ECEWO_PG_EXPORT bool ecewo_pg_result_is_null(const ecewo_pg_result_t *result, int row, int field);

/** Return the error message for a failed result, or an empty string when there is none. */
ECEWO_PG_EXPORT const char *ecewo_pg_result_error_message(const ecewo_pg_result_t *result);

/** Return the command tag (e.g. "INSERT 0 1") of the result, or an empty string when absent. */
ECEWO_PG_EXPORT const char *ecewo_pg_result_cmd_tuples(const ecewo_pg_result_t *result);

#ifdef __cplusplus
}
#endif

#endif
