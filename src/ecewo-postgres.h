#ifndef ECEWO_POSTGRES_H
#define ECEWO_POSTGRES_H

#ifdef __cplusplus
extern "C" {
#endif

#include "ecewo.h"
#include "libpq-fe.h"

typedef struct pg_async_s PGquery;
typedef struct pg_query_s pg_query_t;
typedef struct pg_pool_s PGpool;
typedef struct pg_parallel_s PGparallel;

// Consider that it may be better if
// we remove PGquery from parameters
// and wait for it in a context
// as like as we do for Res.
// Because we don't need it all time,
// just like we don't need Res.
typedef void (*pg_result_cb_t)(PGquery *pg, PGresult *result, void *data);

typedef void (*pg_complete_cb_t)(PGquery *pg, void *data);
typedef void (*pg_parallel_cb_t)(PGparallel *parallel, int success, void *data);

typedef struct {
  const char *host;
  const char *port;
  const char *dbname;
  const char *user;
  const char *password;
  int pool_size;
  int timeout_ms; // 0 = no wait, -1 = infinite
} PGPoolConfig;

typedef struct {
  int total;
  int available;
  int in_use;
} PGPoolStats;

PGpool *pg_pool_create(PGPoolConfig *config);
void pg_pool_destroy(PGpool *pool);

PGconn *pg_pool_borrow(PGpool *pool);
void pg_pool_return(PGpool *pool, PGconn *conn);

void pg_pool_get_stats(PGpool *pool, PGPoolStats *stats);
int pg_pool_cleanup_idle(PGpool *pool, uint64_t max_idle_ms);

PGquery *pg_query_create(PGpool *pool, Arena *arena);
void pg_query_on_complete(PGquery *pg, pg_complete_cb_t callback, void *data);

int pg_query_queue(PGquery *pg,
                   const char *sql,
                   int param_count,
                   const char **params,
                   pg_result_cb_t result_cb,
                   void *query_data);

int pg_query_exec(PGquery *pg);

// Execute with automatic transaction handling
// Wraps all queued queries in BEGIN/COMMIT
// Automatically rolls back on any query failure
int pg_query_exec_trans(PGquery *pg);

// Create parallel execution context
// count is number of parallel query streams
PGparallel *pg_parallel_create(PGpool *pool, int count, Arena *arena);

// Get a query stream by index
// Each stream executes on its own connection
PGquery *pg_parallel_get(PGparallel *parallel, int index);

// Set completion callback - called when ALL streams complete
// success is 1 if all succeeded, 0 if any failed
void pg_parallel_on_complete(PGparallel *parallel, pg_parallel_cb_t callback, void *data);

// Execute all parallel streams
// Each stream acquires its own connection and runs independently
int pg_parallel_exec(PGparallel *parallel);

// Get the number of streams in a parallel context
int pg_parallel_count(PGparallel *parallel);

#ifdef __cplusplus
}
#endif

#endif
