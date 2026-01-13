#include "ecewo-postgres.h"
#include "uv.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

#define LOG_ERROR(fmt, ...) \
  fprintf(stderr, "[ERROR] [ecewo-postgres] " fmt "\n", ##__VA_ARGS__)

struct pg_query_s {
  char *sql;
  char **params;
  uint8_t param_count;
  pg_result_cb_t result_cb;
  void *data;
  pg_query_t *next;
};

struct pg_async_s {
  PGconn *conn;
  Arena *arena;
  void *data;

  bool is_connected;
  bool is_executing;
  bool in_callback;

  pg_query_t *query_queue;
  pg_query_t *query_queue_tail;
  pg_query_t *current_query;

  // Pool integration
  PGpool *pool; // If non-NULL, release connection on completion

  // Arena management
  bool arena_owned; // If true, we borrowed it and must return it

  // Completion callback
  pg_complete_cb_t on_complete;
  void *complete_data;

  // Parallel execution support
  PGparallel *parallel; // If non-NULL, part of parallel execution
  int parallel_index; // Index in parallel context

  bool handle_initialized;
#ifdef _WIN32
  uv_timer_t timer;
#else
  uv_poll_t poll;
#endif
};

typedef struct {
  PGconn *conn;
  bool in_use;
  uint64_t last_used;
} pool_connection_t;

typedef struct pool_wait_s {
  void (*callback)(PGconn *conn, void *data);
  void *data;
  struct pool_wait_s *next;
  uv_timer_t *timeout_timer;
  bool cancelled;
} pool_wait_t;

struct pg_pool_s {
  pool_connection_t *connections;
  uint16_t size;
  int timeout_ms;
  char *conninfo;
  uv_mutex_t mutex;
  pool_wait_t *wait_queue_head;
  pool_wait_t *wait_queue_tail;
  uint16_t wait_count;
  bool destroyed;
};

struct pg_parallel_s {
  PGpool *pool;
  Arena *arena;
  bool arena_owned;
  PGquery **streams;
  PGconn **conns;
  uint8_t count;
  uint8_t completed;
  uint8_t started;
  pg_parallel_cb_t on_complete;
  void *complete_data;
  uv_mutex_t mutex;
};

typedef struct {
  PGconn *conn;
  uv_poll_t poll;
  PGpool *pool;
  void (*callback)(PGconn *conn, void *data);
  void *data;
  uint64_t start_time;
  bool connecting;
} async_connect_ctx_t;

static void execute_next_query(PGquery *pg);
static void cleanup_and_destroy(PGquery *pg);
static void on_parallel_stream_complete(PGquery *pg, void *data);

#ifdef _WIN32
static void on_timer(uv_timer_t *handle);
#else
static void on_poll(uv_poll_t *handle, int status, int events);
#endif

typedef struct {
  uv_timer_t timer;
  pool_wait_t *waiter;
  PGpool *pool;
} pool_timeout_ctx_t;

static void on_pool_timeout(uv_timer_t *handle) {
  pool_timeout_ctx_t *ctx = handle->data;

  uv_mutex_lock(&ctx->pool->mutex);

  if (ctx->waiter->cancelled) {
    uv_mutex_unlock(&ctx->pool->mutex);
    uv_close((uv_handle_t *)handle, (uv_close_cb)free);
    return;
  }

  ctx->waiter->cancelled = true;

  pool_wait_t *prev = NULL;
  pool_wait_t *curr = ctx->pool->wait_queue_head;

  while (curr) {
    if (curr == ctx->waiter) {
      if (prev) {
        prev->next = curr->next;
      } else {
        ctx->pool->wait_queue_head = curr->next;
      }

      if (ctx->pool->wait_queue_tail == curr)
        ctx->pool->wait_queue_tail = prev;

      ctx->pool->wait_count--;
      break;
    }
    prev = curr;
    curr = curr->next;
  }

  uv_mutex_unlock(&ctx->pool->mutex);
  decrement_async_work();

  ctx->waiter->callback(NULL, ctx->waiter->data);
  free(ctx->waiter);

  uv_close((uv_handle_t *)handle, (uv_close_cb)free);
}

static char *build_conninfo(PGPoolConfig *config) {
  size_t len = snprintf(NULL, 0,
                        "host=%s port=%s dbname=%s user=%s password=%s",
                        config->host, config->port, config->dbname,
                        config->user, config->password)
      + 1;

  char *conninfo = malloc(len);
  if (!conninfo)
    return NULL;

  snprintf(conninfo, len,
           "host=%s port=%s dbname=%s user=%s password=%s",
           config->host, config->port, config->dbname,
           config->user, config->password);

  return conninfo;
}

typedef struct {
  PGconn *conn;
  uv_poll_t poll;
  void (*callback)(PGconn *conn, void *data);
  void *data;
} async_reset_ctx_t;

static void on_reset_ready(uv_poll_t *handle, int status, int events) {
  (void)status;
  (void)events;

  async_reset_ctx_t *ctx = handle->data;

  if (!server_is_running()) {
    uv_poll_stop(&ctx->poll);
    ctx->callback(NULL, ctx->data);
    free(ctx);
    return;
  }

  PostgresPollingStatusType poll_status = PQresetPoll(ctx->conn);

  switch (poll_status) {
  case PGRES_POLLING_OK:
    uv_poll_stop(&ctx->poll);
    uv_close((uv_handle_t *)&ctx->poll, NULL);

    if (PQsetnonblocking(ctx->conn, 1) != 0) {
      LOG_ERROR("Failed to set non-blocking after reset");
      ctx->callback(NULL, ctx->data);
    } else {
      ctx->callback(ctx->conn, ctx->data);
    }
    free(ctx);
    break;

  case PGRES_POLLING_READING:
    uv_poll_start(&ctx->poll, UV_READABLE, on_reset_ready);
    break;

  case PGRES_POLLING_WRITING:
    uv_poll_start(&ctx->poll, UV_WRITABLE, on_reset_ready);
    break;

  case PGRES_POLLING_FAILED:
  default:
    uv_poll_stop(&ctx->poll);
    uv_close((uv_handle_t *)&ctx->poll, NULL);
    ctx->callback(NULL, ctx->data);
    free(ctx);
    break;
  }
}

static void pg_async_reset(PGconn *conn,
                           void (*callback)(PGconn *, void *),
                           void *data) {
  if (!PQresetStart(conn)) {
    callback(NULL, data);
    return;
  }

  async_reset_ctx_t *ctx = malloc(sizeof(async_reset_ctx_t));
  if (!ctx) {
    callback(NULL, data);
    return;
  }

  ctx->conn = conn;
  ctx->callback = callback;
  ctx->data = data;

  int sock = PQsocket(conn);
  if (uv_poll_init(get_loop(), &ctx->poll, sock) != 0) {
    free(ctx);
    callback(NULL, data);
    return;
  }

  ctx->poll.data = ctx;
  on_reset_ready(&ctx->poll, 0, 0);
}

static PGconn *pg_connect(const char *conninfo) {
  PGconn *conn = PQconnectdb(conninfo); // Blocking only at startup

  if (!conn || PQstatus(conn) != CONNECTION_OK) {
    if (conn) {
      LOG_ERROR("Connection failed: %s", PQerrorMessage(conn));
      PQfinish(conn);
    }
    return NULL;
  }

  // Non-blocking for future queries
  if (PQsetnonblocking(conn, 1) != 0) {
    LOG_ERROR("Failed to set non-blocking mode");
    PQfinish(conn);
    return NULL;
  }

  return conn;
}

PGpool *pg_pool_create(PGPoolConfig *config) {
  if (!config || config->pool_size <= 0 || config->pool_size > 1024) {
    LOG_ERROR("Invalid pool configuration");
    return NULL;
  }

  PGpool *pool = malloc(sizeof(PGpool));
  if (!pool) {
    LOG_ERROR("Failed to allocate pool");
    return NULL;
  }

  memset(pool, 0, sizeof(PGpool));

  pool->conninfo = build_conninfo(config);
  if (!pool->conninfo) {
    free(pool);
    return NULL;
  }

  pool->size = config->pool_size;
  pool->timeout_ms = config->timeout_ms;
  pool->destroyed = false;
  pool->wait_queue_head = NULL;
  pool->wait_queue_tail = NULL;
  pool->wait_count = 0;

  if (uv_mutex_init(&pool->mutex) != 0) {
    LOG_ERROR("Failed to initialize mutex");
    free(pool->conninfo);
    free(pool);
    return NULL;
  }

  pool->connections = calloc(pool->size, sizeof(pool_connection_t));
  if (!pool->connections) {
    LOG_ERROR("Failed to allocate connections array");
    uv_mutex_destroy(&pool->mutex);
    free(pool->conninfo);
    free(pool);
    return NULL;
  }

  int connected = 0;
  for (int i = 0; i < pool->size; i++) {
    PGconn *conn = pg_connect(pool->conninfo);

    if (conn) {
      pool->connections[i].conn = conn;
      pool->connections[i].in_use = false;
      pool->connections[i].last_used = 0;
      connected++;
    } else {
      LOG_ERROR("Failed to create pool connection %d", i);
    }
  }

  if (connected == 0) {
    LOG_ERROR("No connections could be established");
    pg_pool_destroy(pool);
    return NULL;
  }

#ifdef ECEWO_DEBUG
  if (connected < pool->size) {
    printf("Pool created with %d/%d connections\n", connected, pool->size);
  }
#endif

  return pool;
}

void pg_pool_destroy(PGpool *pool) {
  if (!pool)
    return;

  uv_mutex_lock(&pool->mutex);
  pool->destroyed = true;

  // Reject all pending waiters
  pool_wait_t *waiter = pool->wait_queue_head;
  while (waiter) {
    pool_wait_t *next = waiter->next;
    waiter->callback(NULL, waiter->data);
    free(waiter);
    waiter = next;
  }
  pool->wait_queue_head = NULL;
  pool->wait_queue_tail = NULL;

  for (int i = 0; i < pool->size; i++) {
    if (pool->connections[i].conn) {
      PQfinish(pool->connections[i].conn);
      pool->connections[i].conn = NULL;
    }
  }

  uv_mutex_unlock(&pool->mutex);
  uv_mutex_destroy(&pool->mutex);

  free(pool->connections);
  free(pool->conninfo);
  free(pool);
}

typedef struct {
  PGpool *pool;
  int conn_index;
  void (*original_callback)(PGconn *, void *);
  void *original_data;
} reset_and_return_ctx_t;

static void on_connection_reset_for_borrow(PGconn *conn, void *data) {
  reset_and_return_ctx_t *ctx = data;
  decrement_async_work();

  if (!conn) {
    // Reset failed, mark connection as dead
    uv_mutex_lock(&ctx->pool->mutex);
    ctx->pool->connections[ctx->conn_index].conn = NULL;
    uv_mutex_unlock(&ctx->pool->mutex);

    // Try to get another connection
    pg_pool_request(ctx->pool, ctx->original_callback, ctx->original_data);
    free(ctx);
    return;
  }

  // Reset successful, update pool
  uv_mutex_lock(&ctx->pool->mutex);
  ctx->pool->connections[ctx->conn_index].conn = conn;
  ctx->pool->connections[ctx->conn_index].in_use = true;
  ctx->pool->connections[ctx->conn_index].last_used = uv_hrtime() / 1000000;
  uv_mutex_unlock(&ctx->pool->mutex);

  // Return to original requester
  ctx->original_callback(conn, ctx->original_data);
  free(ctx);
}

static PGconn *pg_pool_borrow_internal(PGpool *pool,
                                       void (*callback)(PGconn *, void *),
                                       void *data,
                                       bool *needs_async_reset) {
  // Called with mutex locked
  *needs_async_reset = false;

  for (int i = 0; i < pool->size; i++) {
    if (pool->connections[i].conn && !pool->connections[i].in_use) {
      PGconn *conn = pool->connections[i].conn;
      ConnStatusType status = PQstatus(conn);

      if (status == CONNECTION_OK) {
        pool->connections[i].in_use = true;
        pool->connections[i].last_used = uv_hrtime() / 1000000;
        return conn;
      } else if (status == CONNECTION_BAD) {
        *needs_async_reset = true;

        reset_and_return_ctx_t *reset_ctx = malloc(sizeof(reset_and_return_ctx_t));
        if (reset_ctx) {
          reset_ctx->pool = pool;
          reset_ctx->conn_index = i;
          reset_ctx->original_callback = callback;
          reset_ctx->original_data = data;

          pool->connections[i].in_use = true;
          uv_mutex_unlock(&pool->mutex);
          pg_async_reset(conn, on_connection_reset_for_borrow, reset_ctx);
          uv_mutex_lock(&pool->mutex);
        }

        return NULL; // Caller will get connection via callback
      }
    }
  }

  return NULL;
}

PGconn *pg_pool_borrow(PGpool *pool) {
  if (!pool)
    return NULL;

  uv_mutex_lock(&pool->mutex);

  if (pool->destroyed) {
    uv_mutex_unlock(&pool->mutex);
    return NULL;
  }

  bool dummy = false;
  PGconn *conn = pg_pool_borrow_internal(pool, NULL, NULL, &dummy);

  uv_mutex_unlock(&pool->mutex);
  return conn;
}

static void pg_pool_request(PGpool *pool,
                            void (*callback)(PGconn *conn, void *data),
                            void *data) {
  if (!pool || !callback) {
    if (callback)
      callback(NULL, data);
    return;
  }

  increment_async_work();

  uv_mutex_lock(&pool->mutex);

  if (pool->destroyed) {
    uv_mutex_unlock(&pool->mutex);
    decrement_async_work();
    callback(NULL, data);
    return;
  }

  bool needs_async_reset = false;
  PGconn *conn = pg_pool_borrow_internal(pool, callback, data, &needs_async_reset);

  if (needs_async_reset) {
    uv_mutex_unlock(&pool->mutex);
    return;
  }

  if (conn) {
    uv_mutex_unlock(&pool->mutex);
    decrement_async_work();
    callback(conn, data);
    return;
  }

  pool_wait_t *waiter = malloc(sizeof(pool_wait_t));
  if (!waiter) {
    uv_mutex_unlock(&pool->mutex);
    callback(NULL, data);
    return;
  }

  waiter->callback = callback;
  waiter->data = data;
  waiter->next = NULL;
  waiter->cancelled = false;
  waiter->timeout_timer = NULL;

  if (!pool->wait_queue_head) {
    pool->wait_queue_head = pool->wait_queue_tail = waiter;
  } else {
    pool->wait_queue_tail->next = waiter;
    pool->wait_queue_tail = waiter;
  }

  pool->wait_count++;

  if (pool->timeout_ms > 0) {
    waiter->timeout_timer = malloc(sizeof(uv_timer_t));
    if (waiter->timeout_timer) {
      pool_timeout_ctx_t *timeout_ctx = malloc(sizeof(pool_timeout_ctx_t));
      if (timeout_ctx) {
        timeout_ctx->pool = pool;
        timeout_ctx->waiter = waiter;

        if (uv_timer_init(get_loop(), waiter->timeout_timer) == 0) {
          waiter->timeout_timer->data = timeout_ctx;
          uv_timer_start(waiter->timeout_timer, on_pool_timeout,
                         pool->timeout_ms, 0);
        } else {
          free(timeout_ctx);
          free(waiter->timeout_timer);
          waiter->timeout_timer = NULL;
        }
      } else {
        free(waiter->timeout_timer);
        waiter->timeout_timer = NULL;
      }
    }
  }

  uv_mutex_unlock(&pool->mutex);
}

void pg_pool_return(PGpool *pool, PGconn *conn) {
  if (!pool || !conn) {
    LOG_ERROR("pg_pool_return: invalid parameters");
    return;
  }

  uv_mutex_lock(&pool->mutex);

  int conn_idx = -1;
  for (int i = 0; i < pool->size; i++) {
    if (pool->connections[i].conn == conn) {
      conn_idx = i;
      pool->connections[i].in_use = false;
      break;
    }
  }

  if (conn_idx < 0) {
    uv_mutex_unlock(&pool->mutex);
    LOG_ERROR("Warning: attempted to release unknown connection");
    return;
  }

  if (pool->wait_queue_head) {
    pool_wait_t *waiter = pool->wait_queue_head;
    pool->wait_queue_head = waiter->next;

    if (!pool->wait_queue_head)
      pool->wait_queue_tail = NULL;

    pool->wait_count--;

    if (waiter->timeout_timer) {
      waiter->cancelled = true;
      pool_timeout_ctx_t *timeout_ctx = waiter->timeout_timer->data;
      uv_timer_stop(waiter->timeout_timer);
      uv_close((uv_handle_t *)waiter->timeout_timer, (uv_close_cb)free);
      free(timeout_ctx);
    }

    pool->connections[conn_idx].in_use = true;
    pool->connections[conn_idx].last_used = uv_hrtime() / 1000000;

    uv_mutex_unlock(&pool->mutex);

    decrement_async_work();
    waiter->callback(conn, waiter->data);
    free(waiter);
    return;
  }

  uv_mutex_unlock(&pool->mutex);
}

void pg_pool_get_stats(PGpool *pool, PGPoolStats *stats) {
  if (!pool || !stats) {
    if (stats) {
      stats->total = 0;
      stats->available = 0;
      stats->in_use = false;
    }
    return;
  }

  uv_mutex_lock(&pool->mutex);

  stats->total = pool->size;
  stats->available = 0;
  stats->in_use = false;

  for (int i = 0; i < pool->size; i++) {
    if (pool->connections[i].conn) {
      if (pool->connections[i].in_use) {
        stats->in_use++;
      } else {
        stats->available++;
      }
    }
  }

  uv_mutex_unlock(&pool->mutex);
}

int pg_pool_cleanup_idle(PGpool *pool, uint64_t max_idle_ms) {
  if (!pool)
    return -1;

  int closed_count = 0;
  uint64_t now = uv_hrtime() / 1000000;

  uv_mutex_lock(&pool->mutex);

  for (int i = 0; i < pool->size; i++) {
    if (pool->connections[i].conn && !pool->connections[i].in_use) {
      uint64_t idle_time = now - pool->connections[i].last_used;

      if (idle_time > max_idle_ms) {
        PQfinish(pool->connections[i].conn);
        pool->connections[i].conn = NULL; // Will be recreated on next borrow
        closed_count++;
      }
    }
  }

  uv_mutex_unlock(&pool->mutex);

  return closed_count;
}

static void on_handle_closed(uv_handle_t *handle) {
  if (!handle || !handle->data)
    return;

  PGquery *pg = (PGquery *)handle->data;
  pg->handle_initialized = false;
}

static void cancel_execution(PGquery *pg) {
  if (!pg)
    return;

  if (pg->handle_initialized) {
#ifdef _WIN32
    uv_timer_stop(&pg->timer);
    if (!uv_is_closing((uv_handle_t *)&pg->timer)) {
      uv_close((uv_handle_t *)&pg->timer, on_handle_closed);
    }
#else
    uv_poll_stop(&pg->poll);
    if (!uv_is_closing((uv_handle_t *)&pg->poll)) {
      uv_close((uv_handle_t *)&pg->poll, on_handle_closed);
    }
#endif
  }

  if (pg->conn && pg->is_executing) {
    PGcancel *cancel = PQgetCancel(pg->conn);
    if (cancel) {
      char errbuf[256];
      PQcancel(cancel, errbuf, sizeof(errbuf));
      PQfreeCancel(cancel);
    }
  }

  pg->is_executing = false;
}

static void cleanup_and_destroy(PGquery *pg) {
  if (!pg)
    return;

  cancel_execution(pg);

  if (pg->pool && pg->conn)
    pg_pool_return(pg->pool, pg->conn);

  if (pg->on_complete)
    pg->on_complete(pg, pg->complete_data);

  // If part of parallel execution, notify the parallel context
  // Parallel callback is called via on_complete

  if (pg->arena_owned && pg->arena && !pg->parallel)
    arena_return(pg->arena);

  // PGquery itself is in the arena, so it's freed when arena is returned
  // If arena is user-managed, user is responsible for cleanup
}

static void handle_query_error(PGquery *pg) {
  pg->is_executing = false;
  cleanup_and_destroy(pg);
}

#ifdef _WIN32
static void on_timer(uv_timer_t *handle) {
  if (!handle || !handle->data)
    return;

  PGquery *pg = (PGquery *)handle->data;

  if (!server_is_running()) {
    uv_timer_stop(&pg->timer);
    if (!uv_is_closing((uv_handle_t *)&pg->timer)) {
      uv_close((uv_handle_t *)&pg->timer, on_handle_closed);
    }
    pg->is_executing = false;
    cleanup_and_destroy(pg);
    return;
  }

  if (!PQconsumeInput(pg->conn)) {
    LOG_ERROR("PQconsumeInput failed: %s", PQerrorMessage(pg->conn));
    handle_query_error(pg);
    return;
  }

  if (PQisBusy(pg->conn))
    return;

  uv_timer_stop(&pg->timer);

  PGresult *result;
  while ((result = PQgetResult(pg->conn)) != NULL) {
    ExecStatusType result_status = PQresultStatus(result);

    if (result_status != PGRES_TUPLES_OK && result_status != PGRES_COMMAND_OK) {
      LOG_ERROR("Query failed: %s", PQresultErrorMessage(result));
      PQclear(result);
      pg->current_query = NULL;
      handle_query_error(pg);
      return;
    }

    if (pg->current_query && pg->current_query->result_cb) {
      pg->in_callback = true;
      pg->current_query->result_cb(pg, result, pg->current_query->data);
      pg->in_callback = false;
    }

    PQclear(result);
  }

  pg->current_query = NULL;

  if (pg->query_queue) {
    execute_next_query(pg);
  } else {
    pg->is_executing = false;
    cleanup_and_destroy(pg);
  }
}
#else
static void on_poll(uv_poll_t *handle, int status, int events) {
  (void)events;

  if (!handle || !handle->data)
    return;

  PGquery *pg = (PGquery *)handle->data;

  if (!server_is_running()) {
    uv_poll_stop(&pg->poll);
    if (!uv_is_closing((uv_handle_t *)&pg->poll)) {
      uv_close((uv_handle_t *)&pg->poll, on_handle_closed);
    }
    pg->is_executing = false;
    cleanup_and_destroy(pg);
    return;
  }

  if (status < 0) {
    LOG_ERROR("Poll error: %s", uv_strerror(status));
    handle_query_error(pg);
    return;
  }

  if (!PQconsumeInput(pg->conn)) {
    LOG_ERROR("PQconsumeInput failed: %s", PQerrorMessage(pg->conn));
    handle_query_error(pg);
    return;
  }

  if (PQisBusy(pg->conn))
    return;

  uv_poll_stop(&pg->poll);

  PGresult *result;
  while ((result = PQgetResult(pg->conn)) != NULL) {
    ExecStatusType result_status = PQresultStatus(result);

    if (result_status != PGRES_TUPLES_OK && result_status != PGRES_COMMAND_OK) {
      LOG_ERROR("Query failed: %s", PQresultErrorMessage(result));
      PQclear(result);
      pg->current_query = NULL;
      handle_query_error(pg);
      return;
    }

    if (pg->current_query && pg->current_query->result_cb) {
      pg->in_callback = true;
      pg->current_query->result_cb(pg, result, pg->current_query->data);
      pg->in_callback = false;
    }

    PQclear(result);
  }

  pg->current_query = NULL;

  if (pg->query_queue) {
    execute_next_query(pg);
  } else {
    pg->is_executing = false;
    cleanup_and_destroy(pg);
  }
}
#endif

static void execute_next_query(PGquery *pg) {
  if (!pg->query_queue) {
    if (!pg->in_callback) {
      pg->is_executing = false;
      cleanup_and_destroy(pg);
    }
    return;
  }

  if (!server_is_running()) {
    pg->is_executing = false;
    cleanup_and_destroy(pg);
    return;
  }

  pg->current_query = pg->query_queue;
  pg->query_queue = pg->query_queue->next;
  if (!pg->query_queue) {
    pg->query_queue_tail = NULL;
  }

  int result;
  if (pg->current_query->param_count > 0) {
    result = PQsendQueryParams(
        pg->conn,
        pg->current_query->sql,
        pg->current_query->param_count,
        NULL,
        (const char **)pg->current_query->params,
        NULL,
        NULL,
        0);
  } else {
    result = PQsendQuery(pg->conn, pg->current_query->sql);
  }

  if (!result) {
    LOG_ERROR("Failed to send query: %s", PQerrorMessage(pg->conn));
    handle_query_error(pg);
    return;
  }

#ifdef _WIN32
  if (!pg->handle_initialized) {
    int init_result = uv_timer_init(get_loop(), &pg->timer);
    if (init_result != 0) {
      LOG_ERROR("uv_timer_init failed: %s", uv_strerror(init_result));
      handle_query_error(pg);
      return;
    }
    pg->handle_initialized = true;
    pg->timer.data = pg;
  }

  int start_result = uv_timer_start(&pg->timer, on_timer, 10, 10);
  if (start_result != 0) {
    LOG_ERROR("uv_timer_start failed: %s", uv_strerror(start_result));
    handle_query_error(pg);
    return;
  }
#else
  int sock = PQsocket(pg->conn);

  if (sock < 0) {
    LOG_ERROR("Invalid PostgreSQL socket");
    handle_query_error(pg);
    return;
  }

  if (!pg->handle_initialized) {
    int init_result = uv_poll_init(get_loop(), &pg->poll, sock);
    if (init_result != 0) {
      LOG_ERROR("uv_poll_init failed: %s", uv_strerror(init_result));
      handle_query_error(pg);
      return;
    }

    pg->handle_initialized = true;
    pg->poll.data = pg;
  }

  int start_result = uv_poll_start(&pg->poll, UV_READABLE | UV_WRITABLE, on_poll);
  if (start_result != 0) {
    LOG_ERROR("uv_poll_start failed: %s", uv_strerror(start_result));
    handle_query_error(pg);
    return;
  }
#endif
}

static PGquery *pg_query_init(PGconn *conn, Arena *arena, PGpool *pool) {
  if (!arena) {
    LOG_ERROR("pg_query_init: arena is NULL");
    return NULL;
  }

  if (conn && PQstatus(conn) != CONNECTION_OK) {
    LOG_ERROR("pg_query_init: Connection status is not OK");
    return NULL;
  }

  PGquery *pg = arena_alloc(arena, sizeof(PGquery));
  if (!pg) {
    LOG_ERROR("pg_query_init: Failed to allocate from arena");
    return NULL;
  }

  memset(pg, 0, sizeof(PGquery));
  pg->conn = conn;
  pg->arena = arena;
  pg->is_connected = (conn != NULL);
  pg->is_executing = false;
  pg->handle_initialized = false;
  pg->query_queue = NULL;
  pg->query_queue_tail = NULL;
  pg->current_query = NULL;
  pg->pool = pool;
  pg->arena_owned = false;
  pg->on_complete = NULL;
  pg->complete_data = NULL;
  pg->parallel = NULL;
  pg->parallel_index = -1;

#ifdef _WIN32
  pg->timer.data = pg;
#else
  pg->poll.data = pg;
#endif

  return pg;
}

void pg_query_on_complete(PGquery *pg, pg_complete_cb_t callback, void *data) {
  if (!pg)
    return;

  pg->on_complete = callback;
  pg->complete_data = data;
}

int pg_query_queue(PGquery *pg,
                   const char *sql,
                   int param_count,
                   const char **params,
                   pg_result_cb_t result_cb,
                   void *query_data) {
  if (!pg || !sql) {
    LOG_ERROR("pg_query_queue: Invalid parameters");
    return -1;
  }

  pg_query_t *query = arena_alloc(pg->arena, sizeof(pg_query_t));
  if (!query) {
    LOG_ERROR("pg_query_queue: Failed to allocate query");
    return -1;
  }

  memset(query, 0, sizeof(pg_query_t));
  query->next = NULL;

  query->sql = arena_strdup(pg->arena, sql);
  if (!query->sql) {
    LOG_ERROR("pg_query_queue: Failed to copy SQL");
    return -1;
  }

  if (param_count > 0 && params) {
    query->params = arena_alloc(pg->arena, param_count * sizeof(char *));
    if (!query->params) {
      LOG_ERROR("pg_query_queue: Failed to allocate params");
      return -1;
    }

    for (int i = 0; i < param_count; i++) {
      if (params[i]) {
        query->params[i] = arena_strdup(pg->arena, params[i]);
        if (!query->params[i]) {
          LOG_ERROR("pg_query_queue: Failed to allocate a param");
          return -1;
        }
      } else {
        query->params[i] = NULL;
      }
    }
  } else {
    query->params = NULL;
  }

  query->param_count = param_count;
  query->result_cb = result_cb;
  query->data = query_data;

  if (!pg->query_queue) {
    pg->query_queue = pg->query_queue_tail = query;
  } else {
    pg->query_queue_tail->next = query;
    pg->query_queue_tail = query;
  }

  return 0;
}

PGquery *pg_query_create(PGpool *pool, Arena *arena) {
  if (!pool) {
    LOG_ERROR("pg_query_async_create: pool is NULL");
    return NULL;
  }

  bool arena_owned = false;
  if (!arena) {
    arena = arena_borrow();
    if (!arena) {
      LOG_ERROR("pg_query_async_create: Failed to borrow arena");
      return NULL;
    }
    arena_owned = true;
  }

  PGquery *pg = pg_query_init(NULL, arena, pool);
  if (!pg) {
    if (arena_owned)
      arena_return(arena);
    return NULL;
  }

  pg->arena_owned = arena_owned;
  return pg;
}

static void on_connection_acquired_for_query(PGconn *conn, void *data) {
  PGquery *pg = data;

  decrement_async_work();

  if (!server_is_running()) {
    if (conn && pg->pool)
      pg_pool_return(pg->pool, conn);

    if (pg->on_complete)
      pg->on_complete(NULL, pg->complete_data);

    if (pg->arena_owned && pg->arena)
      arena_return(pg->arena);

    return;
  }

  if (!conn) {
    LOG_ERROR("Failed to acquire connection for query");

    if (pg->on_complete)
      pg->on_complete(NULL, pg->complete_data);

    if (pg->arena_owned && pg->arena)
      arena_return(pg->arena);

    return;
  }

  pg->conn = conn;
  pg->is_connected = true;
  pg->is_executing = true;

  execute_next_query(pg);
}

int pg_query_exec(PGquery *pg) {
  if (!pg) {
    LOG_ERROR("pg_query_exec: pg is NULL");
    return -1;
  }

  if (pg->is_executing) {
    LOG_ERROR("pg_query_exec: Already executing");
    return -1;
  }

  if (!pg->query_queue) {
    cleanup_and_destroy(pg);
    return 0;
  }

  increment_async_work();

  pg_pool_request(pg->pool, on_connection_acquired_for_query, pg);

  return 0;
}

int pg_query_exec_trans(PGquery *pg) {
  if (!pg) {
    LOG_ERROR("pg_query_exec_trans_async: pg is NULL");
    return -1;
  }

  if (pg->is_executing) {
    LOG_ERROR("pg_query_exec_trans_async: Already executing");
    return -1;
  }

  if (!pg->query_queue) {
    cleanup_and_destroy(pg);
    return 0;
  }

  pg_query_t *begin_query = arena_alloc(pg->arena, sizeof(pg_query_t));
  if (!begin_query) {
    LOG_ERROR("pg_query_exec_trans_async: Failed to allocate BEGIN query");
    return -1;
  }

  memset(begin_query, 0, sizeof(pg_query_t));
  begin_query->sql = arena_strdup(pg->arena, "BEGIN");
  if (!begin_query->sql) {
    LOG_ERROR("pg_query_exec_trans_async: Failed to copy BEGIN SQL");
    return -1;
  }
  begin_query->params = NULL;
  begin_query->param_count = 0;
  begin_query->result_cb = NULL;
  begin_query->data = NULL;
  begin_query->next = pg->query_queue;
  pg->query_queue = begin_query;

  pg_query_t *commit_query = arena_alloc(pg->arena, sizeof(pg_query_t));
  if (!commit_query) {
    LOG_ERROR("pg_query_exec_trans_async: Failed to allocate COMMIT query");
    return -1;
  }

  memset(commit_query, 0, sizeof(pg_query_t));
  commit_query->sql = arena_strdup(pg->arena, "COMMIT");
  if (!commit_query->sql) {
    LOG_ERROR("pg_query_exec_trans_async: Failed to copy COMMIT SQL");
    return -1;
  }
  commit_query->params = NULL;
  commit_query->param_count = 0;
  commit_query->result_cb = NULL;
  commit_query->data = NULL;
  commit_query->next = NULL;

  pg->query_queue_tail->next = commit_query;
  pg->query_queue_tail = commit_query;

  return pg_query_exec(pg);
}

static void on_parallel_stream_complete(PGquery *pg, void *data) {
  (void)data;

  if (!pg || !pg->parallel)
    return;

  PGparallel *parallel = pg->parallel;

  uv_mutex_lock(&parallel->mutex);

  parallel->completed++;
  bool is_last = (parallel->completed >= parallel->started);

  if (!is_last) {
    // Not the last one, just return
    uv_mutex_unlock(&parallel->mutex);
    return;
  }

  // The last stream to complete
  // Keep mutex locked during cleanup

  pg_parallel_cb_t callback = parallel->on_complete;
  void *callback_data = parallel->complete_data;
  bool arena_owned = parallel->arena_owned;
  Arena *arena = parallel->arena;

  // Unlock before calling user callback
  uv_mutex_unlock(&parallel->mutex);

  if (callback)
    callback(parallel, 1, callback_data);

  uv_mutex_destroy(&parallel->mutex);

  if (arena_owned && arena)
    arena_return(arena);
}

PGparallel *pg_parallel_create(PGpool *pool, int count, Arena *arena) {
  if (!pool || count <= 0) {
    LOG_ERROR("pg_parallel_create: Invalid parameters");
    return NULL;
  }

  bool arena_owned = false;

  if (!arena) {
    arena = arena_borrow();
    if (!arena) {
      LOG_ERROR("pg_parallel_create: Failed to borrow arena");
      return NULL;
    }
    arena_owned = true;
  }

  PGparallel *parallel = arena_alloc(arena, sizeof(PGparallel));
  if (!parallel) {
    LOG_ERROR("pg_parallel_create: Failed to allocate parallel context");
    if (arena_owned)
      arena_return(arena);
    return NULL;
  }

  memset(parallel, 0, sizeof(PGparallel));
  parallel->pool = pool;
  parallel->arena = arena;
  parallel->arena_owned = arena_owned;
  parallel->count = count;
  parallel->completed = 0;
  parallel->started = 0;
  parallel->on_complete = NULL;
  parallel->complete_data = NULL;

  if (uv_mutex_init(&parallel->mutex) != 0) {
    LOG_ERROR("pg_parallel_create: Failed to initialize mutex");
    if (arena_owned)
      arena_return(arena);
    return NULL;
  }

  parallel->streams = arena_alloc(arena, count * sizeof(PGquery *));
  if (!parallel->streams) {
    LOG_ERROR("pg_parallel_create: Failed to allocate streams array");
    uv_mutex_destroy(&parallel->mutex);
    if (arena_owned)
      arena_return(arena);
    return NULL;
  }

  parallel->conns = arena_alloc(arena, count * sizeof(PGconn *));
  if (!parallel->conns) {
    LOG_ERROR("pg_parallel_create: Failed to allocate conns array");
    uv_mutex_destroy(&parallel->mutex);
    if (arena_owned)
      arena_return(arena);
    return NULL;
  }

  for (int i = 0; i < count; i++) {
    parallel->streams[i] = NULL;
    parallel->conns[i] = NULL;
  }

  return parallel;
}

PGquery *pg_parallel_get(PGparallel *parallel, int index) {
  if (!parallel || index < 0 || index >= parallel->count) {
    LOG_ERROR("pg_parallel_get: Invalid parameters");
    return NULL;
  }

  // Lazy initialization of stream
  // without connection, happens in worker thread
  if (!parallel->streams[index]) {
    PGquery *pg = arena_alloc(parallel->arena, sizeof(PGquery));
    if (!pg) {
      LOG_ERROR("pg_parallel_get: Failed to allocate query for stream %d", index);
      return NULL;
    }

    memset(pg, 0, sizeof(PGquery));
    pg->conn = NULL; // Will be set after acquisition
    pg->arena = parallel->arena;
    pg->is_connected = false; // Not connected yet
    pg->is_executing = false;
    pg->handle_initialized = false;
    pg->query_queue = NULL;
    pg->query_queue_tail = NULL;
    pg->current_query = NULL;
    pg->pool = parallel->pool;
    pg->arena_owned = false; // Parallel context owns the arena
    pg->parallel = parallel;
    pg->parallel_index = index;

    pg->on_complete = on_parallel_stream_complete;
    pg->complete_data = parallel;

#ifdef _WIN32
    pg->timer.data = pg;
#else
    pg->poll.data = pg;
#endif

    parallel->streams[index] = pg;
  }

  return parallel->streams[index];
}

void pg_parallel_on_complete(PGparallel *parallel, pg_parallel_cb_t callback, void *data) {
  if (!parallel)
    return;

  parallel->on_complete = callback;
  parallel->complete_data = data;
}

typedef struct {
  PGparallel *parallel;
  int index;
} parallel_conn_ctx_t;

static void on_parallel_connection_ready(PGconn *conn, void *data) {
  parallel_conn_ctx_t *ctx = data;
  PGparallel *parallel = ctx->parallel;
  int index = ctx->index;

  decrement_async_work();

  free(ctx);

  if (!server_is_running()) {
    if (conn && parallel->pool)
      pg_pool_return(parallel->pool, conn);

    uv_mutex_lock(&parallel->mutex);
    parallel->completed++;

    if (parallel->completed >= parallel->count) {
      if (parallel->on_complete)
        parallel->on_complete(parallel, 0, parallel->complete_data);

      uv_mutex_destroy(&parallel->mutex);

      if (parallel->arena_owned && parallel->arena)
        arena_return(parallel->arena);
    }
    uv_mutex_unlock(&parallel->mutex);
    return;
  }

  uv_mutex_lock(&parallel->mutex);

  if (conn && parallel->streams[index]) {
    parallel->conns[index] = conn;
    parallel->streams[index]->conn = conn;
    parallel->streams[index]->is_connected = true;
    parallel->started++;
  } else {
    parallel->completed++;
  }

  bool all_ready = true;
  for (int i = 0; i < parallel->count; i++) {
    if (parallel->streams[i] && parallel->streams[i]->query_queue) {
      if (!parallel->conns[i]) {
        all_ready = false;
        break;
      }
    }
  }

  uv_mutex_unlock(&parallel->mutex);

  if (all_ready) {
    for (int i = 0; i < parallel->count; i++) {
      PGquery *pg = parallel->streams[i];
      if (pg && pg->query_queue && pg->conn) {
        pg->is_executing = true;
        execute_next_query(pg);
      }
    }
  }
}

int pg_parallel_exec(PGparallel *parallel) {
  if (!parallel) {
    LOG_ERROR("pg_parallel_exec: parallel is NULL");
    return -1;
  }

  int active_streams = 0;
  for (int i = 0; i < parallel->count; i++) {
    if (parallel->streams[i] && parallel->streams[i]->query_queue) {
      active_streams++;
    }
  }

  if (active_streams == 0) {
    if (parallel->on_complete)
      parallel->on_complete(parallel, 1, parallel->complete_data);

    uv_mutex_destroy(&parallel->mutex);

    if (parallel->arena_owned && parallel->arena)
      arena_return(parallel->arena);

    return 0;
  }

  for (int i = 0; i < parallel->count; i++) {
    if (parallel->streams[i] && parallel->streams[i]->query_queue) {
      increment_async_work();
      parallel_conn_ctx_t *ctx = malloc(sizeof(parallel_conn_ctx_t));
      ctx->parallel = parallel;
      ctx->index = i;

      pg_pool_request(parallel->pool, on_parallel_connection_ready, ctx);
    }
  }

  return 0;
}

int pg_parallel_count(PGparallel *parallel) {
  if (!parallel)
    return 0;

  return parallel->count;
}
