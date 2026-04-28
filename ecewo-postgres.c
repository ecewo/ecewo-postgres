#include "ecewo-postgres.h"
#include "libpq-fe.h"
#include "uv.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

#ifdef ECEWO_DEBUG
#define LOG_DEBUG(fmt, ...) \
  fprintf(stderr, "[DEBUG] " fmt "\n", ##__VA_ARGS__)
#else
#define LOG_DEBUG(fmt, ...) ((void)0)
#endif

#define LOG_ERROR(fmt, ...) \
  fprintf(stderr, "[ERROR] [ecewo-postgres] " fmt "\n", ##__VA_ARGS__)

static inline uv_loop_t *pg_loop(void) {
  return (uv_loop_t *)ecewo_get_loop();
}

static inline PGresult *as_pgresult(ecewo_pg_result_t *r) { return (PGresult *)r; }
static inline const PGresult *as_pgresult_c(const ecewo_pg_result_t *r) { return (const PGresult *)r; }
static inline ecewo_pg_result_t *from_pgresult(PGresult *r) { return (ecewo_pg_result_t *)r; }

typedef struct pg_query_node_s {
  char *sql;
  char **params;
  uint8_t param_count;
  ecewo_pg_result_cb_t result_cb;
  void *data;
  struct pg_query_node_s *next;
} pg_query_node_t;

struct ecewo_pg_query_s {
  PGconn *conn;
  ecewo_arena_t *arena;
  void *data;

  bool is_connected;
  bool is_executing;
  bool in_callback;

  pg_query_node_t *query_queue;
  pg_query_node_t *query_queue_tail;
  pg_query_node_t *current_query;

  ecewo_pg_pool_t *pool;
  bool arena_owned;

  ecewo_pg_complete_cb_t on_complete;
  void *complete_data;

  ecewo_pg_parallel_t *parallel;
  int parallel_index;

  bool handle_initialized;
  atomic_bool handle_closing;
  bool needs_close;
  uv_mutex_t handle_mutex;

  ecewo_response_t *res;
  ecewo_client_t *client;

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

struct ecewo_pg_pool_s {
  ecewo_app_t *app;
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

struct ecewo_pg_pool_config_s {
  ecewo_app_t *app;
  char *host;
  char *port;
  char *dbname;
  char *user;
  char *password;
  int pool_size;
  int timeout_ms;
};

struct ecewo_pg_parallel_s {
  ecewo_pg_pool_t *pool;
  ecewo_arena_t *arena;
  bool arena_owned;
  ecewo_pg_query_t **streams;
  PGconn **conns;
  uint8_t count;
  uint8_t completed;
  uint8_t started;
  ecewo_pg_parallel_cb_t on_complete;
  void *complete_data;
  uv_mutex_t mutex;
  ecewo_response_t *res;
  ecewo_client_t *client;
};

typedef struct {
  PGconn *conn;
  uv_poll_t poll;
  ecewo_pg_pool_t *pool;
  void (*callback)(PGconn *conn, void *data);
  void *data;
  uint64_t start_time;
  bool connecting;
} async_connect_ctx_t;

static void execute_next_query(ecewo_pg_query_t *pg);
static void on_parallel_stream_complete(ecewo_pg_query_t *pg, void *data);
static void pool_request(ecewo_pg_pool_t *pool,
                         void (*callback)(PGconn *conn, void *data),
                         void *data);
static void pool_release(ecewo_pg_pool_t *pool, PGconn *conn);

#ifdef _WIN32
static void on_timer(uv_timer_t *handle);
#else
static void on_poll(uv_poll_t *handle, int status, int events);
#endif

typedef struct {
  uv_timer_t timer;
  pool_wait_t *waiter;
  ecewo_pg_pool_t *pool;
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
  ecewo_decrement_async_work();

  ctx->waiter->callback(NULL, ctx->waiter->data);
  free(ctx->waiter);

  uv_close((uv_handle_t *)handle, (uv_close_cb)free);
}

static char *build_conninfo(const ecewo_pg_pool_config_t *config) {
  const char *host = config->host ? config->host : "";
  const char *port = config->port ? config->port : "";
  const char *dbname = config->dbname ? config->dbname : "";
  const char *user = config->user ? config->user : "";
  const char *password = config->password ? config->password : "";

  size_t len = snprintf(NULL, 0,
                        "host=%s port=%s dbname=%s user=%s password=%s",
                        host, port, dbname, user, password)
      + 1;

  char *conninfo = malloc(len);
  if (!conninfo)
    return NULL;

  snprintf(conninfo, len,
           "host=%s port=%s dbname=%s user=%s password=%s",
           host, port, dbname, user, password);

  return conninfo;
}

typedef struct {
  PGconn *conn;
  uv_poll_t poll;
  void (*callback)(PGconn *conn, void *data);
  void *data;
  ecewo_app_t *app;
} async_reset_ctx_t;

static void on_reset_ready(uv_poll_t *handle, int status, int events) {
  (void)status;
  (void)events;

  async_reset_ctx_t *ctx = handle->data;

  if (!ecewo_is_running(ctx->app)) {
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
                           ecewo_app_t *app,
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
  ctx->app = app;

  int sock = PQsocket(conn);
  if (uv_poll_init(pg_loop(), &ctx->poll, sock) != 0) {
    free(ctx);
    callback(NULL, data);
    return;
  }

  ctx->poll.data = ctx;
  on_reset_ready(&ctx->poll, 0, 0);
}

static PGconn *pg_connect(const char *conninfo) {
  PGconn *conn = PQconnectdb(conninfo);

  if (!conn || PQstatus(conn) != CONNECTION_OK) {
    if (conn) {
      LOG_ERROR("Connection failed: %s", PQerrorMessage(conn));
      PQfinish(conn);
    }
    return NULL;
  }

  if (PQsetnonblocking(conn, 1) != 0) {
    LOG_ERROR("Failed to set non-blocking mode");
    PQfinish(conn);
    return NULL;
  }

  return conn;
}

// ---------------------------------------------------------------------------
// POOL CONFIG
// ---------------------------------------------------------------------------

ecewo_pg_pool_config_t *ecewo_pg_pool_config_new(void) {
  ecewo_pg_pool_config_t *config = calloc(1, sizeof(ecewo_pg_pool_config_t));
  if (!config)
    return NULL;

  config->timeout_ms = -1;
  config->pool_size = 1;
  return config;
}

void ecewo_pg_pool_config_free(ecewo_pg_pool_config_t *config) {
  if (!config)
    return;

  free(config->host);
  free(config->port);
  free(config->dbname);
  free(config->user);
  free(config->password);
  free(config);
}

static void config_set_string(char **dst, const char *src) {
  free(*dst);
  *dst = src ? strdup(src) : NULL;
}

void ecewo_pg_pool_config_set_app(ecewo_pg_pool_config_t *config, ecewo_app_t *app) {
  if (config) config->app = app;
}

void ecewo_pg_pool_config_set_host(ecewo_pg_pool_config_t *config, const char *host) {
  if (config) config_set_string(&config->host, host);
}

void ecewo_pg_pool_config_set_port(ecewo_pg_pool_config_t *config, const char *port) {
  if (config) config_set_string(&config->port, port);
}

void ecewo_pg_pool_config_set_dbname(ecewo_pg_pool_config_t *config, const char *dbname) {
  if (config) config_set_string(&config->dbname, dbname);
}

void ecewo_pg_pool_config_set_user(ecewo_pg_pool_config_t *config, const char *user) {
  if (config) config_set_string(&config->user, user);
}

void ecewo_pg_pool_config_set_password(ecewo_pg_pool_config_t *config, const char *password) {
  if (config) config_set_string(&config->password, password);
}

void ecewo_pg_pool_config_set_pool_size(ecewo_pg_pool_config_t *config, int pool_size) {
  if (config) config->pool_size = pool_size;
}

void ecewo_pg_pool_config_set_timeout_ms(ecewo_pg_pool_config_t *config, int timeout_ms) {
  if (config) config->timeout_ms = timeout_ms;
}

// ---------------------------------------------------------------------------
// POOL LIFECYCLE
// ---------------------------------------------------------------------------

ecewo_pg_pool_t *ecewo_pg_pool_create(const ecewo_pg_pool_config_t *config) {
  if (!config || config->pool_size <= 0 || config->pool_size > 1024) {
    LOG_ERROR("Invalid pool configuration");
    return NULL;
  }

  if (!config->app) {
    LOG_ERROR("Pool configuration requires a non-NULL ecewo_app_t *app");
    return NULL;
  }

  ecewo_pg_pool_t *pool = malloc(sizeof(ecewo_pg_pool_t));
  if (!pool) {
    LOG_ERROR("Failed to allocate pool");
    return NULL;
  }

  memset(pool, 0, sizeof(ecewo_pg_pool_t));

  pool->app = config->app;
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
    ecewo_pg_pool_destroy(pool);
    return NULL;
  }

#ifdef ECEWO_DEBUG
  if (connected < pool->size) {
    printf("Pool created with %d/%d connections\n", connected, pool->size);
  }
#endif

  return pool;
}

void ecewo_pg_pool_destroy(ecewo_pg_pool_t *pool) {
  if (!pool)
    return;

  uv_mutex_lock(&pool->mutex);
  pool->destroyed = true;

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
  ecewo_pg_pool_t *pool;
  int conn_index;
  void (*original_callback)(PGconn *, void *);
  void *original_data;
} reset_and_return_ctx_t;

static void on_connection_reset_for_borrow(PGconn *conn, void *data) {
  reset_and_return_ctx_t *ctx = data;
  ecewo_decrement_async_work();

  if (!conn) {
    uv_mutex_lock(&ctx->pool->mutex);
    ctx->pool->connections[ctx->conn_index].conn = NULL;
    uv_mutex_unlock(&ctx->pool->mutex);

    pool_request(ctx->pool, ctx->original_callback, ctx->original_data);
    free(ctx);
    return;
  }

  uv_mutex_lock(&ctx->pool->mutex);
  ctx->pool->connections[ctx->conn_index].conn = conn;
  ctx->pool->connections[ctx->conn_index].in_use = true;
  ctx->pool->connections[ctx->conn_index].last_used = uv_hrtime() / 1000000;
  uv_mutex_unlock(&ctx->pool->mutex);

  ctx->original_callback(conn, ctx->original_data);
  free(ctx);
}

static PGconn *pool_borrow_internal(ecewo_pg_pool_t *pool,
                                    void (*callback)(PGconn *, void *),
                                    void *data,
                                    bool *needs_async_reset) {
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
          pg_async_reset(conn, pool->app, on_connection_reset_for_borrow, reset_ctx);
          uv_mutex_lock(&pool->mutex);
        }

        return NULL;
      }
    }
  }

  return NULL;
}

static void pool_request(ecewo_pg_pool_t *pool,
                         void (*callback)(PGconn *conn, void *data),
                         void *data) {
  if (!pool || !callback) {
    if (callback)
      callback(NULL, data);
    return;
  }

  ecewo_increment_async_work();

  uv_mutex_lock(&pool->mutex);

  if (pool->destroyed) {
    uv_mutex_unlock(&pool->mutex);
    ecewo_decrement_async_work();
    callback(NULL, data);
    return;
  }

  bool needs_async_reset = false;
  PGconn *conn = pool_borrow_internal(pool, callback, data, &needs_async_reset);

  if (needs_async_reset) {
    uv_mutex_unlock(&pool->mutex);
    return;
  }

  if (conn) {
    uv_mutex_unlock(&pool->mutex);
    ecewo_decrement_async_work();
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

        if (uv_timer_init(pg_loop(), waiter->timeout_timer) == 0) {
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

static void pool_release(ecewo_pg_pool_t *pool, PGconn *conn) {
  if (!pool || !conn) {
    LOG_ERROR("pool_release: invalid parameters");
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

    ecewo_decrement_async_work();
    waiter->callback(conn, waiter->data);
    free(waiter);
    return;
  }

  uv_mutex_unlock(&pool->mutex);
}

int ecewo_pg_pool_total(ecewo_pg_pool_t *pool) {
  if (!pool)
    return 0;

  uv_mutex_lock(&pool->mutex);
  int total = pool->size;
  uv_mutex_unlock(&pool->mutex);
  return total;
}

int ecewo_pg_pool_available(ecewo_pg_pool_t *pool) {
  if (!pool)
    return 0;

  int available = 0;
  uv_mutex_lock(&pool->mutex);
  for (int i = 0; i < pool->size; i++) {
    if (pool->connections[i].conn && !pool->connections[i].in_use)
      available++;
  }
  uv_mutex_unlock(&pool->mutex);
  return available;
}

int ecewo_pg_pool_in_use(ecewo_pg_pool_t *pool) {
  if (!pool)
    return 0;

  int in_use = 0;
  uv_mutex_lock(&pool->mutex);
  for (int i = 0; i < pool->size; i++) {
    if (pool->connections[i].conn && pool->connections[i].in_use)
      in_use++;
  }
  uv_mutex_unlock(&pool->mutex);
  return in_use;
}

int ecewo_pg_pool_cleanup_idle(ecewo_pg_pool_t *pool, uint64_t max_idle_ms) {
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
        pool->connections[i].conn = NULL;
        closed_count++;
      }
    }
  }

  uv_mutex_unlock(&pool->mutex);

  return closed_count;
}

// ---------------------------------------------------------------------------
// QUERY EXECUTION
// ---------------------------------------------------------------------------

static void on_handle_closed(uv_handle_t *handle) {
  if (!handle || !handle->data)
    return;

  ecewo_pg_query_t *pg = (ecewo_pg_query_t *)handle->data;

  if (pg->pool && pg->conn)
    pool_release(pg->pool, pg->conn);

  if (pg->on_complete) {
    if (pg->client) {
      if (ecewo_client_is_valid(pg->client)) {
        pg->on_complete(pg, pg->complete_data);
      } else {
        LOG_DEBUG("Client closed before query completion");
      }
      ecewo_client_unref(pg->client);
      pg->client = NULL;
    } else {
      pg->on_complete(pg, pg->complete_data);
    }
  }

  uv_mutex_destroy(&pg->handle_mutex);

  if (pg->arena_owned && pg->arena && !pg->parallel)
    ecewo_arena_return(pg->arena);
}

static void cancel_execution(ecewo_pg_query_t *pg) {
  if (!pg)
    return;

  uv_mutex_lock(&pg->handle_mutex);

  if (pg->needs_close && pg->handle_initialized && !atomic_load(&pg->handle_closing)) {
    atomic_store(&pg->handle_closing, true);

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

  uv_mutex_unlock(&pg->handle_mutex);

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

static void cleanup_and_destroy(ecewo_pg_query_t *pg) {
  if (!pg)
    return;

  cancel_execution(pg);

  if (!pg->handle_initialized) {
    if (pg->pool && pg->conn)
      pool_release(pg->pool, pg->conn);

    if (pg->on_complete) {
      if (pg->client) {
        if (ecewo_client_is_valid(pg->client)) {
          pg->on_complete(pg, pg->complete_data);
        } else {
          LOG_ERROR("Client closed before query completion");
        }
        ecewo_client_unref(pg->client);
        pg->client = NULL;
      } else {
        pg->on_complete(pg, pg->complete_data);
      }
    }

    uv_mutex_destroy(&pg->handle_mutex);

    if (pg->arena_owned && pg->arena && !pg->parallel)
      ecewo_arena_return(pg->arena);
  }
}

static void handle_query_error(ecewo_pg_query_t *pg) {
  pg->is_executing = false;

  if (pg->conn && PQtransactionStatus(pg->conn) == PQTRANS_INERROR) {
    PGresult *rb = PQexec(pg->conn, "ROLLBACK");
    if (rb)
      PQclear(rb);
  }

  cleanup_and_destroy(pg);
}

#ifdef _WIN32
static void on_timer(uv_timer_t *handle) {
  if (!handle || !handle->data)
    return;

  ecewo_pg_query_t *pg = (ecewo_pg_query_t *)handle->data;

  if (!ecewo_is_running(pg->pool ? pg->pool->app : NULL)) {
    uv_timer_stop(&pg->timer);
    pg->needs_close = true;
    pg->is_executing = false;
    cleanup_and_destroy(pg);
    return;
  }

  if (!PQconsumeInput(pg->conn)) {
    LOG_ERROR("PQconsumeInput failed: %s", PQerrorMessage(pg->conn));
    uv_timer_stop(&pg->timer);
    pg->needs_close = true;
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
      pg->needs_close = true;
      handle_query_error(pg);
      return;
    }

    if (pg->current_query && pg->current_query->result_cb) {
      pg->in_callback = true;
      pg->current_query->result_cb(pg, from_pgresult(result), pg->current_query->data);
      pg->in_callback = false;
    }

    PQclear(result);
  }

  pg->current_query = NULL;

  if (pg->query_queue) {
    execute_next_query(pg);
  } else {
    pg->needs_close = true;
    pg->is_executing = false;
    cleanup_and_destroy(pg);
  }
}
#else
static void on_poll(uv_poll_t *handle, int status, int events) {
  (void)events;

  if (!handle || !handle->data)
    return;

  ecewo_pg_query_t *pg = (ecewo_pg_query_t *)handle->data;

  if (!ecewo_is_running(pg->pool ? pg->pool->app : NULL)) {
    uv_poll_stop(&pg->poll);
    pg->needs_close = true;
    pg->is_executing = false;
    cleanup_and_destroy(pg);
    return;
  }

  if (status < 0) {
    LOG_ERROR("Poll error: %s", uv_strerror(status));
    uv_poll_stop(&pg->poll);
    pg->needs_close = true;
    handle_query_error(pg);
    return;
  }

  if (!PQconsumeInput(pg->conn)) {
    LOG_ERROR("PQconsumeInput failed: %s", PQerrorMessage(pg->conn));
    uv_poll_stop(&pg->poll);
    pg->needs_close = true;
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
      pg->needs_close = true;
      handle_query_error(pg);
      return;
    }

    if (pg->current_query && pg->current_query->result_cb) {
      pg->in_callback = true;
      pg->current_query->result_cb(pg, from_pgresult(result), pg->current_query->data);
      pg->in_callback = false;
    }

    PQclear(result);
  }

  pg->current_query = NULL;

  if (pg->query_queue) {
    execute_next_query(pg);
  } else {
    pg->needs_close = true;
    pg->is_executing = false;
    cleanup_and_destroy(pg);
  }
}
#endif

static void execute_next_query(ecewo_pg_query_t *pg) {
  if (!pg->query_queue) {
    if (!pg->in_callback) {
      pg->is_executing = false;
      cleanup_and_destroy(pg);
    }
    return;
  }

  if (!ecewo_is_running(pg->pool ? pg->pool->app : NULL)) {
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
    int init_result = uv_timer_init(pg_loop(), &pg->timer);
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
    int init_result = uv_poll_init(pg_loop(), &pg->poll, sock);
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

static ecewo_pg_query_t *query_init(PGconn *conn, ecewo_arena_t *arena, ecewo_pg_pool_t *pool) {
  if (!arena) {
    LOG_ERROR("query_init: arena is NULL");
    return NULL;
  }

  if (conn && PQstatus(conn) != CONNECTION_OK) {
    LOG_ERROR("query_init: Connection status is not OK");
    return NULL;
  }

  ecewo_pg_query_t *pg = ecewo_alloc(arena, sizeof(ecewo_pg_query_t));
  if (!pg) {
    LOG_ERROR("query_init: Failed to allocate from arena");
    return NULL;
  }

  memset(pg, 0, sizeof(ecewo_pg_query_t));
  pg->conn = conn;
  pg->arena = arena;
  pg->is_connected = (conn != NULL);
  pg->is_executing = false;
  pg->handle_initialized = false;
  atomic_init(&pg->handle_closing, false);
  pg->needs_close = false;

  if (uv_mutex_init(&pg->handle_mutex) != 0) {
    LOG_ERROR("Failed to initialize handle mutex");
    return NULL;
  }

  pg->query_queue = NULL;
  pg->query_queue_tail = NULL;
  pg->current_query = NULL;
  pg->pool = pool;
  pg->arena_owned = false;
  pg->on_complete = NULL;
  pg->complete_data = NULL;
  pg->parallel = NULL;
  pg->parallel_index = -1;
  pg->res = NULL;
  pg->client = NULL;

#ifdef _WIN32
  pg->timer.data = pg;
#else
  pg->poll.data = pg;
#endif

  return pg;
}

void ecewo_pg_query_on_complete(ecewo_pg_query_t *pg, ecewo_pg_complete_cb_t callback, void *data) {
  if (!pg)
    return;

  pg->on_complete = callback;
  pg->complete_data = data;
}

int ecewo_pg_query_queue(ecewo_pg_query_t *pg,
                         const char *sql,
                         int param_count,
                         const char **params,
                         ecewo_pg_result_cb_t result_cb,
                         void *query_data) {
  if (!pg || !sql) {
    LOG_ERROR("ecewo_pg_query_queue: Invalid parameters");
    return -1;
  }

  pg_query_node_t *query = ecewo_alloc(pg->arena, sizeof(pg_query_node_t));
  if (!query) {
    LOG_ERROR("ecewo_pg_query_queue: Failed to allocate query");
    return -1;
  }

  memset(query, 0, sizeof(pg_query_node_t));
  query->next = NULL;

  query->sql = ecewo_strdup(pg->arena, sql);
  if (!query->sql) {
    LOG_ERROR("ecewo_pg_query_queue: Failed to copy SQL");
    return -1;
  }

  if (param_count > 0 && params) {
    query->params = ecewo_alloc(pg->arena, param_count * sizeof(char *));
    if (!query->params) {
      LOG_ERROR("ecewo_pg_query_queue: Failed to allocate params");
      return -1;
    }

    for (int i = 0; i < param_count; i++) {
      if (params[i]) {
        query->params[i] = ecewo_strdup(pg->arena, params[i]);
        if (!query->params[i]) {
          LOG_ERROR("ecewo_pg_query_queue: Failed to allocate a param");
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

ecewo_pg_query_t *ecewo_pg_query_create(ecewo_pg_pool_t *pool, ecewo_response_t *res) {
  if (!pool) {
    LOG_ERROR("ecewo_pg_query_create: pool is NULL");
    return NULL;
  }

  ecewo_arena_t *pg_arena = ecewo_arena_borrow();
  if (!pg_arena) {
    LOG_ERROR("ecewo_pg_query_create: Failed to borrow arena");
    return NULL;
  }

  ecewo_pg_query_t *pg = query_init(NULL, pg_arena, pool);
  if (!pg) {
    ecewo_arena_return(pg_arena);
    return NULL;
  }

  pg->arena_owned = true;

  ecewo_client_t *client = res ? ecewo_res_client(res) : NULL;
  if (client) {
    pg->res = res;
    pg->client = client;
    ecewo_client_ref(pg->client);
  } else {
    pg->res = NULL;
    pg->client = NULL;
  }

  return pg;
}

static void on_connection_acquired_for_query(PGconn *conn, void *data) {
  ecewo_pg_query_t *pg = data;

  ecewo_decrement_async_work();

  if (!ecewo_is_running(pg->pool ? pg->pool->app : NULL)) {
    if (conn && pg->pool)
      pool_release(pg->pool, conn);

    if (pg->on_complete)
      pg->on_complete(NULL, pg->complete_data);

    if (pg->arena_owned && pg->arena)
      ecewo_arena_return(pg->arena);

    return;
  }

  if (!conn) {
    LOG_ERROR("Failed to acquire connection for query");

    if (pg->on_complete)
      pg->on_complete(NULL, pg->complete_data);

    if (pg->arena_owned && pg->arena)
      ecewo_arena_return(pg->arena);

    return;
  }

  pg->conn = conn;
  pg->is_connected = true;
  pg->is_executing = true;

  execute_next_query(pg);
}

int ecewo_pg_query_exec(ecewo_pg_query_t *pg) {
  if (!pg) {
    LOG_ERROR("ecewo_pg_query_exec: pg is NULL");
    return -1;
  }

  if (pg->is_executing) {
    LOG_ERROR("ecewo_pg_query_exec: Already executing");
    return -1;
  }

  if (!pg->query_queue) {
    cleanup_and_destroy(pg);
    return 0;
  }

  ecewo_increment_async_work();

  pool_request(pg->pool, on_connection_acquired_for_query, pg);

  return 0;
}

int ecewo_pg_query_exec_trans(ecewo_pg_query_t *pg) {
  if (!pg) {
    LOG_ERROR("ecewo_pg_query_exec_trans: pg is NULL");
    return -1;
  }

  if (pg->is_executing) {
    LOG_ERROR("ecewo_pg_query_exec_trans: Already executing");
    return -1;
  }

  if (!pg->query_queue) {
    cleanup_and_destroy(pg);
    return 0;
  }

  pg_query_node_t *begin_query = ecewo_alloc(pg->arena, sizeof(pg_query_node_t));
  if (!begin_query) {
    LOG_ERROR("ecewo_pg_query_exec_trans: Failed to allocate BEGIN query");
    return -1;
  }

  memset(begin_query, 0, sizeof(pg_query_node_t));
  begin_query->sql = ecewo_strdup(pg->arena, "BEGIN");
  if (!begin_query->sql) {
    LOG_ERROR("ecewo_pg_query_exec_trans: Failed to copy BEGIN SQL");
    return -1;
  }
  begin_query->params = NULL;
  begin_query->param_count = 0;
  begin_query->result_cb = NULL;
  begin_query->data = NULL;
  begin_query->next = pg->query_queue;
  pg->query_queue = begin_query;

  pg_query_node_t *commit_query = ecewo_alloc(pg->arena, sizeof(pg_query_node_t));
  if (!commit_query) {
    LOG_ERROR("ecewo_pg_query_exec_trans: Failed to allocate COMMIT query");
    return -1;
  }

  memset(commit_query, 0, sizeof(pg_query_node_t));
  commit_query->sql = ecewo_strdup(pg->arena, "COMMIT");
  if (!commit_query->sql) {
    LOG_ERROR("ecewo_pg_query_exec_trans: Failed to copy COMMIT SQL");
    return -1;
  }
  commit_query->params = NULL;
  commit_query->param_count = 0;
  commit_query->result_cb = NULL;
  commit_query->data = NULL;
  commit_query->next = NULL;

  pg->query_queue_tail->next = commit_query;
  pg->query_queue_tail = commit_query;

  return ecewo_pg_query_exec(pg);
}

// ---------------------------------------------------------------------------
// PARALLEL EXECUTION
// ---------------------------------------------------------------------------

static void on_parallel_stream_complete(ecewo_pg_query_t *pg, void *data) {
  (void)data;

  if (!pg || !pg->parallel)
    return;

  ecewo_pg_parallel_t *parallel = pg->parallel;

  uv_mutex_lock(&parallel->mutex);

  parallel->completed++;
  bool is_last = (parallel->completed >= parallel->started);

  if (!is_last) {
    uv_mutex_unlock(&parallel->mutex);
    return;
  }

  ecewo_pg_parallel_cb_t callback = parallel->on_complete;
  void *callback_data = parallel->complete_data;
  bool arena_owned = parallel->arena_owned;
  ecewo_arena_t *arena = parallel->arena;

  uv_mutex_unlock(&parallel->mutex);

  if (callback) {
    if (parallel->client) {
      if (ecewo_client_is_valid(parallel->client)) {
        callback(parallel, 1, callback_data);
      } else {
        LOG_DEBUG("Client closed before parallel completion");
      }
      ecewo_client_unref(parallel->client);
    } else {
      callback(parallel, 1, callback_data);
    }
  }

  uv_mutex_destroy(&parallel->mutex);

  if (arena_owned && arena)
    ecewo_arena_return(arena);
}

ecewo_pg_parallel_t *ecewo_pg_parallel_create(ecewo_pg_pool_t *pool, int count, ecewo_response_t *res) {
  if (!pool || count <= 0) {
    LOG_ERROR("ecewo_pg_parallel_create: Invalid parameters");
    return NULL;
  }

  ecewo_arena_t *arena = ecewo_arena_borrow();
  if (!arena) {
    LOG_ERROR("ecewo_pg_parallel_create: Failed to borrow arena");
    return NULL;
  }

  ecewo_pg_parallel_t *parallel = ecewo_alloc(arena, sizeof(ecewo_pg_parallel_t));
  if (!parallel) {
    LOG_ERROR("ecewo_pg_parallel_create: Failed to allocate parallel context");
    ecewo_arena_return(arena);
    return NULL;
  }

  memset(parallel, 0, sizeof(ecewo_pg_parallel_t));
  parallel->pool = pool;
  parallel->arena = arena;
  parallel->arena_owned = true;
  parallel->count = count;
  parallel->completed = 0;
  parallel->started = 0;
  parallel->on_complete = NULL;
  parallel->complete_data = NULL;

  ecewo_client_t *client = res ? ecewo_res_client(res) : NULL;
  if (client) {
    parallel->res = res;
    parallel->client = client;
    ecewo_client_ref(parallel->client);
  } else {
    parallel->res = NULL;
    parallel->client = NULL;
  }

  if (uv_mutex_init(&parallel->mutex) != 0) {
    LOG_ERROR("ecewo_pg_parallel_create: Failed to initialize mutex");
    ecewo_arena_return(arena);
    return NULL;
  }

  parallel->streams = ecewo_alloc(arena, count * sizeof(ecewo_pg_query_t *));
  if (!parallel->streams) {
    LOG_ERROR("ecewo_pg_parallel_create: Failed to allocate streams array");
    uv_mutex_destroy(&parallel->mutex);
    ecewo_arena_return(arena);
    return NULL;
  }

  parallel->conns = ecewo_alloc(arena, count * sizeof(PGconn *));
  if (!parallel->conns) {
    LOG_ERROR("ecewo_pg_parallel_create: Failed to allocate conns array");
    uv_mutex_destroy(&parallel->mutex);
    ecewo_arena_return(arena);
    return NULL;
  }

  for (int i = 0; i < count; i++) {
    parallel->streams[i] = NULL;
    parallel->conns[i] = NULL;
  }

  return parallel;
}

ecewo_pg_query_t *ecewo_pg_parallel_get(ecewo_pg_parallel_t *parallel, int index) {
  if (!parallel || index < 0 || index >= parallel->count) {
    LOG_ERROR("ecewo_pg_parallel_get: Invalid parameters");
    return NULL;
  }

  if (!parallel->streams[index]) {
    ecewo_pg_query_t *pg = ecewo_alloc(parallel->arena, sizeof(ecewo_pg_query_t));
    if (!pg) {
      LOG_ERROR("ecewo_pg_parallel_get: Failed to allocate query for stream %d", index);
      return NULL;
    }

    memset(pg, 0, sizeof(ecewo_pg_query_t));
    pg->conn = NULL;
    pg->arena = parallel->arena;
    pg->is_connected = false;
    pg->is_executing = false;
    pg->handle_initialized = false;
    pg->needs_close = false;
    atomic_init(&pg->handle_closing, false);
    pg->query_queue = NULL;
    pg->query_queue_tail = NULL;
    pg->current_query = NULL;
    pg->pool = parallel->pool;
    pg->arena_owned = false;
    pg->parallel = parallel;
    pg->parallel_index = index;

    pg->on_complete = on_parallel_stream_complete;
    pg->complete_data = parallel;

    if (uv_mutex_init(&pg->handle_mutex) != 0) {
      LOG_ERROR("ecewo_pg_parallel_get: Failed to initialize handle mutex for stream %d", index);
      return NULL;
    }

#ifdef _WIN32
    pg->timer.data = pg;
#else
    pg->poll.data = pg;
#endif

    parallel->streams[index] = pg;
  }

  return parallel->streams[index];
}

void ecewo_pg_parallel_on_complete(ecewo_pg_parallel_t *parallel, ecewo_pg_parallel_cb_t callback, void *data) {
  if (!parallel)
    return;

  parallel->on_complete = callback;
  parallel->complete_data = data;
}

typedef struct {
  ecewo_pg_parallel_t *parallel;
  int index;
} parallel_conn_ctx_t;

static void on_parallel_connection_ready(PGconn *conn, void *data) {
  parallel_conn_ctx_t *ctx = data;
  ecewo_pg_parallel_t *parallel = ctx->parallel;
  int index = ctx->index;

  ecewo_decrement_async_work();

  if (!ecewo_is_running(parallel->pool ? parallel->pool->app : NULL)) {
    if (conn && parallel->pool)
      pool_release(parallel->pool, conn);

    uv_mutex_lock(&parallel->mutex);
    parallel->completed++;

    if (parallel->completed >= parallel->count) {
      if (parallel->on_complete)
        parallel->on_complete(parallel, 0, parallel->complete_data);

      uv_mutex_destroy(&parallel->mutex);

      if (parallel->arena_owned && parallel->arena)
        ecewo_arena_return(parallel->arena);
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
      ecewo_pg_query_t *pg = parallel->streams[i];
      if (pg && pg->query_queue && pg->conn) {
        pg->is_executing = true;
        execute_next_query(pg);
      }
    }
  }
}

int ecewo_pg_parallel_exec(ecewo_pg_parallel_t *parallel) {
  if (!parallel) {
    LOG_ERROR("ecewo_pg_parallel_exec: parallel is NULL");
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
      ecewo_arena_return(parallel->arena);

    return 0;
  }

  for (int i = 0; i < parallel->count; i++) {
    if (parallel->streams[i] && parallel->streams[i]->query_queue) {
      ecewo_increment_async_work();
      parallel_conn_ctx_t *ctx = ecewo_alloc(parallel->arena, sizeof(parallel_conn_ctx_t));
      if (!ctx) {
        ecewo_decrement_async_work();
        LOG_ERROR("ecewo_pg_parallel_exec: Failed to allocate connection context for stream %d", i);
        continue;
      }
      ctx->parallel = parallel;
      ctx->index = i;

      pool_request(parallel->pool, on_parallel_connection_ready, ctx);
    }
  }

  return 0;
}

int ecewo_pg_parallel_count(ecewo_pg_parallel_t *parallel) {
  if (!parallel)
    return 0;

  return parallel->count;
}

// ---------------------------------------------------------------------------
// RESULT ACCESSORS
// ---------------------------------------------------------------------------

bool ecewo_pg_result_ok(const ecewo_pg_result_t *result) {
  if (!result)
    return false;

  ExecStatusType status = PQresultStatus(as_pgresult_c(result));
  return status == PGRES_TUPLES_OK || status == PGRES_COMMAND_OK;
}

int ecewo_pg_result_ntuples(const ecewo_pg_result_t *result) {
  if (!result)
    return 0;
  return PQntuples(as_pgresult_c(result));
}

int ecewo_pg_result_nfields(const ecewo_pg_result_t *result) {
  if (!result)
    return 0;
  return PQnfields(as_pgresult_c(result));
}

const char *ecewo_pg_result_field_name(const ecewo_pg_result_t *result, int field) {
  if (!result)
    return NULL;
  return PQfname(as_pgresult_c(result), field);
}

const char *ecewo_pg_result_get_value(const ecewo_pg_result_t *result, int row, int field) {
  if (!result)
    return NULL;
  return PQgetvalue(as_pgresult_c(result), row, field);
}

int ecewo_pg_result_get_length(const ecewo_pg_result_t *result, int row, int field) {
  if (!result)
    return 0;
  return PQgetlength(as_pgresult_c(result), row, field);
}

bool ecewo_pg_result_is_null(const ecewo_pg_result_t *result, int row, int field) {
  if (!result)
    return true;
  return PQgetisnull(as_pgresult_c(result), row, field) != 0;
}

const char *ecewo_pg_result_error_message(const ecewo_pg_result_t *result) {
  if (!result)
    return "";
  const char *msg = PQresultErrorMessage(as_pgresult_c(result));
  return msg ? msg : "";
}

const char *ecewo_pg_result_cmd_tuples(const ecewo_pg_result_t *result) {
  if (!result)
    return "";
  const char *tag = PQcmdTuples((PGresult *)as_pgresult_c(result));
  return tag ? tag : "";
}
