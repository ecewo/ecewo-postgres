# ecewo-postgres

**Fully asynchronous PostgreSQL client for [ecewo](https://github.com/savashn/ecewo)**

ecewo-postgres is a non-blocking PostgreSQL client library designed specifically for [ecewo](https://github.com/savashn/ecewo). It provides connection pooling, async query execution, transaction support, and parallel query capabilities. All fully integrated with ecewo's event loop and arena allocator.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [Connection Pool](#connection-pool)
  - [Query Execution](#query-execution)
  - [Transactions](#transactions)
  - [Parallel Queries](#parallel-queries)
  - [Result Accessors](#result-accessors)
- [Examples](#examples)
  - [Simple Select Example](#simple-select-example)
  - [Insert Example](#insert-example)
  - [Multiple Queries Example](#multiple-queries-example)
  - [Query Chaining Example](#query-chaining-example)
  - [Transaction Example](#transaction-example)
  - [Parallel Querying Example](#parallel-querying-example)
- [Best Practices](#best-practices)
- [Error Handling](#error-handling)
- [Memory Management](#memory-management)
- [Performance Tips](#performance-tips)
- [Troubleshooting](#troubleshooting)

---

## Installation

### Prerequisites

Install PostgreSQL development library:

**Ubuntu/Debian:**
```bash
sudo apt-get install libpq-dev
```

**Fedora/RHEL/CentOS:**
```bash
sudo dnf install libpq-devel
```

**macOS:**
```bash
brew install libpq
```

**Windows (MSYS2):**
```bash
pacman -S mingw-w64-x86_64-libpq
```

---

### Add as a dependency

```cmake
ecewo_plugin(postgres)

target_link_libraries(app PRIVATE
    ecewo::ecewo
    ecewo::postgres
)
```

---

## Quick Start

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static ecewo_pg_pool_t *pool = NULL;

static void on_users(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    ecewo_response_t *res = (ecewo_response_t *)data;

    int rows = ecewo_pg_result_ntuples(result);
    ecewo_arena_t *arena = ecewo_res_arena(res);

    char *json = ecewo_sprintf(arena, "[");
    for (int i = 0; i < rows; i++) {
        const char *name = ecewo_pg_result_get_value(result, i, 0);
        json = ecewo_sprintf(arena, "%s%s{\"name\":\"%s\"}",
                             json, i > 0 ? "," : "", name);
    }
    json = ecewo_sprintf(arena, "%s]", json);

    ecewo_send_json(res, ECEWO_OK, json);
}

static void get_users(ecewo_request_t *req, ecewo_response_t *res) {
    ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);

    ecewo_pg_query_queue(pg, "SELECT name FROM users ORDER BY name",
                         0, NULL, on_users, res);

    ecewo_pg_query_exec(pg);
}

static void cleanup(void *data) {
    ecewo_pg_pool_destroy(pool);
}

int main(void) {
    ecewo_app_t *app = ecewo_create();

    ecewo_pg_pool_config_t *config = ecewo_pg_pool_config_new();
    ecewo_pg_pool_config_set_app(config, app);
    ecewo_pg_pool_config_set_host(config, "localhost");
    ecewo_pg_pool_config_set_port(config, "5432");
    ecewo_pg_pool_config_set_dbname(config, "mydb");
    ecewo_pg_pool_config_set_user(config, "postgres");
    ecewo_pg_pool_config_set_password(config, "secret");
    ecewo_pg_pool_config_set_pool_size(config, 10);
    ecewo_pg_pool_config_set_timeout_ms(config, 5000);

    pool = ecewo_pg_pool_create(config);
    ecewo_pg_pool_config_free(config);

    ECEWO_GET(app, "/api/users", get_users);

    ecewo_atexit(app, cleanup, NULL);
    ecewo_listen(app, 3000);

    return 0;
}
```

---

## API Reference

### Connection Pool

#### `ecewo_pg_pool_config_new`

Allocates a new pool configuration object.
```c
ecewo_pg_pool_config_t *ecewo_pg_pool_config_new(void);
```

**Returns:** Config handle or `NULL` on allocation failure.

---

#### `ecewo_pg_pool_config_free`

Frees the config object. Safe to call immediately after `ecewo_pg_pool_create()`.
```c
void ecewo_pg_pool_config_free(ecewo_pg_pool_config_t *config);
```

---

#### Pool config setters

```c
void ecewo_pg_pool_config_set_app(ecewo_pg_pool_config_t *config, ecewo_app_t *app);
void ecewo_pg_pool_config_set_host(ecewo_pg_pool_config_t *config, const char *host);
void ecewo_pg_pool_config_set_port(ecewo_pg_pool_config_t *config, const char *port);
void ecewo_pg_pool_config_set_dbname(ecewo_pg_pool_config_t *config, const char *dbname);
void ecewo_pg_pool_config_set_user(ecewo_pg_pool_config_t *config, const char *user);
void ecewo_pg_pool_config_set_password(ecewo_pg_pool_config_t *config, const char *password);
void ecewo_pg_pool_config_set_pool_size(ecewo_pg_pool_config_t *config, int pool_size);
void ecewo_pg_pool_config_set_timeout_ms(ecewo_pg_pool_config_t *config, int timeout_ms);
```

- `app`: the ecewo application instance. **Required.**
- `pool_size`: number of physical connections, between 1 and 1024.
- `timeout_ms`: `0` = fail immediately when no connection is free, `-1` = wait indefinitely, `> 0` = wait up to N milliseconds.

---

#### `ecewo_pg_pool_create`

Creates a connection pool from a populated config. Opens `pool_size` connections eagerly.
```c
ecewo_pg_pool_t *ecewo_pg_pool_create(const ecewo_pg_pool_config_t *config);
```

**Returns:** Pool handle, or `NULL` if no connections could be established.

**Example:**
```c
ecewo_pg_pool_config_t *config = ecewo_pg_pool_config_new();
ecewo_pg_pool_config_set_app(config, app);
ecewo_pg_pool_config_set_host(config, "localhost");
ecewo_pg_pool_config_set_port(config, "5432");
ecewo_pg_pool_config_set_dbname(config, "myapp");
ecewo_pg_pool_config_set_user(config, "postgres");
ecewo_pg_pool_config_set_password(config, "secret");
ecewo_pg_pool_config_set_pool_size(config, 20);
ecewo_pg_pool_config_set_timeout_ms(config, 5000);

ecewo_pg_pool_t *pool = ecewo_pg_pool_create(config);
ecewo_pg_pool_config_free(config);

if (!pool) {
    fprintf(stderr, "Failed to create pool\n");
    exit(1);
}
```

---

#### `ecewo_pg_pool_destroy`

Closes all connections and frees the pool. Pending waiters receive a `NULL` connection.
```c
void ecewo_pg_pool_destroy(ecewo_pg_pool_t *pool);
```

**Example:**
```c
static void cleanup(void *data) {
    ecewo_pg_pool_destroy(pool);
}

ecewo_atexit(app, cleanup, NULL);
```

---

#### Pool statistics

```c
int ecewo_pg_pool_total(ecewo_pg_pool_t *pool);      // total connection slots
int ecewo_pg_pool_available(ecewo_pg_pool_t *pool);  // idle, ready to borrow
int ecewo_pg_pool_in_use(ecewo_pg_pool_t *pool);     // currently checked out
```

**Example:**
```c
static void pool_status(ecewo_request_t *req, ecewo_response_t *res) {
    ecewo_arena_t *arena = ecewo_res_arena(res);
    char *text = ecewo_sprintf(arena,
        "total: %d, available: %d, in_use: %d",
        ecewo_pg_pool_total(pool),
        ecewo_pg_pool_available(pool),
        ecewo_pg_pool_in_use(pool));

    ecewo_send_text(res, ECEWO_OK, text);
}
```

---

#### `ecewo_pg_pool_cleanup_idle`

Closes connections that have been idle longer than `max_idle_ms`. They are re-created lazily on demand.
```c
int ecewo_pg_pool_cleanup_idle(ecewo_pg_pool_t *pool, uint64_t max_idle_ms);
```

**Returns:** Number of connections closed.

**Example:**
```c
static void cleanup_cb(void *data) {
    ecewo_pg_pool_cleanup_idle((ecewo_pg_pool_t *)data, 30 * 60 * 1000);
}

// Run every 15 minutes
ecewo_interval(cleanup_cb, 15 * 60 * 1000, pool);
```

---

### Query Execution

#### `ecewo_pg_query_create`

Creates a query handle attached to the given response. The handle is freed automatically once the query chain completes.
```c
ecewo_pg_query_t *ecewo_pg_query_create(ecewo_pg_pool_t *pool, ecewo_response_t *res);
```

- `res` may be `NULL` for background work not tied to an HTTP request.
- When `res` is provided, the library ref-counts the client connection so that a disconnecting client cannot cause a crash during async execution.

---

#### `ecewo_pg_query_queue`

Appends a SQL statement to the query queue.
```c
int ecewo_pg_query_queue(ecewo_pg_query_t *pg,
                         const char *sql,
                         int param_count,
                         const char **params,
                         ecewo_pg_result_cb_t result_cb,
                         void *query_data);
```

- `sql`: use `$1`, `$2`, … for parameters.
- `params`: array of string values; may be `NULL` when `param_count` is 0.
- `result_cb`: called once per result set (valid only inside the callback); may be `NULL`.

**Result callback signature:**
```c
typedef void (*ecewo_pg_result_cb_t)(ecewo_pg_query_t *pg,
                                     ecewo_pg_result_t *result,
                                     void *data);
```

**Returns:** `0` on success, `-1` on failure.

**Examples:**

Without parameters:
```c
ecewo_pg_query_queue(pg, "SELECT COUNT(*) FROM users",
                     0, NULL, on_count, res);
```

With parameters:
```c
const char *params[] = { user_id };
ecewo_pg_query_queue(pg, "SELECT * FROM users WHERE id = $1",
                     1, params, on_user, res);
```

---

#### `ecewo_pg_query_exec`

Executes all queued statements sequentially on a single connection (non-blocking).
```c
int ecewo_pg_query_exec(ecewo_pg_query_t *pg);
```

**Returns:** `0` when execution has been scheduled, `-1` on immediate error.

---

#### `ecewo_pg_query_on_complete`

Registers a callback invoked after the entire query chain settles. Typical place to send the HTTP response.
```c
void ecewo_pg_query_on_complete(ecewo_pg_query_t *pg,
                                ecewo_pg_complete_cb_t callback,
                                void *data);
```

**Callback signature:**
```c
typedef void (*ecewo_pg_complete_cb_t)(ecewo_pg_query_t *pg, void *data);
```

**Example:**
```c
static void on_complete(ecewo_pg_query_t *pg, void *data) {
    if (!pg) {
        // Connection acquisition failed or pool destroyed
        ecewo_send_json((ecewo_response_t *)data,
                        ECEWO_SERVICE_UNAVAILABLE,
                        "{\"error\":\"Database unavailable\"}");
        return;
    }
    ecewo_send_text((ecewo_response_t *)data, ECEWO_OK, "Done");
}

ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);
ecewo_pg_query_on_complete(pg, on_complete, res);
ecewo_pg_query_queue(pg, "SELECT * FROM users", 0, NULL, on_users, res);
ecewo_pg_query_exec(pg);
```

---

### Transactions

#### `ecewo_pg_query_exec_trans`

Executes all queued statements wrapped in an implicit transaction. Prepends `BEGIN`, appends `COMMIT`, and rolls back automatically on failure.
```c
int ecewo_pg_query_exec_trans(ecewo_pg_query_t *pg);
```

**Returns:** `0` on success, `-1` on failure.

**Example:**
```c
static void transfer_money(ecewo_request_t *req, ecewo_response_t *res) {
    const char *from_id = "1";
    const char *to_id   = "2";
    const char *amount  = "100.00";

    ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);

    const char *debit[]  = { amount, from_id };
    const char *credit[] = { amount, to_id };

    ecewo_pg_query_queue(pg,
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
        2, debit, NULL, NULL);

    ecewo_pg_query_queue(pg,
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        2, credit, on_transfer_done, res);

    ecewo_pg_query_exec_trans(pg);
}
```

For advanced control (savepoints, isolation levels) you can queue `BEGIN`/`COMMIT` manually and call `ecewo_pg_query_exec()` instead.

---

### Parallel Queries

Execute multiple independent queries concurrently, each on its own connection.

#### `ecewo_pg_parallel_create`

```c
ecewo_pg_parallel_t *ecewo_pg_parallel_create(ecewo_pg_pool_t *pool,
                                              int count,
                                              ecewo_response_t *res);
```

- `count`: number of independent streams.
- `res`: may be `NULL` for non-HTTP usage.

---

#### `ecewo_pg_parallel_get`

Returns the query handle for stream `index`. Populate it with `ecewo_pg_query_queue()`.
```c
ecewo_pg_query_t *ecewo_pg_parallel_get(ecewo_pg_parallel_t *parallel, int index);
```

---

#### `ecewo_pg_parallel_on_complete`

Registers a callback invoked once every stream has completed.
```c
void ecewo_pg_parallel_on_complete(ecewo_pg_parallel_t *parallel,
                                   ecewo_pg_parallel_cb_t callback,
                                   void *data);
```

**Callback signature:**
```c
typedef void (*ecewo_pg_parallel_cb_t)(ecewo_pg_parallel_t *parallel,
                                       int success,
                                       void *data);
```

- `success`: `1` when every stream succeeded, `0` if any failed.

---

#### `ecewo_pg_parallel_exec`

Starts all streams. Each acquires its own pool connection and runs concurrently.
```c
int ecewo_pg_parallel_exec(ecewo_pg_parallel_t *parallel);
```

---

#### `ecewo_pg_parallel_count`

Returns the number of streams configured on the parallel context.
```c
int ecewo_pg_parallel_count(ecewo_pg_parallel_t *parallel);
```

---

### Result Accessors

These functions are valid only inside a `ecewo_pg_result_cb_t` callback.

```c
bool        ecewo_pg_result_ok(const ecewo_pg_result_t *result);
int         ecewo_pg_result_ntuples(const ecewo_pg_result_t *result);
int         ecewo_pg_result_nfields(const ecewo_pg_result_t *result);
const char *ecewo_pg_result_field_name(const ecewo_pg_result_t *result, int field);
const char *ecewo_pg_result_get_value(const ecewo_pg_result_t *result, int row, int field);
int         ecewo_pg_result_get_length(const ecewo_pg_result_t *result, int row, int field);
bool        ecewo_pg_result_is_null(const ecewo_pg_result_t *result, int row, int field);
const char *ecewo_pg_result_error_message(const ecewo_pg_result_t *result);
const char *ecewo_pg_result_cmd_tuples(const ecewo_pg_result_t *result);
```

---

## Examples

### Simple Select Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static ecewo_pg_pool_t *pool = NULL;

static void on_user(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    ecewo_response_t *res = (ecewo_response_t *)data;

    if (ecewo_pg_result_ntuples(result) == 0) {
        ecewo_send_text(res, ECEWO_NOT_FOUND, "User not found");
        return;
    }

    const char *name  = ecewo_pg_result_get_value(result, 0, 0);
    const char *email = ecewo_pg_result_get_value(result, 0, 1);

    ecewo_arena_t *arena = ecewo_res_arena(res);
    char *body = ecewo_sprintf(arena, "name: %s email: %s", name, email);
    ecewo_send_text(res, ECEWO_OK, body);
}

static void get_user(ecewo_request_t *req, ecewo_response_t *res) {
    const char *id = ecewo_param(req, "id");

    ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);

    const char *params[] = { id };
    ecewo_pg_query_queue(pg,
        "SELECT name, email FROM users WHERE id = $1",
        1, params, on_user, res);

    ecewo_pg_query_exec(pg);
}

static void cleanup(void *data) {
    ecewo_pg_pool_destroy(pool);
}

int main(void) {
    ecewo_app_t *app = ecewo_create();

    ecewo_pg_pool_config_t *config = ecewo_pg_pool_config_new();
    ecewo_pg_pool_config_set_app(config, app);
    ecewo_pg_pool_config_set_host(config, "localhost");
    ecewo_pg_pool_config_set_port(config, "5432");
    ecewo_pg_pool_config_set_dbname(config, "mydb");
    ecewo_pg_pool_config_set_user(config, "postgres");
    ecewo_pg_pool_config_set_password(config, "secret");
    ecewo_pg_pool_config_set_pool_size(config, 10);
    ecewo_pg_pool_config_set_timeout_ms(config, 5000);

    pool = ecewo_pg_pool_create(config);
    ecewo_pg_pool_config_free(config);

    ECEWO_GET(app, "/users/:id", get_user);

    ecewo_atexit(app, cleanup, NULL);
    ecewo_listen(app, 3000);

    return 0;
}
```

---

### Insert Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static ecewo_pg_pool_t *pool = NULL;

static void on_user_created(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    ecewo_response_t *res = (ecewo_response_t *)data;
    const char *id = ecewo_pg_result_get_value(result, 0, 0);
    char *body = ecewo_sprintf(ecewo_res_arena(res), "{\"id\":\"%s\"}", id);
    ecewo_send_json(res, ECEWO_CREATED, body);
}

static void create_user(ecewo_request_t *req, ecewo_response_t *res) {
    // Parse from request body as needed
    const char *name  = "John Doe";
    const char *email = "john@example.com";

    ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);

    const char *params[] = { name, email };
    ecewo_pg_query_queue(pg,
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
        2, params, on_user_created, res);

    ecewo_pg_query_exec(pg);
}

static void cleanup(void *data) { ecewo_pg_pool_destroy(pool); }

int main(void) {
    ecewo_app_t *app = ecewo_create();

    ecewo_pg_pool_config_t *config = ecewo_pg_pool_config_new();
    ecewo_pg_pool_config_set_app(config, app);
    ecewo_pg_pool_config_set_host(config, "localhost");
    ecewo_pg_pool_config_set_port(config, "5432");
    ecewo_pg_pool_config_set_dbname(config, "mydb");
    ecewo_pg_pool_config_set_user(config, "postgres");
    ecewo_pg_pool_config_set_password(config, "secret");
    ecewo_pg_pool_config_set_pool_size(config, 10);
    ecewo_pg_pool_config_set_timeout_ms(config, 5000);

    pool = ecewo_pg_pool_create(config);
    ecewo_pg_pool_config_free(config);

    ECEWO_POST(app, "/users", create_user);

    ecewo_atexit(app, cleanup, NULL);
    ecewo_listen(app, 3000);

    return 0;
}
```

---

### Multiple Queries Example

Queue several statements on a single connection; each result callback populates a shared context, and the last one sends the response.

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static ecewo_pg_pool_t *pool = NULL;

typedef struct {
    ecewo_response_t *res;
    char *user_name;
    char *user_email;
    char *posts_json;
} profile_ctx_t;

static void on_user_info(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    profile_ctx_t *ctx = (profile_ctx_t *)data;
    ecewo_arena_t *arena = ecewo_res_arena(ctx->res);

    if (ecewo_pg_result_ntuples(result) == 0) {
        ecewo_send_text(ctx->res, ECEWO_NOT_FOUND, "User not found");
        return;
    }

    ctx->user_name  = ecewo_strdup(arena, ecewo_pg_result_get_value(result, 0, 0));
    ctx->user_email = ecewo_strdup(arena, ecewo_pg_result_get_value(result, 0, 1));
}

static void on_user_posts(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    profile_ctx_t *ctx = (profile_ctx_t *)data;
    ecewo_arena_t *arena = ecewo_res_arena(ctx->res);
    int count = ecewo_pg_result_ntuples(result);

    char *json = ecewo_sprintf(arena, "[");
    for (int i = 0; i < count; i++) {
        const char *title = ecewo_pg_result_get_value(result, i, 0);
        json = ecewo_sprintf(arena, "%s%s{\"title\":\"%s\"}",
                             json, i > 0 ? "," : "", title);
    }
    ctx->posts_json = ecewo_sprintf(arena, "%s]", json);
}

static void on_user_comments(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    profile_ctx_t *ctx = (profile_ctx_t *)data;
    ecewo_arena_t *arena = ecewo_res_arena(ctx->res);
    int comment_count = ecewo_pg_result_ntuples(result);

    char *json = ecewo_sprintf(arena,
        "{\"name\":\"%s\",\"email\":\"%s\",\"posts\":%s,\"comment_count\":%d}",
        ctx->user_name, ctx->user_email, ctx->posts_json, comment_count);

    ecewo_send_json(ctx->res, ECEWO_OK, json);
}

static void get_user_profile(ecewo_request_t *req, ecewo_response_t *res) {
    const char *user_id = ecewo_param(req, "id");

    ecewo_arena_t *arena = ecewo_req_arena(req);
    profile_ctx_t *ctx = ecewo_alloc(arena, sizeof(profile_ctx_t));
    ctx->res = res;

    ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);

    const char *params[] = { user_id };

    ecewo_pg_query_queue(pg,
        "SELECT name, email FROM users WHERE id = $1",
        1, params, on_user_info, ctx);

    ecewo_pg_query_queue(pg,
        "SELECT title FROM posts WHERE user_id = $1 ORDER BY created_at DESC LIMIT 5",
        1, params, on_user_posts, ctx);

    ecewo_pg_query_queue(pg,
        "SELECT COUNT(*) FROM comments WHERE user_id = $1",
        1, params, on_user_comments, ctx);

    // All three run sequentially on the same connection
    ecewo_pg_query_exec(pg);
}

static void cleanup(void *data) { ecewo_pg_pool_destroy(pool); }

int main(void) {
    ecewo_app_t *app = ecewo_create();

    ecewo_pg_pool_config_t *config = ecewo_pg_pool_config_new();
    ecewo_pg_pool_config_set_app(config, app);
    ecewo_pg_pool_config_set_host(config, "localhost");
    ecewo_pg_pool_config_set_port(config, "5432");
    ecewo_pg_pool_config_set_dbname(config, "mydb");
    ecewo_pg_pool_config_set_user(config, "postgres");
    ecewo_pg_pool_config_set_password(config, "secret");
    ecewo_pg_pool_config_set_pool_size(config, 10);
    ecewo_pg_pool_config_set_timeout_ms(config, 5000);

    pool = ecewo_pg_pool_create(config);
    ecewo_pg_pool_config_free(config);

    ECEWO_GET(app, "/users/:id/profile", get_user_profile);

    ecewo_atexit(app, cleanup, NULL);
    ecewo_listen(app, 3000);

    return 0;
}
```

---

### Query Chaining Example

A result callback can queue additional statements on the same `ecewo_pg_query_t`; they execute on the same connection without re-acquiring one.

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static ecewo_pg_pool_t *pool = NULL;

typedef struct {
    ecewo_response_t *res;
    const char *user_id;
} chain_ctx_t;

static void on_posts_inserted(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    chain_ctx_t *ctx = (chain_ctx_t *)data;
    ecewo_send_text(ctx->res, ECEWO_CREATED, "Success!");
}

static void on_user_created(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    chain_ctx_t *ctx = (chain_ctx_t *)data;

    ctx->user_id = ecewo_pg_result_get_value(result, 0, 0);

    const char *params[] = { ctx->user_id, "First Post" };
    ecewo_pg_query_queue(pg,
        "INSERT INTO posts (user_id, title) VALUES ($1, $2)",
        2, params, on_posts_inserted, ctx);
}

static void create_user_with_post(ecewo_request_t *req, ecewo_response_t *res) {
    ecewo_arena_t *arena = ecewo_req_arena(req);
    chain_ctx_t *ctx = ecewo_alloc(arena, sizeof(chain_ctx_t));
    ctx->res = res;

    ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);

    const char *params[] = { "John Doe", "john@example.com" };
    ecewo_pg_query_queue(pg,
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
        2, params, on_user_created, ctx);

    ecewo_pg_query_exec(pg);
}

static void cleanup(void *data) { ecewo_pg_pool_destroy(pool); }

int main(void) {
    ecewo_app_t *app = ecewo_create();

    ecewo_pg_pool_config_t *config = ecewo_pg_pool_config_new();
    ecewo_pg_pool_config_set_app(config, app);
    ecewo_pg_pool_config_set_host(config, "localhost");
    ecewo_pg_pool_config_set_port(config, "5432");
    ecewo_pg_pool_config_set_dbname(config, "mydb");
    ecewo_pg_pool_config_set_user(config, "postgres");
    ecewo_pg_pool_config_set_password(config, "secret");
    ecewo_pg_pool_config_set_pool_size(config, 10);
    ecewo_pg_pool_config_set_timeout_ms(config, 5000);

    pool = ecewo_pg_pool_create(config);
    ecewo_pg_pool_config_free(config);

    ECEWO_POST(app, "/users", create_user_with_post);

    ecewo_atexit(app, cleanup, NULL);
    ecewo_listen(app, 3000);

    return 0;
}
```

---

### Transaction Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static ecewo_pg_pool_t *pool = NULL;

static void on_transfer_complete(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    ecewo_response_t *res = (ecewo_response_t *)data;

    if (!ecewo_pg_result_ok(result)) {
        ecewo_send_json(res, ECEWO_INTERNAL_SERVER_ERROR,
                        "{\"error\":\"Transfer failed\"}");
        return;
    }

    ecewo_send_json(res, ECEWO_OK, "{\"status\":\"transferred\"}");
}

static void transfer_money(ecewo_request_t *req, ecewo_response_t *res) {
    const char *from_id = ecewo_param(req, "from");
    const char *to_id   = ecewo_param(req, "to");
    const char *amount  = ecewo_param(req, "amount");

    ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);

    const char *debit[]  = { amount, from_id };
    const char *credit[] = { amount, to_id };

    ecewo_pg_query_queue(pg,
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
        2, debit, NULL, NULL);

    ecewo_pg_query_queue(pg,
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        2, credit, on_transfer_complete, res);

    ecewo_pg_query_exec_trans(pg);
}

static void cleanup(void *data) { ecewo_pg_pool_destroy(pool); }

int main(void) {
    ecewo_app_t *app = ecewo_create();

    ecewo_pg_pool_config_t *config = ecewo_pg_pool_config_new();
    ecewo_pg_pool_config_set_app(config, app);
    ecewo_pg_pool_config_set_host(config, "localhost");
    ecewo_pg_pool_config_set_port(config, "5432");
    ecewo_pg_pool_config_set_dbname(config, "mydb");
    ecewo_pg_pool_config_set_user(config, "postgres");
    ecewo_pg_pool_config_set_password(config, "secret");
    ecewo_pg_pool_config_set_pool_size(config, 10);
    ecewo_pg_pool_config_set_timeout_ms(config, 5000);

    pool = ecewo_pg_pool_create(config);
    ecewo_pg_pool_config_free(config);

    ECEWO_POST(app, "/transfer", transfer_money);

    ecewo_atexit(app, cleanup, NULL);
    ecewo_listen(app, 3000);

    return 0;
}
```

---

### Parallel Querying Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static ecewo_pg_pool_t *pool = NULL;

typedef struct {
    ecewo_response_t *res;
    int user_count;
    int post_count;
    int comment_count;
} stats_ctx_t;

static void on_user_count(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    ((stats_ctx_t *)data)->user_count = atoi(ecewo_pg_result_get_value(result, 0, 0));
}

static void on_post_count(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    ((stats_ctx_t *)data)->post_count = atoi(ecewo_pg_result_get_value(result, 0, 0));
}

static void on_comment_count(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    ((stats_ctx_t *)data)->comment_count = atoi(ecewo_pg_result_get_value(result, 0, 0));
}

static void on_all_done(ecewo_pg_parallel_t *parallel, int success, void *data) {
    stats_ctx_t *ctx = (stats_ctx_t *)data;

    if (!success) {
        ecewo_send_text(ctx->res, ECEWO_INTERNAL_SERVER_ERROR, "Query failed");
        return;
    }

    ecewo_arena_t *arena = ecewo_res_arena(ctx->res);
    char *json = ecewo_sprintf(arena,
        "{\"users\":%d,\"posts\":%d,\"comments\":%d}",
        ctx->user_count, ctx->post_count, ctx->comment_count);

    ecewo_send_json(ctx->res, ECEWO_OK, json);
}

static void get_stats(ecewo_request_t *req, ecewo_response_t *res) {
    ecewo_arena_t *arena = ecewo_req_arena(req);
    stats_ctx_t *ctx = ecewo_alloc(arena, sizeof(stats_ctx_t));
    ctx->res = res;

    ecewo_pg_parallel_t *parallel = ecewo_pg_parallel_create(pool, 3, res);

    ecewo_pg_query_t *pg0 = ecewo_pg_parallel_get(parallel, 0);
    ecewo_pg_query_queue(pg0, "SELECT COUNT(*) FROM users",
                         0, NULL, on_user_count, ctx);

    ecewo_pg_query_t *pg1 = ecewo_pg_parallel_get(parallel, 1);
    ecewo_pg_query_queue(pg1, "SELECT COUNT(*) FROM posts",
                         0, NULL, on_post_count, ctx);

    ecewo_pg_query_t *pg2 = ecewo_pg_parallel_get(parallel, 2);
    ecewo_pg_query_queue(pg2, "SELECT COUNT(*) FROM comments",
                         0, NULL, on_comment_count, ctx);

    ecewo_pg_parallel_on_complete(parallel, on_all_done, ctx);
    ecewo_pg_parallel_exec(parallel);
}

static void cleanup(void *data) { ecewo_pg_pool_destroy(pool); }

int main(void) {
    ecewo_app_t *app = ecewo_create();

    ecewo_pg_pool_config_t *config = ecewo_pg_pool_config_new();
    ecewo_pg_pool_config_set_app(config, app);
    ecewo_pg_pool_config_set_host(config, "localhost");
    ecewo_pg_pool_config_set_port(config, "5432");
    ecewo_pg_pool_config_set_dbname(config, "mydb");
    ecewo_pg_pool_config_set_user(config, "postgres");
    ecewo_pg_pool_config_set_password(config, "secret");
    ecewo_pg_pool_config_set_pool_size(config, 10);
    ecewo_pg_pool_config_set_timeout_ms(config, 5000);

    pool = ecewo_pg_pool_create(config);
    ecewo_pg_pool_config_free(config);

    ECEWO_GET(app, "/stats", get_stats);

    ecewo_atexit(app, cleanup, NULL);
    ecewo_listen(app, 3000);

    return 0;
}
```

---

## Best Practices

### 1. Always Use Parameterized Queries

**Wrong: SQL injection risk:**
```c
char sql[256];
sprintf(sql, "SELECT * FROM users WHERE email = '%s'", email);
ecewo_pg_query_queue(pg, sql, 0, NULL, on_result, NULL);
```

**Correct:**
```c
const char *params[] = { email };
ecewo_pg_query_queue(pg, "SELECT * FROM users WHERE email = $1",
                     1, params, on_result, NULL);
```

---

### 2. Pass the Response Object to `ecewo_pg_query_create`

```c
static void handler(ecewo_request_t *req, ecewo_response_t *res) {
    // Pass res so the library can ref-count the client connection
    ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);

    ecewo_arena_t *arena = ecewo_req_arena(req);
    my_ctx_t *ctx = ecewo_alloc(arena, sizeof(my_ctx_t));
    ctx->res = res;

    ecewo_pg_query_queue(pg, "SELECT * FROM users", 0, NULL, callback, ctx);
    ecewo_pg_query_exec(pg);
}
```

For background jobs not tied to a request, pass `NULL`:
```c
ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, NULL);
```

---

### 3. Use `ecewo_pg_result_ok` to Check Query Status

```c
static void on_result(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    if (!ecewo_pg_result_ok(result)) {
        fprintf(stderr, "Query failed: %s\n", ecewo_pg_result_error_message(result));
        return;
    }

    // Process rows...
}
```

---

### 4. Use Transactions for Related Updates

**Wrong: two separate connections, no atomicity:**
```c
ecewo_pg_query_t *pg1 = ecewo_pg_query_create(pool, res);
ecewo_pg_query_queue(pg1, "UPDATE table1 ...", ...);
ecewo_pg_query_exec(pg1);

ecewo_pg_query_t *pg2 = ecewo_pg_query_create(pool, res);
ecewo_pg_query_queue(pg2, "UPDATE table2 ...", ...);
ecewo_pg_query_exec(pg2);
```

**Correct: single atomic transaction:**
```c
ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);
ecewo_pg_query_queue(pg, "UPDATE table1 ...", 2, params1, NULL, NULL);
ecewo_pg_query_queue(pg, "UPDATE table2 ...", 2, params2, on_done, res);
ecewo_pg_query_exec_trans(pg);
```

---

### 5. Configure Appropriate Pool Size

```c
ecewo_pg_pool_config_set_pool_size(config, 20);    // based on expected load
ecewo_pg_pool_config_set_timeout_ms(config, 5000); // fail fast under heavy load
```

---

### 6. Monitor Pool Health

```c
static void check_pool(void *data) {
    ecewo_pg_pool_t *pool = (ecewo_pg_pool_t *)data;

    if (ecewo_pg_pool_available(pool) == 0) {
        fprintf(stderr, "Pool exhausted! %d/%d in use\n",
                ecewo_pg_pool_in_use(pool),
                ecewo_pg_pool_total(pool));
    }

    ecewo_pg_pool_cleanup_idle(pool, 5 * 60 * 1000);
}

ecewo_interval(check_pool, 60000, pool);
```

---

## Error Handling

### Query Errors

```c
static void on_result(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    ecewo_response_t *res = (ecewo_response_t *)data;

    if (!ecewo_pg_result_ok(result)) {
        fprintf(stderr, "Query error: %s\n", ecewo_pg_result_error_message(result));
        ecewo_send_json(res, ECEWO_INTERNAL_SERVER_ERROR,
                        "{\"error\":\"Database error\"}");
        return;
    }

    // Process results...
}
```

---

### Connection Errors

```c
ecewo_pg_pool_t *pool = ecewo_pg_pool_create(config);
if (!pool) {
    fprintf(stderr, "Failed to create connection pool\n");
    exit(1);
}
```

---

### Pool Exhaustion

When all connections are busy and the timeout expires, the `on_complete` callback receives `NULL` for the query handle:

```c
static void on_complete(ecewo_pg_query_t *pg, void *data) {
    ecewo_response_t *res = (ecewo_response_t *)data;

    if (!pg) {
        ecewo_send_json(res, ECEWO_SERVICE_UNAVAILABLE,
                        "{\"error\":\"Database unavailable\"}");
        return;
    }
    // Normal completion path
}

ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);
ecewo_pg_query_on_complete(pg, on_complete, res);
ecewo_pg_query_queue(pg, "SELECT * FROM users", 0, NULL, on_users, res);
ecewo_pg_query_exec(pg);
```

---

## Memory Management

### Arena Integration

ecewo-postgres borrows its own arena for each query lifecycle internally. User code allocates context data in the request or response arena.

```c
static void handler(ecewo_request_t *req, ecewo_response_t *res) {
    ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);

    ecewo_arena_t *arena = ecewo_req_arena(req);
    my_ctx_t *ctx = ecewo_alloc(arena, sizeof(my_ctx_t));
    ctx->res = res;

    ecewo_pg_query_queue(pg, "SELECT * FROM users", 0, NULL, callback, ctx);
    ecewo_pg_query_exec(pg);
}
```

The library handles its own cleanup; the request/response arenas are freed automatically after the response is sent.

---

### Copy Result Values Before the Callback Returns

The `ecewo_pg_result_t *` pointer is only valid inside the result callback.

**Wrong: dangling pointer after callback returns:**
```c
static void on_result(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    ctx->name = ecewo_pg_result_get_value(result, 0, 0); // invalid after return
}
```

**Correct: copy into an arena:**
```c
static void on_result(ecewo_pg_query_t *pg, ecewo_pg_result_t *result, void *data) {
    my_ctx_t *ctx = (my_ctx_t *)data;
    ecewo_arena_t *arena = ecewo_res_arena(ctx->res);
    ctx->name = ecewo_strdup(arena, ecewo_pg_result_get_value(result, 0, 0));
}
```

---

## Performance Tips

### 1. One Global Pool

```c
static ecewo_pg_pool_t *pool = NULL;

int main(void) {
    pool = ecewo_pg_pool_create(config); // reused for the application lifetime
}
```

Never create a pool inside a request handler.

---

### 2. Queue Sequential Queries on One Handle

One connection, one `ecewo_pg_query_t`:

```c
ecewo_pg_query_t *pg = ecewo_pg_query_create(pool, res);
ecewo_pg_query_queue(pg, "SELECT * FROM users WHERE id = $1", ...);
ecewo_pg_query_queue(pg, "SELECT * FROM posts WHERE user_id = $1", ...);
ecewo_pg_query_queue(pg, "SELECT COUNT(*) FROM comments WHERE user_id = $1", ...);
ecewo_pg_query_exec(pg);
```

Avoid creating multiple handles for dependent queries. Use query chaining instead.

---

### 3. Use Parallel Queries for Independent Data

```c
ecewo_pg_parallel_t *parallel = ecewo_pg_parallel_create(pool, 3, res);

ecewo_pg_query_queue(ecewo_pg_parallel_get(parallel, 0), "SELECT COUNT(*) FROM users", ...);
ecewo_pg_query_queue(ecewo_pg_parallel_get(parallel, 1), "SELECT COUNT(*) FROM posts", ...);
ecewo_pg_query_queue(ecewo_pg_parallel_get(parallel, 2), "SELECT COUNT(*) FROM comments", ...);

ecewo_pg_parallel_exec(parallel); // all three run concurrently
```

---

## Troubleshooting

### Connection Refused
```
[ERROR] Connection failed: could not connect to server
```

- Verify PostgreSQL is running: `systemctl status postgresql`
- Check host/port values in your config
- Check `pg_hba.conf` for access permissions
- Check firewall rules

---

### Pool Exhausted

Increase `pool_size`, set a positive `timeout_ms`, or investigate slow queries:

```sql
SELECT pid, query, state, wait_event
FROM pg_stat_activity
WHERE state != 'idle';
```

---

### Queries Appear to Hang

Check active connections and terminate stuck ones:

```sql
SELECT pid, query, state FROM pg_stat_activity WHERE state != 'idle';
SELECT pg_terminate_backend(pid);
```

Also inspect pool state:
```c
fprintf(stderr, "available: %d  in_use: %d\n",
        ecewo_pg_pool_available(pool),
        ecewo_pg_pool_in_use(pool));
```

---

## License

Licensed under [MIT](./LICENSE)

---
