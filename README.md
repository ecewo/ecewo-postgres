# ecewo-postgres

**Fully asynchronous PostgreSQL client for [ecewo](https://github.com/savashn/ecewo)**

ecewo-postgres is a non-blocking PostgreSQL client library designed specifically for [ecewo](https://github.com/savashn/ecewo). It provides connection pooling, async query execution, transaction support, and parallel query capabilities—all fully integrated with ecewo's event loop and arena allocator.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [Connection Pool](#connection-pool)
  - [Query Execution](#query-execution)
  - [Transactions](#transactions)
  - [Parallel Queries](#parallel-queries)
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

### Add Plugin

```shell
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

static PGpool *pool = NULL;

// Result callback
static void on_users(PGquery *pg, PGresult *result, void *data) {
    Res *res = (Res *)data;
    
    int rows = PQntuples(result);
    char *json = arena_sprintf(res->arena, "[");
    
    for (int i = 0; i < rows; i++) {
        char *name = PQgetvalue(result, i, 0);
        char *item = arena_sprintf(res->arena,
            "%s{\"name\":\"%s\"}",
            i > 0 ? "," : "", name
        );
        json = arena_sprintf(res->arena, "%s%s", json, item);
    }
    
    json = arena_sprintf(res->arena, "%s]", json);
    send_json(res, OK, json);
}

// Route handler
static void get_users(Req *req, Res *res) {
    PGquery *pg = pg_query_create(pool, res);
    
    pg_query_queue(pg, "SELECT name FROM users ORDER BY name",
                   0, NULL, on_users, res);
    
    pg_query_exec(pg);
}

void cleanup(void) {
    pg_pool_destroy(pool);
}

int main(void) {
    server_init();
    
    // Create connection pool
    PGPoolConfig config = {
        .host = "localhost",
        .port = "5432",
        .dbname = "mydb",
        .user = "postgres",
        .password = "secret",
        .pool_size = 10,
        .timeout_ms = 5000
    };
    
    pool = pg_pool_create(&config);
    
    // Register routes
    get("/api/users", get_users);
    
    // Cleanup on shutdown
    server_atexit(cleanup);
    
    server_listen(3000);
    server_run();
    
    return 0;
}
```

---

## API Reference

### Connection Pool

#### `pg_pool_create`

Creates a connection pool with the specified configuration.
```c
PGpool *pg_pool_create(PGPoolConfig *config);
```

**Parameters:**
- `config` - Pool configuration

**Configuration Structure:**
```c
typedef struct {
    const char *host;      // Database host (e.g., "localhost")
    const char *port;      // Port number (e.g., "5432")
    const char *dbname;    // Database name
    const char *user;      // Username
    const char *password;  // Password
    int pool_size;         // Number of connections (1-1024)
    int timeout_ms;        // 0 = no wait, -1 = infinite, >0 = wait up to N milliseconds
} PGPoolConfig;
```

**Returns:** Pool handle or `NULL` on failure

**Example:**
```c
PGPoolConfig config = {
    .host = "localhost",
    .port = "5432",
    .dbname = "myapp",
    .user = "postgres",
    .password = "secret",
    .pool_size = 20,
    .timeout_ms = 5000
};

PGpool *pool = pg_pool_create(&config);
if (!pool) {
    fprintf(stderr, "Failed to create pool\n");
    exit(1);
}
```

---

#### `pg_pool_destroy`

Destroys the pool and closes all connections.
```c
void pg_pool_destroy(PGpool *pool);
```

**Parameters:**
- `pool` - Pool to destroy

**Example:**
```c
void cleanup(void) {
    pg_pool_destroy(pool);
}

server_atexit(cleanup);
```

---

#### `pg_pool_get_stats`

Retrieves current pool statistics.
```c
void pg_pool_get_stats(PGpool *pool, PGPoolStats *stats);
```

**Parameters:**
- `pool` - Connection pool
- `stats` - Output statistics structure

**Statistics Structure:**
```c
typedef struct {
    int total;      // Total connections in pool
    int available;  // Available connections
    int in_use;     // Connections currently in use
} PGPoolStats;
```

**Example:**
```c
static void pool_status(Req *req, Res *res) {
    PGPoolStats stats;
    pg_pool_get_stats(pool, &stats);
    
    char *text = arena_sprintf(res->arena,
        "total: %d, available: %d, in_use: %d",
        stats.total, stats.available, stats.in_use
    );
    
    send_text(res, OK, text);
}
```

---

#### `pg_pool_cleanup_idle`

Resets connections that have been idle for too long.
```c
int pg_pool_cleanup_idle(PGpool *pool, uint64_t max_idle_ms);
```

**Parameters:**
- `pool` - Connection pool
- `max_idle_ms` - Maximum idle time in milliseconds

**Returns:** Number of connections reset

**Example:**
```c
// Reset connections idle for more than 30 minutes
void cleanup_callback(void *data) {
    PGpool *pool = (PGpool *)data;
    int reset = pg_pool_cleanup_idle(pool, 30 * 60 * 1000);
    
    if (reset > 0) {
        printf("Reset %d idle connections\n", reset);
    }
}

// Run every 15 minutes
set_interval(cleanup_callback, 15 * 60 * 1000, pool);
```

---

### Query Execution

#### `pg_query_create`

Creates a query context for executing queries.
```c
PGquery *pg_query_create(PGpool *pool, Res *res);
```

**Parameters:**
- `pool` - Connection pool
- `res` - Response object (or `NULL` for non-HTTP usage)

**Returns:** Query handle or `NULL` on failure

**How it works:**
- Library automatically borrows its own arena for the query lifecycle
- If `res` is provided, tracks the client connection via ref-counting
- Prevents crashes if client disconnects during async query execution
- Automatically cleans up resources when query completes

**Example (HTTP handler):**
```c
static void handler(Req *req, Res *res) {
    PGquery *pg = pg_query_create(pool, res);
    
    // Allocate context data in request arena
    my_ctx_t *ctx = arena_alloc(req->arena, sizeof(my_ctx_t));
    ctx->res = res;
    
    pg_query_queue(pg, "SELECT * FROM users", 0, NULL, callback, ctx);
    pg_query_exec(pg);
}
```

**Example (non-HTTP usage - background jobs, CLI tools):**
```c
// Pass NULL when not in HTTP request context
PGquery *pg = pg_query_create(pool, NULL);
pg_query_queue(pg, "DELETE FROM old_logs WHERE created_at < NOW() - INTERVAL '30 days'",
               0, NULL, NULL, NULL);
pg_query_exec(pg);
```

---

#### `pg_query_queue`

Queues a SQL command for execution.
```c
int pg_query_queue(PGquery *pg,
                   const char *sql,
                   int param_count,
                   const char **params,
                   pg_result_cb_t result_cb,
                   void *query_data);
```

**Parameters:**
- `pg` - Query context
- `sql` - SQL command (use `$1`, `$2`, etc. for parameters)
- `param_count` - Number of parameters
- `params` - Array of parameter values (strings)
- `result_cb` - Result callback function (or `NULL`)
- `query_data` - User data passed to callback

**Result Callback:**
```c
typedef void (*pg_result_cb_t)(PGquery *pg, PGresult *result, void *data);
```

**Returns:** `0` on success, `-1` on failure

**Examples:**

**Query without parameters:**
```c
pg_query_queue(pg, "SELECT COUNT(*) FROM users",
               0, NULL, on_count, res);
```

**Query with parameters:**
```c
const char *params[] = { user_id };
pg_query_queue(pg, "SELECT * FROM users WHERE id = $1",
               1, params, on_user, res);
```

**Multiple parameters:**
```c
const char *params[] = { email, password_hash };
pg_query_queue(pg, 
    "SELECT id FROM users WHERE email = $1 AND password = $2",
    2, params, on_login, res);
```

---

#### `pg_query_exec`

Executes all queued queries asynchronously.
```c
int pg_query_exec(PGquery *pg);
```

**Parameters:**
- `pg` - Query context

**Returns:** `0` on success, `-1` on failure

**Example:**
```c
PGquery *pg = pg_query_create(pool, res);

pg_query_queue(pg, "SELECT * FROM products",
               0, NULL, on_products, res);

pg_query_exec(pg);  // Non-blocking
```

---

#### `pg_query_on_complete`

Sets a callback to be invoked when all queries complete.
```c
void pg_query_on_complete(PGquery *pg, pg_complete_cb_t callback, void *data);
```

**Parameters:**
- `pg` - Query context
- `callback` - Completion callback
- `data` - User data

**Completion Callback:**
```c
typedef void (*pg_complete_cb_t)(PGquery *pg, void *data);
```

**Example:**
```c
static void on_complete(PGquery *pg, void *data) {
    Res *res = (Res *)data;
    send_text(res, OK, "All queries completed");
}

PGquery *pg = pg_query_create(pool, res);
pg_query_on_complete(pg, on_complete, res);
pg_query_queue(pg, "SELECT * FROM users", 0, NULL, on_users, res);
pg_query_exec(pg);
```

---

### Transactions

#### `pg_query_exec_trans`

Executes all queued queries wrapped in a transaction.
```c
int pg_query_exec_trans(PGquery *pg);
```

**Parameters:**
- `pg` - Query context with queued queries

**Returns:** `0` on success, `-1` on failure

**Behavior:**
- Automatically prepends `BEGIN`
- Automatically appends `COMMIT`
- PostgreSQL rolls back automatically on any query failure

**Example:**
```c
static void transfer_money(Req *req, Res *res) {
    const char *from_id = "1";
    const char *to_id = "2";
    const char *amount = "100.00";
    
    PGquery *pg = pg_query_create(pool, res);
    
    // Deduct from sender
    const char *debit_params[] = { amount, from_id };
    pg_query_queue(pg,
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
        2, debit_params, NULL, NULL);
    
    // Add to receiver
    const char *credit_params[] = { amount, to_id };
    pg_query_queue(pg,
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        2, credit_params, on_transfer_done, res);
    
    // Execute as transaction (BEGIN...COMMIT)
    pg_query_exec_trans(pg);
}

static void on_transfer_done(PGquery *pg, PGresult *result, void *data) {
    Res *res = (Res *)data;
    send_json(res, OK, "{\"status\":\"transferred\"}");
}
```

**Manual Transaction Control:**

For advanced use cases (savepoints, isolation levels):
```c
pg_query_queue(pg, "BEGIN", 0, NULL, NULL, NULL);
pg_query_queue(pg, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", 
               0, NULL, NULL, NULL);
pg_query_queue(pg, "UPDATE accounts ...", 2, params1, NULL, NULL);
pg_query_queue(pg, "SAVEPOINT sp1", 0, NULL, NULL, NULL);
pg_query_queue(pg, "UPDATE logs ...", 1, params2, NULL, NULL);
pg_query_queue(pg, "COMMIT", 0, NULL, on_complete, res);
pg_query_exec(pg);
```

---

### Parallel Queries

Execute multiple independent queries concurrently using separate connections.

#### `pg_parallel_create`

Creates a parallel query context.
```c
PGparallel *pg_parallel_create(PGpool *pool, int count, Res *res);
```

**Parameters:**
- `pool` - Connection pool
- `count` - Number of parallel streams
- `res` - Response object (or `NULL` for non-HTTP usage)

**Returns:** Parallel context or `NULL` on failure

---

#### `pg_parallel_get`

Gets a query stream for a specific index.
```c
PGquery *pg_parallel_get(PGparallel *parallel, int index);
```

**Parameters:**
- `parallel` - Parallel context
- `index` - Stream index (0 to count-1)

**Returns:** Query handle for the stream

---

#### `pg_parallel_on_complete`

Sets a callback for when all parallel queries complete.
```c
void pg_parallel_on_complete(PGparallel *parallel,
                              pg_parallel_cb_t callback,
                              void *data);
```

**Callback Signature:**
```c
typedef void (*pg_parallel_cb_t)(PGparallel *parallel, int success, void *data);
```

**Parameters:**
- `success` - `1` if all succeeded, `0` if any failed

---

#### `pg_parallel_exec`

Executes all parallel queries concurrently.
```c
int pg_parallel_exec(PGparallel *parallel);
```

**Returns:** `0` on success, `-1` on failure

---

#### `pg_parallel_count`

Returns the number of parallel streams.
```c
int pg_parallel_count(PGparallel *parallel);
```

**Returns:** Number of streams

---

## Examples

### Simple Select Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static PGpool *pool = NULL;

static void on_user(PGquery *pg, PGresult *result, void *data) {
    Res *res = (Res *)data;
    
    if (PQntuples(result) == 0) {
        send_text(res, NOT_FOUND, "User not found");
        return;
    }
    
    const char *name = PQgetvalue(result, 0, 0);
    const char *email = PQgetvalue(result, 0, 1);
    
    char *response = arena_sprintf(res->arena,
        "name: %s email: %s",
        name, email
    );
    
    send_text(res, OK, response);
}

static void get_user(Req *req, Res *res) {
    const char *id = get_param(req, "id");
    
    PGquery *pg = pg_query_create(pool, res);
    
    const char *params[] = { id };
    pg_query_queue(pg,
        "SELECT name, email FROM users WHERE id = $1",
        1, params, on_user, res);
    
    pg_query_exec(pg);
}

void cleanup(void) {
    pg_pool_destroy(pool);
}

int main(void) {
    server_init();
    
    PGPoolConfig config = {
        .host = "localhost",
        .port = "5432",
        .dbname = "mydb",
        .user = "postgres",
        .password = "secret",
        .pool_size = 10,
        .timeout_ms = 5000
    };
    
    pool = pg_pool_create(&config);
    
    get("/user", get_user);
    
    server_atexit(cleanup);
    server_listen(3000);
    server_run();
    
    return 0;
}
```

### Insert Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static PGpool *pool = NULL;

static void on_user_created(PGquery *pg, PGresult *result, void *data) {
    Res *res = (Res *)data;
    const char *id = PQgetvalue(result, 0, 0);
    char *text = arena_sprintf(res->arena, "id: %s", id);
    send_text(res, CREATED, text);
}

static void create_user(Req *req, Res *res) {
    // Parse from req->body (simplified)
    const char *name = "John Doe";
    const char *email = "john@example.com";
    
    PGquery *pg = pg_query_create(pool, res);
    
    const char *params[] = { name, email };
    pg_query_queue(pg,
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
        2, params, on_user_created, res);
    
    pg_query_exec(pg);
}

void cleanup(void) {
    pg_pool_destroy(pool);
}

int main(void) {
    server_init();
    
    PGPoolConfig config = {
        .host = "localhost",
        .port = "5432",
        .dbname = "mydb",
        .user = "postgres",
        .password = "secret",
        .pool_size = 10,
        .timeout_ms = 5000
    };
    
    pool = pg_pool_create(&config);
    
    post("/user", create_user);
    
    server_atexit(cleanup);
    server_listen(3000);
    server_run();
    
    return 0;
}
```

### Multiple Queries Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static PGpool *pool = NULL;

typedef struct {
    Res *res;
    char *user_name;
    char *user_email;
    char *posts_json;
} profile_ctx_t;

static void on_user_info(PGquery *pg, PGresult *result, void *data) {
    profile_ctx_t *ctx = (profile_ctx_t *)data;
    
    if (PQntuples(result) == 0) {
        send_text(ctx->res, NOT_FOUND, "Error: User not found");
        return;
    }
    
    ctx->user_name = arena_strdup(ctx->res->arena, PQgetvalue(result, 0, 0));
    ctx->user_email = arena_strdup(ctx->res->arena, PQgetvalue(result, 0, 1));
}

static void on_user_posts(PGquery *pg, PGresult *result, void *data) {
    profile_ctx_t *ctx = (profile_ctx_t *)data;
    
    int count = PQntuples(result);

    // Use a json parser here (like cJSON or jansson)
    // This is just an example
    char *json = arena_sprintf(ctx->res->arena, "[");
    
    for (int i = 0; i < count; i++) {
        char *title = PQgetvalue(result, i, 0);
        char *item = arena_sprintf(ctx->res->arena,
            "%s{\"title\":\"%s\"}",
            i > 0 ? "," : "", title
        );
        json = arena_sprintf(ctx->res->arena, "%s%s", json, item);
    }
    
    ctx->posts_json = arena_sprintf(ctx->res->arena, "%s]", json);
}

static void on_user_comments(PGquery *pg, PGresult *result, void *data) {
    profile_ctx_t *ctx = (profile_ctx_t *)data;
    
    int comment_count = PQntuples(result);
    
    // All queries done - build final response
    char *json = arena_sprintf(ctx->res->arena,
        "{"
        "\"name\":\"%s\","
        "\"email\":\"%s\","
        "\"posts\":%s,"
        "\"comment_count\":%d"
        "}",
        ctx->user_name, ctx->user_email, ctx->posts_json, comment_count
    );
    
    send_json(ctx->res, OK, json);
}

static void get_user_profile(Req *req, Res *res) {
    const char *user_id = get_param(req, "id");
    
    profile_ctx_t *ctx = arena_alloc(req->arena, sizeof(profile_ctx_t));
    ctx->res = res;
    
    PGquery *pg = pg_query_create(pool, res);
    
    const char *params[] = { user_id };
    
    // Query 1: Get user basic info
    pg_query_queue(pg,
        "SELECT name, email FROM users WHERE id = $1",
        1, params, on_user_info, ctx);
    
    // Query 2: Get user's posts
    pg_query_queue(pg,
        "SELECT title FROM posts WHERE user_id = $1 ORDER BY created_at DESC LIMIT 5",
        1, params, on_user_posts, ctx);
    
    // Query 3: Count user's comments
    pg_query_queue(pg,
        "SELECT COUNT(*) FROM comments WHERE user_id = $1",
        1, params, on_user_comments, ctx);
    
    // All queries execute sequentially on the same connection
    pg_query_exec(pg);
}

// Execution Flow:

// Connection acquired from pool
//
// Query 1: SELECT name, email FROM users WHERE id = 123
//     on_user_info() -> stores user data in ctx
//
// Query 2: SELECT title FROM posts WHERE user_id = 123
//     on_user_posts() -> stores posts JSON in ctx
//
// Query 3: SELECT COUNT(*) FROM comments WHERE user_id = 123
//     on_user_comments() -> assembles final response from ctx
//
// Connection returned to pool

void cleanup(void) {
    pg_pool_destroy(pool);
}

int main(void) {
    server_init();
    
    PGPoolConfig config = {
        .host = "localhost",
        .port = "5432",
        .dbname = "mydb",
        .user = "postgres",
        .password = "secret",
        .pool_size = 10,
        .timeout_ms = 5000
    };
    
    pool = pg_pool_create(&config);
    
    get("/users/:id/profile", get_user_profile);
    
    server_atexit(cleanup);
    server_listen(3000);
    server_run();
    
    return 0;
}
```

### Query Chaining Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static PGpool *pool = NULL;

typedef struct {
    Res *res;
    const char *user_id;
} chain_ctx_t;

static void on_posts_inserted(PGquery *pg, PGresult *result, void *data) {
    chain_ctx_t *ctx = (chain_ctx_t *)data;
    send_text(ctx->res, CREATED, "Success!");
}

static void on_user_created(PGquery *pg, PGresult *result, void *data) {
    chain_ctx_t *ctx = (chain_ctx_t *)data;
    
    // Get user ID from result
    ctx->user_id = PQgetvalue(result, 0, 0);
    
    // Queue next query - automatically continues
    const char *params[] = { ctx->user_id, "First Post" };
    pg_query_queue(pg,
        "INSERT INTO posts (user_id, title) VALUES ($1, $2)",
        2, params, on_posts_inserted, ctx);
}

static void create_user_with_post(Req *req, Res *res) {
    chain_ctx_t *ctx = arena_alloc(req->arena, sizeof(chain_ctx_t));
    ctx->res = res;
    
    PGquery *pg = pg_query_create(pool, res);
    
    const char *params[] = { "John Doe", "john@example.com" };
    pg_query_queue(pg,
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
        2, params, on_user_created, ctx);
    
    pg_query_exec(pg);  // Execute first query
}

// Execution Flow:
//
// pg_query_exec(pg)
//
// Query 1: INSERT INTO users ... RETURNING id
//     on_user_created() called
//       ├─ Extracts user_id
//       └─ Queues Query 2
//   
// [callback returns]
//
// Check if pg->query_queue is not empty
//
// execute_next_query(pg) automatically called
//
// Query 2: INSERT INTO posts ...
//
// on_posts_inserted() called
//
// [callback returns]
//
// No more queries -> cleanup

void cleanup(void) {
    pg_pool_destroy(pool);
}

int main(void) {
    server_init();
    
    PGPoolConfig config = {
        .host = "localhost",
        .port = "5432",
        .dbname = "mydb",
        .user = "postgres",
        .password = "secret",
        .pool_size = 10,
        .timeout_ms = 5000
    };
    
    pool = pg_pool_create(&config);
    
    post("/post", create_user_with_post);
    
    server_atexit(cleanup);
    server_listen(3000);
    server_run();
    return 0;
}
```

### Transaction Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static PGpool *pool = NULL;

static void on_transfer_complete(PGquery *pg, PGresult *result, void *data) {
    Res *res = (Res *)data;
    
    ExecStatusType status = PQresultStatus(result);
    if (status != PGRES_COMMAND_OK) {
        send_json(res, INTERNAL_SERVER_ERROR,
                 "{\"error\":\"Transfer failed\"}");
        return;
    }
    
    send_json(res, OK, "{\"status\":\"transferred\"}");
}

static void transfer_money(Req *req, Res *res) {
    const char *from_id = get_param(req, "from");
    const char *to_id = get_param(req, "to");
    const char *amount = get_param(req, "amount");
    
    PGquery *pg = pg_query_create(pool, res);
    
    // Deduct from sender
    const char *debit_params[] = { amount, from_id };
    pg_query_queue(pg,
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
        2, debit_params, NULL, NULL);
    
    // Add to receiver
    const char *credit_params[] = { amount, to_id };
    pg_query_queue(pg,
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        2, credit_params, on_transfer_complete, res);
    
    // Execute as transaction
    pg_query_exec_trans(pg);
}

void cleanup(void) {
    pg_pool_destroy(pool);
}

int main(void) {
    server_init();
    
    PGPoolConfig config = {
        .host = "localhost",
        .port = "5432",
        .dbname = "mydb",
        .user = "postgres",
        .password = "secret",
        .pool_size = 10,
        .timeout_ms = 5000
    };
    
    pool = pg_pool_create(&config);
    
    post("/transfer", transfer_money);
    
    server_atexit(cleanup);
    server_listen(3000);
    server_run();
    
    return 0;
}
```

### Parallel Querying Example

```c
#include "ecewo.h"
#include "ecewo-postgres.h"

static PGpool *pool = NULL;

typedef struct {
    Res *res;
    int user_count;
    int post_count;
    int comment_count;
} stats_ctx_t;

static void on_user_count(PGquery *pg, PGresult *result, void *data) {
    stats_ctx_t *ctx = (stats_ctx_t *)data;
    ctx->user_count = atoi(PQgetvalue(result, 0, 0));
}

static void on_post_count(PGquery *pg, PGresult *result, void *data) {
    stats_ctx_t *ctx = (stats_ctx_t *)data;
    ctx->post_count = atoi(PQgetvalue(result, 0, 0));
}

static void on_comment_count(PGquery *pg, PGresult *result, void *data) {
    stats_ctx_t *ctx = (stats_ctx_t *)data;
    ctx->comment_count = atoi(PQgetvalue(result, 0, 0));
}

static void on_all_stats_done(PGparallel *parallel, int success, void *data) {
    stats_ctx_t *ctx = (stats_ctx_t *)data;
    
    if (!success) {
        send_text(ctx->res, INTERNAL_SERVER_ERROR, 
                 "Query failed");
        return;
    }
    
    char *json = arena_sprintf(ctx->res->arena,
        "{\"users\":%d,\"posts\":%d,\"comments\":%d}",
        ctx->user_count, ctx->post_count, ctx->comment_count
    );
    
    send_json(ctx->res, OK, json);
}

static void get_stats(Req *req, Res *res) {
    stats_ctx_t *ctx = arena_alloc(req->arena, sizeof(stats_ctx_t));
    ctx->res = res;
    
    // Create parallel context with 3 streams
    PGparallel *parallel = pg_parallel_create(pool, 3, res);
    
    // Stream 0: Count users
    PGquery *pg0 = pg_parallel_get(parallel, 0);
    pg_query_queue(pg0, "SELECT COUNT(*) FROM users",
                   0, NULL, on_user_count, ctx);
    
    // Stream 1: Count posts
    PGquery *pg1 = pg_parallel_get(parallel, 1);
    pg_query_queue(pg1, "SELECT COUNT(*) FROM posts",
                   0, NULL, on_post_count, ctx);
    
    // Stream 2: Count comments
    PGquery *pg2 = pg_parallel_get(parallel, 2);
    pg_query_queue(pg2, "SELECT COUNT(*) FROM comments",
                   0, NULL, on_comment_count, ctx);
    
    // Set completion callback
    pg_parallel_on_complete(parallel, on_all_stats_done, ctx);
    
    // Execute all in parallel
    pg_parallel_exec(parallel);
}

// **Execution Flow:**

// 3 connections acquired from pool simultaneously
//
// Query 1: SELECT COUNT(*) FROM users     (Connection 1)
// Query 2: SELECT COUNT(*) FROM posts     (Connection 2)
// Query 3: SELECT COUNT(*) FROM comments  (Connection 3)
//
// All complete -> on_all_stats_done() -> send response
//
// All 3 connections returned to pool

void cleanup(void) {
    pg_pool_destroy(pool);
}

int main(void) {
    server_init();
    
    PGPoolConfig config = {
        .host = "localhost",
        .port = "5432",
        .dbname = "mydb",
        .user = "postgres",
        .password = "secret",
        .pool_size = 10,
        .timeout_ms = 5000
    };
    
    pool = pg_pool_create(&config);
    
    post("/stats", get_stats);
    
    server_atexit(cleanup);
    
    server_listen(3000);
    server_run();
    
    return 0;
}
```

## Best Practices

### 1. Always Use Parameterized Queries

**DO NOT DO THIS:**
```c
char sql[256];
sprintf(sql, "SELECT * FROM users WHERE email = '%s'", email);
pg_query_queue(pg, sql, 0, NULL, on_result, NULL);
```

**DO THIS:**
```c
const char *params[] = { email };
pg_query_queue(pg, "SELECT * FROM users WHERE email = $1",
               1, params, on_result, NULL);
```

---

### 2. Pass Response Object to pg_query_create

**Recommended (HTTP handlers):**
```c
static void handler(Req *req, Res *res) {
    // Pass res - library manages arena and client lifecycle automatically
    PGquery *pg = pg_query_create(pool, res);
    
    // Allocate context in request arena
    my_ctx_t *ctx = arena_alloc(req->arena, sizeof(my_ctx_t));
    ctx->res = res;
    
    pg_query_queue(pg, "SELECT * FROM users", 0, NULL, callback, ctx);
    pg_query_exec(pg);
}
```

**For non-HTTP usage:**
```c
// Background jobs, CLI tools, etc.
PGquery *pg = pg_query_create(pool, NULL);
```

---

### 3. Check Query Status
```c
static void on_result(PGquery *pg, PGresult *result, void *data) {
    ExecStatusType status = PQresultStatus(result);
    
    switch (status) {
        case PGRES_TUPLES_OK:
            // SELECT succeeded
            break;
        case PGRES_COMMAND_OK:
            // INSERT/UPDATE/DELETE succeeded
            break;
        default:
            fprintf(stderr, "Query failed: %s\n", 
                   PQresultErrorMessage(result));
            break;
    }
}
```

---

### 4. Use Transactions for Related Updates

**DO NOT DO THIS:**
```c
PGquery *pg1 = pg_query_create(pool, res);
pg_query_queue(pg1, "UPDATE table1 ...", ...);
pg_query_exec(pg1);

PGquery *pg2 = pg_query_create(pool, res);
pg_query_queue(pg2, "UPDATE table2 ...", ...);
pg_query_exec(pg2);
```

**DO THIS:**
```c
PGquery *pg = pg_query_create(pool, res);

pg_query_queue(pg, "UPDATE table1 ...", 2, params1, NULL, NULL);
pg_query_queue(pg, "UPDATE table2 ...", 2, params2, NULL, NULL);
pg_query_queue(pg, "INSERT INTO logs ...", 1, params3, on_done, res);

pg_query_exec_trans(pg);
```

---

### 5. Configure Appropriate Pool Size
```c
PGPoolConfig config = {
    // ...
    .pool_size = 20,     // Based on expected load
    .timeout_ms = 5000   // Fail fast under heavy load
};
```

---

### 6. Monitor Pool Health
```c
void check_pool_health(void *data) {
    PGpool *pool = (PGpool *)data;
    
    PGPoolStats stats;
    pg_pool_get_stats(pool, &stats);
    
    if (stats.available == 0) {
        fprintf(stderr, "Pool exhausted! %d/%d in use\n",
               stats.in_use, stats.total);
    }
    
    // Cleanup idle connections (5 minutes idle)
    pg_pool_cleanup_idle(pool, 5 * 60 * 1000);
}

// Check every minute
set_interval(check_pool_health, 60000, pool);
```

---

## Error Handling

### Query Errors

Errors are logged automatically. Handle them in result callbacks:
```c
static void on_result(PGquery *pg, PGresult *result, void *data) {
    Res *res = (Res *)data;
    ExecStatusType status = PQresultStatus(result);
    
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        const char *error = PQresultErrorMessage(result);
        fprintf(stderr, "Query error: %s\n", error);
        
        send_json(res, INTERNAL_SERVER_ERROR,
                 "{\"error\":\"Database error\"}");
        return;
    }
    
    // Process results...
}
```

---

### Connection Errors
```c
PGpool *pool = pg_pool_create(&config);
if (!pool) {
    fprintf(stderr, "Failed to create connection pool\n");
    fprintf(stderr, "Check: PostgreSQL running? Config correct?\n");
    exit(1);
}
```

---

### Pool Exhaustion

When all connections are in use and timeout expires:
```c
static void on_complete(PGquery *pg, void *data) {
    if (!pg) {
        // Connection acquisition failed (timeout or pool destroyed)
        Res *res = (Res *)data;
        send_json(res, SERVICE_UNAVAILABLE,
                 "{\"error\":\"Database unavailable\"}");
        return;
    }
    
    // Normal completion
}

PGquery *pg = pg_query_create(pool, res);
pg_query_on_complete(pg, on_complete, res);
pg_query_queue(pg, "SELECT * FROM users", 0, NULL, on_users, res);
pg_query_exec(pg);
```

---

## Memory Management

### Arena Integration

ecewo-postgres manages its own arena lifecycle automatically:

**HTTP Handlers:**
```c
static void handler(Req *req, Res *res) {
    // Pass res - library borrows its own arena internally
    PGquery *pg = pg_query_create(pool, res);
    
    // Allocate context data in request arena
    my_ctx_t *ctx = arena_alloc(req->arena, sizeof(my_ctx_t));
    ctx->res = res;
    
    pg_query_queue(pg, "SELECT * FROM users", 0, NULL, callback, ctx);
    pg_query_exec(pg);
}
```

**How it works:**
1. Library borrows its own arena for `PGquery` lifecycle
2. Library tracks client connection via ref-counting
3. Even if client disconnects during async query, no crash occurs
4. Arena and resources are cleaned up when query completes

**Non-HTTP Usage:**
```c
// Pass NULL for res when not in HTTP context
PGquery *pg = pg_query_create(pool, NULL);
pg_query_queue(pg, "DELETE FROM old_logs WHERE created_at < $1",
               1, params, NULL, NULL);
pg_query_exec(pg);
```

---

### Connection Lifecycle
```
1. pg_query_exec() called
2. Connection request queued (non-blocking)
3. Connection acquired from pool (callback-based)
4. Queries executed sequentially
5. Connection automatically returned to pool
6. Arena freed automatically by library
```

Request/response arenas (`req->arena`, `res->arena`) are freed automatically after the response is sent.

---

### Memory Safety

**Unsafe - Direct pointer:**
```c
static void on_result(PGquery *pg, PGresult *result, void *data) {
    // This pointer becomes INVALID after callback returns
    const char *name = PQgetvalue(result, 0, 0);
}
```

**Safe - Copy to arena:**
```c
static void on_result(PGquery *pg, PGresult *result, void *data) {
    Res *res = (Res *)data;
    
    // Copy to response arena - safe after callback returns
    char *name = arena_strdup(res->arena, PQgetvalue(result, 0, 0));
    
    // Use name later...
}
```

---

## Performance Tips

### 1. Use Connection Pooling

**Good - One global pool:**
```c
static PGpool *pool = NULL;

int main(void) {
    pool = pg_pool_create(&config);
    // Use pool throughout application lifetime
}
```

**Bad - Creating pools per request:**
```c
static void handler(Req *req, Res *res) {
    PGpool *pool = pg_pool_create(&config);
}
```

---

### 2. Use Single Connection for Sequential Queries

**Good - One connection:**
```c
PGquery *pg = pg_query_create(pool, res);
pg_query_queue(pg, "SELECT * FROM users WHERE id = $1", ...);
pg_query_queue(pg, "SELECT * FROM posts WHERE user_id = $1", ...);
pg_query_queue(pg, "SELECT COUNT(*) FROM comments WHERE user_id = $1", ...);
pg_query_exec(pg);  // Uses one connection
```

**Less efficient - Multiple connections:**
```c
// Each execution acquires a separate connection
PGquery *pg1 = pg_query_create(pool, res);
pg_query_queue(pg1, "SELECT * FROM users ...", ...);
pg_query_exec(pg1);  // Connection 1

PGquery *pg2 = pg_query_create(pool, res);
pg_query_queue(pg2, "SELECT * FROM posts ...", ...);
pg_query_exec(pg2);  // Connection 2 (unnecessary)
```

**When to use multiple connections:**
- Executing queries in **parallel** (use `PGparallel`)
- Second query depends on first query's **result** (dynamic chaining)

---

### 3. Use Parallel Queries for Independent Data

**Parallel execution:**
```c
PGparallel *parallel = pg_parallel_create(pool, 3, res);

PGquery *pg0 = pg_parallel_get(parallel, 0);
pg_query_queue(pg0, "SELECT FROM table1 ...", ...);

PGquery *pg1 = pg_parallel_get(parallel, 1);
pg_query_queue(pg1, "SELECT FROM table2 ...", ...);

PGquery *pg2 = pg_parallel_get(parallel, 2);
pg_query_queue(pg2, "SELECT FROM table3 ...", ...);

pg_parallel_exec(parallel);  // All execute concurrently
```

**Sequential execution is slower:**
```c
PGquery *pg = pg_query_create(pool, res);
pg_query_queue(pg, "SELECT FROM table1 ...", ...);
pg_query_queue(pg, "SELECT FROM table2 ...", ...);
pg_query_queue(pg, "SELECT FROM table3 ...", ...);
pg_query_exec(pg);  // Executes one after another
```

---

## Troubleshooting

### Connection Refused
```
[ERROR] Connection failed: could not connect to server
```

**Solutions:**
- Verify PostgreSQL is running: `systemctl status postgresql`
- Check host/port in config
- Check `pg_hba.conf` for access permissions
- Check firewall rules

---

### Pool Exhausted
```
[ERROR] All 10 connections in use (no wait mode)
```

**Solutions:**
- Increase `pool_size` in config
- Set `timeout_ms > 0` to wait for connections
- Check for slow queries blocking connections
- Monitor pool stats: `pg_pool_get_stats()`

---

### Query Timeout

If queries seem to hang:

**Check PostgreSQL:**
```sql
-- See active queries
SELECT pid, query, state, wait_event 
FROM pg_stat_activity 
WHERE state != 'idle';

-- Kill hung query
SELECT pg_terminate_backend(pid);
```

**Check Pool:**
```c
PGPoolStats stats;
pg_pool_get_stats(pool, &stats);
printf("Pool: %d available, %d in use\n", 
       stats.available, stats.in_use);
```

---

## License

Licensed under [MIT](./LICENSE)

---
