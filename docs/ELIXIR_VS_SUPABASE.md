# PgFlow: Elixir vs Supabase (TypeScript/Deno)

Both implementations share the same base PostgreSQL schema and SQL orchestration layer. They can run side-by-side against the same database.

## Architecture

| Layer     | TypeScript/Deno                                                   | Elixir                                                           |
| --------- | ----------------------------------------------------------------- | ---------------------------------------------------------------- |
| DSL       | Fluent builder with full generic type inference                   | Macros (`use PgFlow.Flow`, `use PgFlow.Job`)                     |
| SQL Core  | 30+ PL/pgSQL functions                                            | Same SQL, shared via ecto_evolver migrations                     |
| Worker    | Long-lived `EdgeWorker.start()` loop per Edge Function or process | Long-running OTP GenServer per flow/job                          |
| Polling   | `pgmq.read_with_poll()` (blocks in the database)                  | `pgmq.read()` (non-blocking) + adaptive backoff                  |
| Signal    | Polling only; never LISTENs and creates no pgmq triggers          | Polling (default) or LISTEN/NOTIFY on a commit-time trigger      |
| Client    | TS SDK with Supabase Realtime subscriptions                       | `PgFlow.Client` with sync polling                                |
| CLI       | `pgflow compile` via control plane HTTP                           | `mix pgflow.gen.flow_migration` generates migrations directly    |
| Dashboard | None (relies on Supabase dashboard)                               | Built-in Phoenix LiveView dashboard                              |
| Realtime  | Supabase Realtime broadcast via `realtime.send()`                 | Telemetry events + structured logging                            |
| Recovery  | `pgflow.requeue_stalled_tasks()` on pg_cron + visibility timeout  | `StalledTaskRecovery` GenServer + `escalate_permanently_stalled` |

## Flow Definition

**TypeScript** — fluent builder with method chaining:

```typescript
const flow = new Flow<{ orderId: number }>({ slug: 'process_order' })
  .step({ slug: 'validate' }, (input) => ({ valid: true }))
  .step({ slug: 'charge', dependsOn: ['validate'] }, (deps) => ({ charged: true }))
  .map({ slug: 'notify' }, (item) => ({ sent: true }))
```

**Elixir** — macro DSL:

```elixir
defmodule ProcessOrder do
  use PgFlow.Flow
  @flow queue: :process_order, max_attempts: 3

  step :validate do
    fn input, _ctx -> %{valid: true} end
  end

  step :charge, depends_on: [:validate] do
    fn deps, _ctx -> %{charged: true} end
  end

  map :notify do
    fn item, _ctx -> %{sent: true} end
  end
end
```

Both compile to identical SQL: `pgflow.create_flow()` + `pgflow.add_step()`.

## Step Types

| Type           | TypeScript         | Elixir                    |
| -------------- | ------------------ | ------------------------- |
| Single step    | `.step()`          | `step/2`, `step/3`.       |
| Array producer | `.array()`         | `step/2` returning list.  |
| Parallel map   | `.map()`           | `map/2`, `map/3`          |
| Dependent map  | `.map({ array: })` | `map :name, array: :step` |

## Step/Flow Options

| Option            | TypeScript      | Elixir             | TypeScript default            | Elixir default               |
| ----------------- | --------------- | ------------------ | ----------------------------- | ---------------------------- |
| Identifier        | `slug`          | `queue:` / `slug:` | —                             | —                            |
| Max attempts      | `maxAttempts`   | `max_attempts:`    | 3 (SQL default; DSL omits it) | 1                            |
| Retry delay       | `baseDelay`     | `base_delay:`      | 5s (SQL default)              | 1s                           |
| Timeout           | `timeout`       | `timeout:`         | 60s (SQL default)             | 30s                          |
| Start delay       | `startDelay`    | `start_delay:`     | 0                             | 0                            |
| Required pattern  | `if`            | `if:`              | —                             | —                            |
| Forbidden pattern | `ifNot`         | `if_not:`          | —                             | —                            |
| Unmet mode        | `whenUnmet`     | `when_unmet:`      | `skip` when a pattern is set  | `skip` when a pattern is set |
| Exhausted mode    | `whenExhausted` | `when_exhausted:`  | `fail`                        | `fail`                       |

The Elixir DSL always writes its defaults into the flow definition. A flow
defined in both languages without explicit options therefore stores different
`opt_max_attempts`, `opt_base_delay`, and `opt_timeout` values depending on
which runtime compiled it first. Set these options explicitly on flows that
both runtimes serve.

### Conditional Pattern Matching

Both implementations use PostgreSQL's `@>` (JSON contains) operator for pattern matching:

- **Root steps** - Pattern matches against the flow's input map
- **Dependent steps** - Pattern matches against `%{dep_slug => output, ...}`, a map built from dependencies' outputs

Example (Elixir):

```elixir
step :charge, depends_on: [:validate], if: %{"validate" => %{"valid" => true}} do
  fn deps, _ctx -> %{charged: true} end
end
```

When `:validate` runs and returns `%{"valid" => true, "id" => 123}`, the `:charge` step runs because
its input map `%{"validate" => %{"valid" => true, "id" => 123}}` contains the pattern `%{"validate" => %{"valid" => true}}`.

Non-cascade-skipped dependencies are **omitted** from dependent inputs: if `:validate` is skipped (not cascade),
`:charge` receives `%{}` instead of `%{"validate" => nil}`.

## Handler Context

| Field           | TypeScript                    | Elixir                        |
| --------------- | ----------------------------- | ----------------------------- |
| Run ID          | `context.stepTask.run_id`     | `ctx.run_id`                  |
| Step slug       | `context.stepTask.step_slug`  | `ctx.step_slug`               |
| Task index      | `context.stepTask.task_index` | `ctx.task_index`              |
| Attempt         | `context.stepTask.attempt`    | `ctx.attempt`                 |
| Flow input      | `await context.flowInput`     | `Context.get_flow_input(ctx)` |
| DB access       | `context.sql` (postgres.js)   | `ctx.repo` (Ecto)             |
| Shutdown signal | `context.shutdownSignal`      | OTP supervision               |
| Environment     | `context.env`                 | `Application.get_env/3`       |

Both lazy-load flow input to avoid unnecessary DB queries for non-root steps.

## Worker Model

### TypeScript: Edge Worker loops

```
Edge Function instance or Node/Deno process
  → EdgeWorker.start(flow, config)   (once per process)
  → register worker → loop: read_with_poll → start_tasks → handlers → heartbeat
  → stops on shutdown signal, CPU/wall-clock limit, or deprecated_at
  → ensure_workers() on pg_cron pings Edge Functions to start replacements
  → horizontal scaling = more instances/processes
```

### Elixir: Long-Running OTP GenServers

```
PgFlow.Supervisor (rest_for_one)
├── Task.Supervisor
├── Signal.Notify (when signal_strategy: :notify)
├── WorkerSupervisor (dynamic)
│   └── Worker.Server (one per flow/job)
├── StalledTaskRecovery
└── :flow_starter (one-shot registration)
```

| Aspect                 | TypeScript                                  | Elixir                                                        |
| ---------------------- | ------------------------------------------- | ------------------------------------------------------------- |
| Lifecycle              | Long-lived loop per instance or process     | Persistent GenServer                                          |
| State                  | Stateless between instances                 | In-memory (active_tasks map)                                  |
| Scaling                | More Edge Function instances or processes   | One worker per flow/job per node; add nodes                   |
| Per-worker concurrency | `EdgeWorker.start(flow, { maxConcurrent })` | `worker_options`, `start_worker/2`, `scale_worker/2`          |
| Batch size             | `EdgeWorker.start(flow, { batchSize })`     | `worker_options`, `start_worker/2`, `scale_worker/2`          |
| Runtime scaling        | None; restart with new config               | `PgFlow.scale_worker/2`, `PgFlow.check_worker/2` (local node) |
| Crash recovery         | New function invocation                     | OTP supervisor restart                                        |
| Stale task recovery    | `requeue_stalled_tasks()` on pg_cron        | Explicit sweep every 15s + permanent-stall escalation         |
| Worker heartbeat       | `last_heartbeat_at` in DB                   | DB registration + OTP monitor                                 |
| Worker deprecation     | `deprecated_at` flag                        | Honored on heartbeat; drains before replacement               |
| Graceful shutdown      | `shutdownSignal`                            | Lifecycle state machine                                       |

### Worker configuration

Both runtimes configure workers per process, not in the database. Nothing
below changes the pgflow schema, persisted flow definitions, queue names, or
message format, so TypeScript and Elixir workers can use different values on
the same flow.

| Setting               | TypeScript (`EdgeWorker.start` config) | Elixir                                                          |
| --------------------- | -------------------------------------- | --------------------------------------------------------------- |
| Concurrent tasks      | `maxConcurrent` (default 10)           | `max_concurrency` (default 10), per module via `worker_options` |
| Messages per read     | `batchSize` (default 10)               | `batch_size` (default 10), per module via `worker_options`      |
| Read wait             | `maxPollSeconds` (2s flows, 5s queues) | Global `min_poll_interval` / `max_poll_interval` (1s to 5s)     |
| In-database poll step | `pollIntervalMs` (100ms flows)         | None; `pgmq.read()` returns at once                             |
| Visibility timeout    | `visibilityTimeout`                    | 5s read reservation, then the flow or step `timeout` at claim   |
| Connections           | `maxPgConnections` (default 4)         | The Ecto repo pool                                              |
| Change while running  | Not supported                          | `PgFlow.scale_worker/2` (`max_concurrency`, `batch_size`)       |
| Inspect while running | Not supported                          | `PgFlow.check_worker/2`                                         |

`PgFlow.scale_worker/2` changes the worker on the calling node only and keeps
the new limits in memory. A crash or supervisor restart returns to the values
from config, `worker_options`, and `start_worker/2`. Scale-down never
interrupts running handlers. Neither runtime coordinates concurrency through
the database: the effective concurrency of a flow is the sum over every
Elixir node and every TypeScript process serving it.

`PgFlow.start_worker/2` accepts only `:repo`, `:max_concurrency`, and
`:batch_size`, and raises `ArgumentError` on any other key. `worker_options`
keys must also appear in `:flows` or `:jobs`. `scale_worker/2` and
`check_worker/2` also take `:timeout` (default 5000 ms) for the call to the
worker.

## Polling Protocol

Both use the same two-phase protocol:

1. Reserve messages from pgmq (makes them invisible)
2. `pgflow.start_tasks()` — atomically claim tasks, return details
3. Execute handler
4. `pgflow.complete_task()` or `pgflow.fail_task()`

**Difference in phase 1:**

| Aspect        | TypeScript                                    | Elixir                            |
| ------------- | --------------------------------------------- | --------------------------------- |
| Read call     | `pgmq.read_with_poll()` (blocking)            | `pgmq.read()` (non-blocking)      |
| Idle behavior | Blocks in the database up to `maxPollSeconds` | Adaptive jittered backoff (1s-5s) |
| Wake-up       | In-database poll every `pollIntervalMs`       | Polling or LISTEN/NOTIFY          |

TypeScript does the waiting inside PostgreSQL: one `read_with_poll` call holds
a connection and re-checks the queue every `pollIntervalMs` (100ms for flow
workers) for up to `maxPollSeconds` (2s for flow workers, 5s for queue
workers). Elixir workers schedule their own polls with `Process.send_after`,
so `pgmq.read()` returns at once and the connection goes back to the Ecto
pool. Backoff timers are cancellable, so a NOTIFY or a task completion wakes
the worker immediately.

Note that `read_with_poll` is not truly push-based — PostgreSQL runs an internal poll loop at `pollIntervalMs` (100ms for flow workers, 200ms for queue workers) inside the blocking call:

```
Deno worker                    PostgreSQL
   |                              |
   |---- read_with_poll() ------->|
   |     (connection held)        |-- SELECT FROM q_* ... nothing
   |                              |-- wait pollIntervalMs (100ms)
   |                              |-- SELECT FROM q_* ... nothing
   |                              |-- wait pollIntervalMs (100ms)
   |                              |-- SELECT FROM q_* ... MESSAGE!
   |<--- [message] ---------------|
```

The real question is where the poll loop lives and what tradeoffs that creates:

|                               | TypeScript (`read_with_poll`)   | Elixir (polling)  | Elixir (notify)          |
| ----------------------------- | ------------------------------- | ----------------- | ------------------------ |
| **Poll loop location**        | Inside PostgreSQL               | GenServer process | N/A (event-driven)       |
| **Busy latency**              | Up to `pollIntervalMs` (100ms)  | ~50ms             | Near-zero                |
| **Idle latency**              | Up to `pollIntervalMs` (100ms)  | Up to 5s          | Near-zero + 30s fallback |
| **DB connection during idle** | Held for up to `maxPollSeconds` | Not held          | Not held                 |
| **Truly push-based?**         | No                              | No                | Yes                      |

## Queue Notifications

Upstream pgflow never uses LISTEN/NOTIFY. It creates no triggers on pgmq
queue tables and does not call `pgmq.enable_notify_insert`. LISTEN/NOTIFY is
an Elixir-only signal strategy (`signal_strategy: :notify`) and needs pgmq
1.8 or newer, the first release with `enable_notify_insert`.

In `:notify` mode, `PgFlow.FlowStarter` enables notifications for each flow
queue it serves at boot. pgmq's `enable_notify_insert/2` creates the
`trigger_notify_queue_insert_listeners` constraint trigger on
`pgmq.q_<queue>` as `DEFERRABLE INITIALLY IMMEDIATE`. That trigger updates the
queue's row in `pgmq.notify_insert_throttle` at each insert and holds that row
lock until the inserting transaction ends. One long transaction that enqueues,
such as a batch import that starts many flows, blocks every other producer on
that queue until it commits.

By default (`notify_trigger: :deferred`) PgFlow recreates the trigger under the
same name as `DEFERRABLE INITIALLY DEFERRED`. This stays within pgmq's own
contract: every pgmq release since 1.7.0 creates the trigger `DEFERRABLE`, so
any caller can already defer it with `SET CONSTRAINTS`, and every pgmq
function that manages the trigger finds it by name. With the deferred trigger:

- The throttle check and NOTIFY run at COMMIT. A producer holds the throttle
  row lock only while its own commit runs.
- `pgmq.disable_notify_insert/1` still removes it.
- A boot that finds the deferred trigger already in place only reconciles the
  queue's throttle row, inserting it when it is missing, with no DDL.
  Otherwise the install takes a per-queue advisory lock and waits at most
  `notify_lock_timeout_ms` (default 2000) for the queue table lock, then
  retries with backoff.
- `mix pgflow.check_schema` warns, without failing, about an immediate trigger
  on a pgflow flow queue, a notify trigger whose queue has no throttle row,
  and a partitioned flow queue that the installed pgmq cannot notify for.

| Aspect                        | Upstream TypeScript | pgmq default (`:pgmq_default`)   | PgFlow default (`:deferred`)    |
| ----------------------------- | ------------------- | -------------------------------- | ------------------------------- |
| Trigger on `pgmq.q_<queue>`   | None                | `DEFERRABLE INITIALLY IMMEDIATE` | `DEFERRABLE INITIALLY DEFERRED` |
| Throttle row lock held        | N/A                 | Until the producer commits       | During the producer's commit    |
| NOTIFY after rollback         | N/A                 | No (NOTIFY is transactional)     | No                              |
| Throttle window measured from | N/A                 | Insert time                      | Commit time                     |
| Trigger errors surface at     | N/A                 | The INSERT                       | COMMIT                          |

Set `notify_trigger: :pgmq_default` to keep pgmq's trigger. Use one value on
every node: a node booting with `:pgmq_default`, or an older PgFlow release in
`:notify` mode, puts pgmq's immediate trigger back, and the next `:deferred`
boot replaces it again.

Effect on mixed deployments:

- TypeScript workers are unaffected. They never LISTEN and read only committed
  messages with `read_with_poll`.
- TypeScript and SQL producers on a shared flow queue (`start_flow`,
  `complete_task` through `pgmq.send_batch`) fire the trigger at their commit.
  They also stop holding the throttle row lock across their transaction. No
  function signature or result changes.
- A deferred row trigger queues one event per inserted row until commit. A
  very large map fan-out in one transaction carries that queue in memory and
  runs the throttle check once per row at commit.
- Host code that runs `SET CONSTRAINTS ALL IMMEDIATE` moves the trigger back to
  insert time for that transaction.
- Dropping and recreating a queue removes its trigger. Upstream does this in
  `delete_flow_and_data` and in `is_local()` recompilation. PgFlow reinstalls
  the trigger on its next boot; until then the 30s fallback poll picks up
  messages.

pgmq compatibility notes:

- A transaction that sends to a queue and then runs `pgmq.drop_queue`,
  `pgmq.purge_queue`, or `ALTER TABLE` on it fails with "pending trigger
  events" while the trigger is deferred. Run
  `SET CONSTRAINTS pgmq.trigger_notify_queue_insert_listeners IMMEDIATE` first
  (`PgFlow.Queries.Pgmq.flush_notify_events/1` does so when the trigger
  exists). `PgFlow.delete_flow/1` does this itself. Upstream's SQL
  `delete_flow_and_data` does not, so a TypeScript or SQL caller that sends
  and deletes a flow in one transaction needs the statement too.
- REPEATABLE READ and SERIALIZABLE producers can get a serialization failure
  (40001) at COMMIT when another producer notified on the same queue during
  their transaction. The immediate trigger has the same failure for a shorter
  window (from insert to commit).
- `ALTER EXTENSION pgmq UPDATE` to 1.13.1 rebuilds every notify trigger as
  pgmq's immediate trigger. Workers defer them again on their next boot, and
  `mix pgflow.check_schema` warns until then.
- pgmq keeps `pgmq.notify_insert_throttle` UNLOGGED, so crash recovery and
  Neon compute restarts empty it, and a queue without its row sends no NOTIFY
  with either trigger. `PgFlow.Signal.Notify` restores missing rows for the
  queues it listens on every 30 seconds, and every boot restores them too.
- Partitioned queues never notify on pgmq releases before 1.13.1, with either
  trigger, because the listener derives the queue name from the partition's
  table name. pgflow creates queues with `pgmq.create` only.

## Feature Comparison

| Feature                     | TypeScript                | Elixir                          | Notes                                                                                        |
| --------------------------- | ------------------------- | ------------------------------- | -------------------------------------------------------------------------------------------- |
| Single steps                | Yes                       | Yes                             | Identical semantics                                                                          |
| Map steps                   | Yes                       | Yes                             | Identical                                                                                    |
| Dependent map steps         | Yes                       | Yes                             | Identical                                                                                    |
| Empty array cascade         | Yes                       | Yes                             | Same SQL logic                                                                               |
| Conditional steps / skip    | Yes                       | Yes                             | `if` / `if_not` / `when_unmet` / `when_exhausted`                                            |
| Exponential backoff retry   | Yes                       | Yes                             | Same SQL function                                                                            |
| Compile-time DAG validation | Yes                       | Yes                             | TS generics vs Kahn's algorithm                                                              |
| Two-phase polling           | Yes                       | Yes                             | Same protocol                                                                                |
| start_delay option          | Yes                       | Yes                             | Per-step delay before queueing                                                               |
| Flow input lazy loading     | Yes                       | Yes                             | `context.flowInput` / `Context.get_flow_input`                                               |
| Type safety                 | Yes                       | No                              | TS generics flow through entire DAG                                                          |
| Client SDK (browser)        | Yes                       | No                              | Supabase Realtime subscriptions                                                              |
| Realtime events (WebSocket) | Yes                       | No                              | Elixir uses telemetry instead                                                                |
| Worker deprecation          | Yes                       | Yes                             | `deprecated_at`; Elixir drains before replacement                                            |
| LISTEN/NOTIFY signal        | No                        | Yes                             | pgmq 1.8.0+; trigger deferred to commit by default (see Queue Notifications)                 |
| Stalled task recovery       | Yes                       | Yes                             | TS `requeue_stalled_tasks` on pg_cron; Elixir queue-aware sweep + permanent-stall escalation |
| Per-worker concurrency      | Yes                       | Yes                             | `maxConcurrent` / `max_concurrency` + `worker_options`                                       |
| Runtime worker scaling      | No                        | Yes                             | `PgFlow.scale_worker/2`, local node, resets on restart                                       |
| Jobs DSL                    | No                        | Yes                             | `use PgFlow.Job` with `perform` block                                                        |
| Cron scheduling             | No                        | Yes                             | `cron: [schedule: "@hourly"]` via pg_cron                                                    |
| Dashboard                   | No                        | Yes                             | LiveView: flows, jobs, crons, runs, workers                                                  |
| Sync flow execution         | Via client                | Yes                             | `PgFlow.Client.start_flow_sync/3`                                                            |
| Telemetry events            | No                        | Yes                             | 12 events across flow/step/task/worker                                                       |
| Structured logging          | No                        | Yes                             | Fancy (dev) and simple (prod) formats                                                        |
| Mix tasks                   | N/A                       | Yes                             | gen.flow, setup, setup --upgrade, stamp, check_schema                                        |
| Upstream SHA reporting      | Package                   | SHA                             | Elixir reports `PgFlow.upstream_sha/0`; npm may lag unreleased revisions                     |
| JSON falsy output           | `output \|\| null` at pin | Preserved scalars               | Documented unresolved limitation — see UPSTREAM_COMPATIBILITY.md                             |
| `is_local()` recompile      | Supabase JWT probe        | Returns false on plain Postgres | Production mismatches preserve history                                                       |
| External waits              | Future                    | Not shipped                     | No per-step queues or signal store in this release                                           |

## Shared SQL Core

Both call the same PostgreSQL functions:

| Function                                 | Purpose                                      |
| ---------------------------------------- | -------------------------------------------- |
| `pgflow.create_flow`                     | Register flow + create pgmq queue            |
| `pgflow.add_step`                        | Add step with dependencies                   |
| `pgflow.start_flow`                      | Create run, enqueue root steps               |
| `pgflow.start_tasks/4`                   | Claim tasks with queue route + attempt count |
| `pgflow.complete_task`                   | Save output, cascade to dependents           |
| `pgflow.fail_task`                       | Retry with backoff or fail permanently       |
| `pgflow.start_ready_steps`               | Activate steps with all deps satisfied       |
| `pgflow.cascade_complete_taskless_steps` | Handle empty array propagation               |
| `pgflow.maybe_complete_run`              | Complete run when all steps done             |

Core tables include `flows`, `steps`, `deps` (definition) and `runs`, `step_states`, `step_tasks`, `workers` (runtime).

## Schema Divergences

The Elixir implementation adds or omits the following relative to the pinned
upstream commit (`PgFlow.upstream_sha/0`). The compatibility harness manifest
records each one.

| Change                                 | Location         | Description                                                                                                                                                                 |
| -------------------------------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `flow_type` column + check             | `pgflow.flows`   | Distinguishes background jobs from multi-step DAG workflows.                                                                                                                |
| `attempts_count` on `step_task_record` | `pgflow` type    | Real handler attempt count returned by the claim. TypeScript workers ignore the extra field.                                                                                |
| `step_progress` view                   | `pgflow`         | Elixir-only read model for the dashboard and clients.                                                                                                                       |
| Helper SQL functions                   | `pgflow`         | `register_worker`, `flow_exists`, `get_flow_input`, `get_step_output`, `escalate_permanently_stalled`, port-only `delete_run` (one run's history and exact queue messages). |
| Helper overrides                       | `pgflow`         | Queue-aware `start_tasks/4`, `recover_stalled_tasks`, filtered `prune_data_older_than/2` (locks runs before their tasks, like `delete_run`).                                |
| Version tracking views                 | `pgflow`         | `pgflow_version`, `extensions_version`.                                                                                                                                     |
| Omitted `ensure_workers`               | `pgflow`         | Needs Supabase `net.http_post`; Elixir registers workers through OTP.                                                                                                       |
| Deferred notify trigger (runtime only) | `pgmq.q_<queue>` | In `:notify` mode only. Not part of the pgflow schema; see Queue Notifications.                                                                                             |

Existing flow records default to `flow_type = 'flow'`. Helpers are installed
with `mix pgflow.gen.helpers_migration`.

## Compilation

**TypeScript:** CLI sends HTTP request to ControlPlane edge function, which extracts the flow shape and generates SQL. Written to `supabase/migrations/`.

**Elixir:** `mix pgflow.gen.flow_migration MyFlow` reads `__pgflow_definition__/0`
and generates an Ecto migration. Worker startup uses `PgFlow.Worker.Bootstrap`
and `pgflow.ensure_flow_compiled/2` to compile or verify definitions. `FlowCompiler`
remains available for existing migrations and explicit upserts.
