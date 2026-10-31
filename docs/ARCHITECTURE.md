# PgFlow Architecture

PgFlow is a native Elixir implementation of [pgflow](https://pgflow.dev) - a PostgreSQL-based workflow engine built on pgmq. It shares the same database schema and SQL functions as the TypeScript/Deno version, allowing workers from both runtimes to process the same flows side-by-side.

## Design Principles

1. **Single Source of Truth** - All workflow definitions, state, and queues live in Postgres
2. **DAGs Only** - Directed acyclic graphs with explicit dependencies, no cycles or conditional edges
3. **JSON Serializable** - All inputs/outputs must be JSON-compatible
4. **Stateless Workers** - Workers poll and execute; database handles orchestration

## Core Concepts

```
                     PostgreSQL
    -------------------------------------------------
    pgflow.*          pgmq.q_*        pgflow.step_tasks
    (flow schema)     (queues)        (task state)
    -------------------------------------------------
                          |
                          v
                    Elixir/OTP
    -------------------------------------------------
    PgFlow.Supervisor --> Worker.Server --> Signal.*
    -------------------------------------------------
```

**Compatibility:** Same pgflow.* schema, same core SQL functions, same pgmq message format. Elixir and TypeScript workers can run side-by-side processing the same flows when pinned to the same upstream SHA (`PgFlow.upstream_sha/0`). See [UPSTREAM_COMPATIBILITY.md](UPSTREAM_COMPATIBILITY.md).

**Elixir extensions:** Adds helper SQL functions for OTP worker lifecycle. Backward-compatible - TypeScript workers can safely ignore them. Queue identity, four-argument claim, and startup compilation follow upstream core V02; helpers V05 reconcile recovery and pruning with persisted `(queue_name, message_id)` routes.

**Schema verification:** `mix pgflow.check_schema` validates installed core/helpers versions, function signatures, queue constraints, and the eight-field `step_task_record` before production deploys.

## DSL

### Flows

Multi-step DAGs with explicit dependencies:

```elixir
defmodule MyApp.Flows.ProcessOrder do
  use PgFlow.Flow

  @flow queue: :process_order, max_attempts: 3, timeout: 60

  step :validate do
    fn input, _ctx -> %{valid: true} end
  end

  step :charge, depends_on: [:validate] do
    fn deps, _ctx -> %{charged: true} end
  end

  map :notify_parties, depends_on: [:charge] do
    fn item, _ctx -> %{notified: item} end
  end
end
```

### Jobs

Single-step flows with a simpler API:

```elixir
defmodule MyApp.Jobs.SendEmail do
  use PgFlow.Job

  @job queue: :send_email, max_attempts: 3, timeout: 60

  perform :deliver do
    fn input, _ctx -> Mailer.send(input["to"], input["body"]) end
  end
end
```

Jobs compile to a single-step `PgFlow.Flow.Definition` with `flow_type: :job`. Workers process them identically to flows.

### Cron Scheduling

Both flows and jobs support `cron:` for pg_cron scheduling:

```elixir
@flow queue: :daily_report, cron: [schedule: "0 9 * * *", input: %{"type" => "daily"}]
@job queue: :cleanup, cron: [schedule: "@hourly"]
```

## Supervision Tree

```
PgFlow.Supervisor (rest_for_one)
|-- Task.Supervisor (PgFlow.TaskSupervisor)
|-- Signal.Notify (only when signal_strategy: :notify)
|-- WorkerSupervisor (dynamic)
|   |-- Worker.Server (flow: process_order)
|   |-- Worker.Server (job: send_email)
|   +-- ...
|-- StalledTaskRecovery
+-- :flow_starter (temporary Task — registers flows/jobs, starts workers)
```

The `:flow_starter` is a one-shot initialization task that runs after WorkerSupervisor is alive. It registers each flow/job module via `FlowRegistry`, starts a `Worker.Server` for each, and (when using `:notify` strategy) enables pgmq notifications and registers workers with `Signal.Notify`.

## Module Dependency Graph

```
PgFlow.Flow (DSL)     PgFlow.Job (DSL)
    |                      |
    v                      v
FlowRegistry ----------+
    |                   |
    v                   v
WorkerSupervisor --> Worker.Server
                        |
          +-------------+-------------+
          v             v             v
     Queries.*     Executor      Signal.*
          |
          v
    PostgreSQL (pgflow.*, pgmq.*)
```

### Query Modules

```
PgFlow.Queries.Helpers  — UUID parsing, RPC execution, row conversion
PgFlow.Queries.Flows   — start_flow, complete_task, fail_task, read, start_tasks, etc.
PgFlow.Queries.Workers — register_worker, mark_worker_stopped
PgFlow.Queries.Pgmq   — enable_notify_insert (deferred trigger, fast/slow path), disable_notify_insert, get_pgmq_version
```

## Client API

`PgFlow.Client` provides the public interface for starting flows programmatically:

```elixir
{:ok, run_id} = PgFlow.Client.start_flow(:process_order, %{"order_id" => 123})
{:ok, run}    = PgFlow.Client.start_flow_sync(:process_order, %{"order_id" => 123}, timeout: 30_000)
{:ok, run}    = PgFlow.Client.get_run(run_id)
{:ok, run}    = PgFlow.Client.get_run_with_states(run_id)
```

Resolves repo from `persistent_term` (set by supervisor) or application env.

## Signal Strategies

Workers detect new queue messages via two strategies:

### Polling (Default)

```
Worker                         PostgreSQL
   |                              |
   |---- pgmq.read() ----------->|
   |<--- messages[] --------------|
   |                              |
   |     (process tasks...)       |
   |                              |
   |---- pgmq.read() ----------->|  <-- jittered backoff: 50ms -> 5s
   |<--- [] ----------------------|
   |                              |
   +-- wait (backoff) ----------->|
```

Adaptive jittered exponential backoff:
- Active queue: polls at `min_poll_interval` (default: 1s)
- Idle queue: backs off to `max_poll_interval` (default: 5s)
- Uses decorrelated jitter to prevent thundering herd

### Notify (LISTEN/NOTIFY)

More efficient than polling - workers sleep until PostgreSQL pushes a notification. pgmq 1.8.0+ includes built-in LISTEN/NOTIFY support via `enable_notify_insert()`, which creates a trigger that fires on queue inserts. This pairs naturally with `Postgrex.Notifications` for native Elixir pubsub.

```
Worker              Signal.Notify          PostgreSQL
   |                     |                     |
   |--> register ------->|                     |
   |                     |-- LISTEN ---------->|
   |                     |                     |
   |                     |                     |<-- INSERT to q_*
   |                     |<- NOTIFY -----------|
   |<-- :poll_now -------|                     |
   |                     |                     |
   |---- pgmq.read() ------------------------->|
```

**pgmq 1.8.0+ features:**
- `pgmq.enable_notify_insert(queue_name, throttle_ms)` registers the queue's row in the UNLOGGED `pgmq.notify_insert_throttle` table and an `AFTER INSERT` constraint trigger on `pgmq.q_<queue>`. pgmq declares the trigger `DEFERRABLE` with no `INITIALLY` clause, so Postgres makes it `INITIALLY IMMEDIATE`: it runs at each insert.
- Single `Postgrex.Notifications` connection shared across all queues with auto-reconnect

**Trigger deferred to commit:**

The trigger function updates the queue's throttle row on every insert. Run immediately, that row lock is held until the inserting transaction ends, so any long transaction that enqueues (for example a batch import that starts flows) blocks every other producer on the same queue until it commits. `PgFlow.Queries.Pgmq.enable_notify_insert/4`, which `FlowStarter` calls for each flow queue on every boot in `:notify` mode, therefore leaves the trigger, under the same name, as `DEFERRABLE INITIALLY DEFERRED` with the queue name as the trigger argument. This stays within pgmq's own contract: every pgmq release since 1.7.0 creates the trigger as a `DEFERRABLE` constraint trigger, so any caller could already defer it with `SET CONSTRAINTS`; pgflow makes deferred the default. As a result:

- The trigger runs at COMMIT, so a producer holds the throttle row lock only while its own commit runs, never across the body of its transaction. Other producers' inserts never wait on it; concurrent commits on one queue wait on each other only for the length of a commit.
- Each committing producer evaluates the throttle itself, so a second producer that commits while a first transaction is still open still sends its own notification. (A rolled-back transaction sends no notification either way: NOTIFY is transactional, and the rollback also undoes an immediate trigger's throttle update.)
- `pgmq.disable_notify_insert/1` still drops it, since the name is unchanged.
- `mix pgflow.check_schema` warns (without failing) about any notify trigger on a flow queue that is not deferred, such as one created before this change, or one that `ALTER EXTENSION pgmq UPDATE` rebuilt, and not yet rebooted. It also warns about notify triggers whose queue has no throttle row, and about partitioned queues that cannot notify (see the caveats below). It checks only the `lower(flow_slug)` queues pgflow manages, not custom step `queue_name`s.

How a boot installs it:

- **Fast path.** It first reads `pg_trigger` and `pgmq.notify_insert_throttle`. When the queue already has the deferred trigger (deferrable, initially deferred, calling `pgmq.notify_queue_listeners` with the queue name), it only upserts the throttle row: it updates `throttle_interval_ms` when that differs from `notify_throttle_ms`, and inserts the row when it is missing. No DDL runs and no queue table lock is taken, so steady-state boots never wait on, or block, producers and readers.
- **Slow path.** Otherwise (trigger missing, immediate, or different) it runs one transaction that sets `lock_timeout` to `notify_lock_timeout_ms` (default 2000), takes a per-queue advisory lock (`pg_advisory_xact_lock(hashtextextended('pgflow:notify:' || queue, 0))`) so installers on several nodes run one at a time, re-checks the trigger under that lock (another node may have finished), then calls `pgmq.enable_notify_insert` and recreates the trigger deferred. pgmq's function drops and recreates the trigger, which needs ACCESS EXCLUSIVE on `pgmq.q_<queue>`. If an open transaction holds a conflicting lock longer than the timeout, the install fails with `lock_not_available`, which `FlowStarter` treats as transient and retries with backoff, instead of queueing every later send and read on the queue behind the pending lock.
- **Degraded swap.** If pgmq's function succeeds but recreating the trigger deferred fails (for example on missing privileges or a renamed pgmq object), only that swap is rolled back to a savepoint. pgmq's immediate trigger stays, so notifications still work; a warning is logged and the boot continues. `mix pgflow.check_schema` then reports the immediate trigger.
- **Not inside a transaction.** `enable_notify_insert/4` returns `{:error, :in_transaction}` when called inside a transaction on the repo. There, pgmq's DROP TRIGGER would hold ACCESS EXCLUSIVE until the caller commits, and dropping the deferred trigger after the caller's own send in that transaction makes Postgres skip that send's pending trigger event, so its notification is lost. `FlowStarter` never calls it inside a transaction.
- **Opt-out.** `notify_trigger: :pgmq_default` calls only `pgmq.enable_notify_insert`, which keeps pgmq's immediate trigger, and turns off the deferred-trigger check-schema warning (pass `--notify-trigger pgmq_default` to the mix task). Use one value on every node: a `:pgmq_default` node, or a pgflow release before 0.6 in `:notify` mode, puts pgmq's immediate trigger back on its boot, and the next `:deferred` boot defers it again.

**pgmq compatibility caveats:**

- **Same-transaction drop, purge, or ALTER.** Postgres refuses to DROP, TRUNCATE, or ALTER a table that has pending trigger events. A transaction that sends to a queue (`pgmq.send`, `send_batch`, `pgflow.start_flow`) and then runs `pgmq.drop_queue`, `pgmq.purge_queue`, or an `ALTER TABLE` on that queue fails with `cannot DROP TABLE "q_<queue>" because it has pending trigger events` (SQLSTATE 55006). The immediate trigger has no pending events, so it never hits this. Run `SET CONSTRAINTS pgmq.trigger_notify_queue_insert_listeners IMMEDIATE` earlier in that transaction to fire the pending events; `PgFlow.Queries.Pgmq.flush_notify_events/1` does that only when the constraint exists, since `SET CONSTRAINTS` on a name that matches no constraint raises (pgmq releases without notify, such as 1.5.1). pgflow's own `PgFlow.delete_flow/1` (`PgFlow.Queries.Flows.delete_flow/2`, including flow re-registration that deletes the old definition) and the test `pgflow_tests.reset_db()` helper flush first. The immediate mode then lasts until that transaction ends.
- **REPEATABLE READ and SERIALIZABLE producers.** The trigger's throttle UPDATE runs against the transaction snapshot. If another transaction notified on the same queue after that snapshot was taken, the UPDATE hits a concurrently updated row and the COMMIT fails with SQLSTATE 40001 (`could not serialize access due to concurrent update`). An immediate trigger has the same failure, but only for notifications that land between the producer's insert and its commit; deferred, the window covers the whole transaction, and on a busy queue the row changes every throttle interval. Retry such producers on 40001 as usual, keep them short, or use `notify_trigger: :pgmq_default`. READ COMMITTED producers are unaffected.
- **Extension updates.** `ALTER EXTENSION pgmq UPDATE` to 1.13.1 drops and recreates every notify trigger listed in `pgmq.notify_insert_throttle` as pgmq's immediate trigger; later upgrade scripts that rebuild triggers are likely to do the same. Notifications keep working, but producers hold the row lock again. The next `:deferred` boot sees the immediate trigger, takes the slow path, and defers it again; `mix pgflow.check_schema` warns until then.
- **Errors at COMMIT and per-row memory.** Any error inside the trigger, such as a deadlock between two transactions that insert into the same two queues in opposite order, a `statement_timeout` or `lock_timeout` hit during commit, or a dropped `pgmq.notify_insert_throttle` table, now fails the producer's COMMIT and rolls back the whole transaction instead of failing the INSERT that fired it. Deferred events also stay in backend memory until commit, one per inserted row, so a single transaction that inserts very many messages carries that queue and runs the throttle check once per row at commit. A transaction that runs `SET CONSTRAINTS ALL IMMEDIATE` (or names this trigger) moves it back to insert time and reintroduces the blocking for that transaction.
- **Throttle rows lost on crash or Neon restart.** pgmq creates `pgmq.notify_insert_throttle` UNLOGGED, so Postgres empties it during crash recovery, and Neon empties unlogged tables whenever a compute suspends or restarts. The trigger only notifies after it updates the queue's row, so a queue without its row sends no NOTIFY and workers fall back to the 30s poll. This affects pgmq's immediate trigger too. pgflow restores missing rows without DDL in two places: the boot fast path (above), and `Signal.Notify`, which every 30s runs `PgFlow.Queries.Pgmq.restore_notify_throttle_rows/3` for the queues its workers listen on. That check is one statement that writes nothing while every row is present, and it logs a warning when it restores a row. `Signal.Notify` runs it rather than a reconnect hook or the worker fallback poll: `Postgrex.Notifications` reconnects without telling its caller, and one statement per node covers every served queue.
- **Partitioned queues.** On a partitioned queue table (`pgmq.create_partitioned`) Postgres clones the trigger onto each partition, where `TG_TABLE_NAME` names the partition. pgmq releases before 1.13.1 derive the queue name from `TG_TABLE_NAME`, find no throttle row, and never notify, with pgmq's trigger or pgflow's. 1.13.1 reads the queue name from the trigger argument, which pgflow's trigger always passes. pgflow creates queues with `pgmq.create` only, but `mix pgflow.check_schema` warns when a managed flow queue is partitioned and the installed listener ignores the argument.

**Throttle window:**
- `notify_throttle_ms` (default 250ms) is the minimum interval between notifications for one queue. At commit, the trigger's UPDATE succeeds, and NOTIFY is sent, only when the window since the last notification has elapsed; `0` notifies on every committed insert.
- A commit that lands inside the window sends no notification. Its messages are picked up by whichever worker read follows: the read woken by the earlier notification if it runs after this commit, a poll-after-completion, or the fallback poll.

**Fallback timer with reset:**
- 30s fallback poll as safety net for missed notifications, including commits suppressed by the throttle window
- When NOTIFY arrives, fallback timer resets to 30s from now
- Reduces unnecessary DB round-trips when pubsub is working
- If NOTIFY stops (network issues, etc.), fallback fires after 30s of silence

**Hybrid wake-up model:**

NOTIFY and polling serve different purposes:
- **NOTIFY** - Wakes idle workers when new flows start externally
- **Poll-after-completion** - When a step with downstream dependents completes, the worker polls immediately to pick up enqueued continuation tasks (bypasses pgmq's throttle window which can suppress rapid-fire notifications)
- **No poll for terminal steps** - Steps without dependents skip the immediate poll

This avoids reliance on NOTIFY for task continuation while still benefiting from instant wake-ups for new work.

## Worker-to-Database Mapping

| Worker State | Database Tables                        |
|--------------|----------------------------------------|
| worker_id    | pgflow.workers.id                      |
| queue_name   | pgmq.q_{lower(flow_slug)}              |
| active_tasks | pgflow.step_tasks (status: 'started')  |

## Task Processing

Both signal strategies use the same two-phase protocol once triggered:

1. `Queries.Flows.read()` - Reserve messages from pgmq (makes them invisible)
2. `Queries.Flows.start_tasks()` - Create step_tasks records, get task details with deps
3. Execute handler in `Task.Supervisor` - Crash isolation per task
4. `Queries.Flows.complete_task()` or `Queries.Flows.fail_task()` - Update state, trigger downstream

The signal strategy only determines *when* to poll (timer backoff vs NOTIFY wake-up).

**Completion-triggered polling:** When a step with downstream dependents completes, an immediate poll is scheduled. This ensures continuation tasks are picked up promptly regardless of NOTIFY throttling. Terminal steps (no dependents) skip the poll.

Skipped steps are decided in SQL (`cascade_resolve_conditions`, `_cascade_force_skip_steps` / `fail_task`). Workers never enqueue them. Elixir emits `[:pgflow, :step, :skipped]` after `start_flow`, `complete_task`, and `fail_task` by reading `step_states`.

Workers do not re-check for skips before dispatching: `start_tasks` returns a task only while its step is still `started`, and every skip path archives that step's queued/started messages in the same transaction. Delivery stays at-least-once, so a handler can still be mid-flight when the skip commits — handlers must be idempotent (see `PgFlow.Worker.Server` docs).

## Task Timeout Enforcement

Each dispatched task gets an OTP-native timeout via `Process.send_after(self(), {:task_timeout, ref}, timeout_ms)`.

Timeout resolution (matches DB schema's per-flow/per-step configuration):
1. Step-level timeout (from DSL: `step :name, timeout: X`)
2. Flow-level timeout (from DSL: `@flow timeout: X`)
3. Default: 60 seconds

On timeout: `Task.Supervisor.terminate_child/2` sends `:shutdown`, then `fail_task` is called.
On completion: `Process.cancel_timer` cancels the pending timeout.

## Worker Lifecycle

States managed by `PgFlow.Worker.Lifecycle`:

```
:created -> :starting -> :running -> :stopping -> :stopped
```

- `:created` - Worker instantiated, not yet started
- `:starting` - Initializing (registering with DB, compiling flow)
- `:running` - Actively processing messages
- `:stopping` - Draining active tasks, releasing resources
- `:stopped` - Terminal state

## Stalled Task Recovery

`StalledTaskRecovery` GenServer sweeps every `recovery_interval` (default: 15s) for tasks stuck in `started` status beyond `stale_threshold` (default: 60s). Resets them to `queued` and makes their pgmq messages immediately visible via `pgflow.set_vt_batch`. After three requeues, the next sweep archives the message and stamps `permanently_stalled_at`. A separate query then calls `pgflow.escalate_permanently_stalled()`, which applies the step's `when_exhausted` policy through `fail_task`. Alert on stamped tasks whose run remains `started`, which indicates escalation failed.

## OTP Integration

PgFlow leverages OTP primitives for fault tolerance:

- **Supervision trees** - Automatic restart on crashes (rest_for_one strategy)
- **GenServer lifecycle** - Worker states: created -> starting -> running -> stopping -> stopped
- **Task.Supervisor** - Crash isolation per task execution
- **Process monitoring** - Automatic cleanup when workers die
- **Independent worker heartbeats** - Persisted every 10 seconds by default, including while polling and notification workers are idle
- **Process scheduling** - `Process.send_after` for adaptive poll timing, replacing Deno's blocking `pgmq.read_with_poll()` with non-blocking `pgmq.read()` — no DB connections held idle, backoff timers are cancellable by NOTIFY or completion events
- **Postgrex.Notifications** - Native PostgreSQL LISTEN/NOTIFY support

The TypeScript implementation runs long-lived Edge Worker loops; upstream waits inside PostgreSQL with `pgmq.read_with_poll`, holding a connection for up to `maxPollSeconds`. Elixir workers are stateful GenServers where OTP itself is the scheduler, enabling adaptive backoff and instant wake-ups.

## Configuration

```elixir
{PgFlow.Supervisor,
  repo: MyApp.Repo,
  flows: [MyApp.Flows.OrderFlow],
  jobs: [MyApp.Jobs.SendEmail],
  signal_strategy: :notify,         # :polling or :notify
  heartbeat_interval: 10_000,       # persisted worker liveness (max 20_000ms)
  notify_throttle_ms: 250,          # min ms between NOTIFYs per queue (0 = every commit)
  notify_trigger: :deferred,        # :deferred (runs at commit) or :pgmq_default (pgmq's immediate trigger)
  notify_lock_timeout_ms: 2_000,    # max wait for the queue table lock when installing the trigger
  min_poll_interval: 1_000,         # ms (polling strategy)
  max_poll_interval: 5_000,         # ms (polling strategy)
  notify_fallback_interval: 30_000, # ms (safety net for notify)
  max_concurrency: 10,              # parallel tasks per worker (global default)
  batch_size: 10,                   # messages per poll (global default)
  worker_options: %{                # per flow/job overrides for max_concurrency and batch_size
    MyApp.Flows.Import => [max_concurrency: 4]
  },
  recovery_interval: 15_000,        # stalled task sweep (ms)
  stale_threshold: 60,              # seconds before task is stale
  worker_name: "my-app",            # optional human-readable prefix for logs
  attach_default_logger: false}     # attach telemetry logger (default: false)
```

**Note:** Reads reserve messages for 5s; the claim then extends visibility to the step's execution timeout, derived from the flow's `@flow timeout:` (default 30s), not a global config.

Runtime scaling via `PgFlow.scale_worker/2` updates the worker GenServer's in-memory limits on the local node only. It does not write to PostgreSQL or propagate to other nodes; supervisor restart or crash recovery restores the resolved configuration from global options, `worker_options`, and any `start_worker/2` overrides.

## Key SQL Functions

Shared with the TypeScript implementation:

| Function                     | Purpose                                    |
|------------------------------|--------------------------------------------|
| `pgflow.create_flow`         | Register flow with retry/timeout settings  |
| `pgflow.add_step`            | Add step with dependencies                 |
| `pgflow.start_flow`          | Create run, enqueue root step tasks        |
| `pgflow.start_tasks`         | Create step_tasks from reserved messages   |
| `pgflow.complete_task`       | Mark done, trigger `start_ready_steps`     |
| `pgflow.fail_task`           | Record error, retry via visibility timeout |
| `pgflow.start_ready_steps`   | Start steps whose deps are complete        |
| `pgflow.maybe_complete_run`  | Complete run when all steps done           |

Elixir-specific additions:

| Function                         | Purpose                                     |
|----------------------------------|---------------------------------------------|
| `pgflow.flow_exists`             | Check if flow is compiled in DB             |
| `pgflow.get_flow_input`          | Retrieve input for a flow run               |
| `pgflow.get_step_output`         | Retrieve output of a completed step         |
| `pgflow.recover_stalled_tasks`   | Reset stalled tasks to queued               |
| `pgflow.escalate_permanently_stalled` | Apply exhausted policy to stamped tasks |
| `pgflow.prune_data_older_than`   | Clean up old run data                       |
| `pgflow.analyze_and_create_flow` | Explicit legacy definition compilation; destructive runtime upsert is deprecated |
| `pgflow.ensure_flow_compiled` | Compile or verify worker definition under the upstream shape/advisory-lock contract |
| `pgflow.track_worker_function` | Register `elixir:<Module>` with start mode `process` |
| `pgflow.is_valid_queue_name` | Validate physical queue identity, derived from `lower(flow_slug)` |

pgmq functions used:

| Function                          | Purpose                                    |
|-----------------------------------|--------------------------------------------|
| `pgmq.read`                       | Non-blocking message read                  |
| `pgmq.enable_notify_insert`       | Enable LISTEN/NOTIFY on queue inserts      |
| `pgmq.disable_notify_insert`      | Disable LISTEN/NOTIFY on queue             |

## Skip Broadcast Pipelines

Skipped steps are announced via two independent pipelines:

### SQL Pipeline (realtime.send)

When a step is skipped in PostgreSQL (`_cascade_force_skip_steps`, `cascade_resolve_conditions`, or `fail_task`),
the database optionally broadcasts via `realtime.send()` (if configured). This is the TypeScript/Deno
integration point for real-time dashboard updates. (Entry points: `pgflow.start_flow/2`, `pgflow.complete_task/3`, `pgflow.fail_task/3`.)

### Elixir Telemetry Pipeline

Elixir workers independently discover skips by polling `step_states` after each `complete_task`/`fail_task`,
then emit `[:pgflow, :step, :skipped]` telemetry events. This is the Elixir runtime's mechanism.

**Key distinction:** These pipelines are separate. A skip committed in SQL is not automatically announced to
Elixir — the worker must poll to discover it. Conversely, the Elixir worker's telemetry emissions are not
sent back to SQL (the database remains the source of truth).

### Delivery Contract (Telemetry)

**Per-emitter exactly-once** — Each Elixir worker maintains a per-run `MapSet` of already-emitted step slugs
(delta emission via per-worker seen-set). This guarantees that within a single worker's observation of a run,
each skip is announced exactly once.

**Caveats:**

- This is a per-worker guarantee, not global. Two workers processing the same run each sweep it independently,
  so a skip can be announced once per worker that touches the run, and once by `Client.start_flow/2` for
  skips decided at run start.
- `PgFlow.LiveClient` handles this structurally by treating `[:pgflow, :step, :skipped]` as an idempotent
  state transition (not a counter), using `{run_id, step_slug}` as the key.

For authoritative per-emitter-exactly-once semantics, see `PgFlow.Telemetry.emit_skipped_steps/4`.

## Task Delivery Guarantee (at-least-once)

Task delivery is at-least-once, even with skipping. See `PgFlow.Worker.Server` moduledoc,
"Delivery Guarantee (at-least-once)" section.

**Key points:**

- A handler may run for a step the database later reports as skipped, because the step can be skipped
  microseconds after `start_tasks` hands out a task.
- Handlers must be idempotent and not treat "my step completed" as guaranteed by the handler having run.
- Side effects that must not happen for a skipped step need their own guard (idempotency key, conditional write,
  or check inside a transaction) rather than relying on the worker.

## Telemetry

PgFlow emits telemetry events for monitoring. Attach handlers via `:telemetry.attach_many/4` or set `attach_default_logger: true` for built-in logging.

Events:
- `[:pgflow, :flow, :started | :completed | :failed]`
- `[:pgflow, :step, :started | :completed | :failed | :skipped]`
- `[:pgflow, :task, :started | :completed | :failed]`
- `[:pgflow, :worker, :started | :stopped | :poll | :error]`
