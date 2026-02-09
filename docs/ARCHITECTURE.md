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

**Compatibility:** Same pgflow.* schema, same core SQL functions, same pgmq message format. Elixir and TypeScript workers can run side-by-side processing the same flows.

**Elixir extensions:** Adds helper SQL functions for OTP worker lifecycle. Backward-compatible - TypeScript workers can safely ignore them.

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
PgFlow.Queries.Pgmq   — disable_notify_insert, get_pgmq_version
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
- Active queue: polls at `min_poll_interval` (default: 50ms)
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
- `pgmq.enable_notify_insert(queue_name, throttle_ms)` - Creates deferrable constraint trigger (fires at COMMIT)
- Throttle batches notifications via atomic UPDATE on UNLOGGED table — one notification per throttle window (default 250ms)
- Single `Postgrex.SimpleConnection` shared across all queues with auto-reconnect

**Fallback timer with reset:**
- 30s fallback poll as safety net for missed notifications
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
| flow_slug    | pgmq.q_{flow_slug}                     |
| active_tasks | pgflow.step_tasks (status: 'started')  |

## Task Processing

Both signal strategies use the same two-phase protocol once triggered:

1. `Queries.Flows.read()` - Reserve messages from pgmq (makes them invisible)
2. `Queries.Flows.start_tasks()` - Create step_tasks records, get task details with deps
3. Execute handler in `Task.Supervisor` - Crash isolation per task
4. `Queries.Flows.complete_task()` or `Queries.Flows.fail_task()` - Update state, trigger downstream

The signal strategy only determines *when* to poll (timer backoff vs NOTIFY wake-up).

**Completion-triggered polling:** When a step with downstream dependents completes, an immediate poll is scheduled. This ensures continuation tasks are picked up promptly regardless of NOTIFY throttling. Terminal steps (no dependents) skip the poll.

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

`StalledTaskRecovery` GenServer sweeps every `recovery_interval` (default: 15s) for tasks stuck in `started` status beyond `stale_threshold` (default: 60s). Resets them to `queued` and makes their pgmq messages immediately visible via `pgflow.set_vt_batch`.

## OTP Integration

PgFlow leverages OTP primitives for fault tolerance:

- **Supervision trees** - Automatic restart on crashes (rest_for_one strategy)
- **GenServer lifecycle** - Worker states: created -> starting -> running -> stopping -> stopped
- **Task.Supervisor** - Crash isolation per task execution
- **Process monitoring** - Automatic cleanup when workers die
- **Postgrex.Notifications** - Native PostgreSQL LISTEN/NOTIFY support

The Deno implementation uses stateless edge functions with external coordination. Elixir workers are stateful GenServers with built-in recovery.

## Configuration

```elixir
{PgFlow.Supervisor,
  repo: MyApp.Repo,
  flows: [MyApp.Flows.OrderFlow],
  jobs: [MyApp.Jobs.SendEmail],
  signal_strategy: :notify,         # :polling or :notify
  notify_throttle_ms: 250,          # pgmq trigger debounce (0 = instant)
  min_poll_interval: 50,            # ms (polling strategy)
  max_poll_interval: 5_000,         # ms (polling strategy)
  notify_fallback_interval: 30_000, # ms (safety net for notify)
  max_concurrency: 10,              # parallel tasks per worker
  batch_size: 10,                   # messages per poll
  recovery_interval: 15_000,        # stalled task sweep (ms)
  stale_threshold: 60,              # seconds before task is stale
  worker_name: "my-app",            # optional human-readable prefix for logs
  attach_default_logger: false}     # attach telemetry logger (default: false)
```

**Note:** `visibility_timeout` is derived from the flow's `@flow timeout:` (default 60s), not a global config.

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
| `pgflow.prune_data_older_than`   | Clean up old run data                       |
| `pgflow.analyze_and_create_flow` | Compile flow definition from Elixir DSL     |

pgmq functions used:

| Function                          | Purpose                                    |
|-----------------------------------|--------------------------------------------|
| `pgmq.read`                       | Non-blocking message read                  |
| `pgmq.enable_notify_insert`       | Enable LISTEN/NOTIFY on queue inserts      |
| `pgmq.disable_notify_insert`      | Disable LISTEN/NOTIFY on queue             |

## Telemetry

PgFlow emits telemetry events for monitoring. Attach handlers via `:telemetry.attach_many/4` or set `attach_default_logger: true` for built-in logging.

Events:
- `[:pgflow, :flow, :started | :completed | :failed]`
- `[:pgflow, :step, :started | :completed | :failed]`
- `[:pgflow, :task, :started | :completed | :failed]`
- `[:pgflow, :worker, :started | :stopped | :poll | :error]`
