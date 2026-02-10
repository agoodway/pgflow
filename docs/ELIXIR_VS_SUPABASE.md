# PgFlow: Elixir vs Supabase (TypeScript/Deno)

Both implementations share the same base PostgreSQL schema and SQL orchestration layer. They can run side-by-side against the same database.

## Architecture

| Layer         | TypeScript/Deno                                   | Elixir                                               |
| ------------- | ------------------------------------------------- | ---------------------------------------------------- |
| DSL           | Fluent builder with full generic type inference   | Macros (`use PgFlow.Flow`, `use PgFlow.Job`)         |
| SQL Core      | 30+ PL/pgSQL functions                            | Same SQL, shared via pg_evolver migrations           |
| Worker        | Stateless Deno edge functions (HTTP-triggered)    | Long-running OTP GenServer per flow/job              |
| Polling       | `pgflow.read_with_poll()` (blocking)              | `pgmq.read()` (non-blocking) + adaptive backoff      |
| Signal        | pgmq polling only                                 | Polling (default) or LISTEN/NOTIFY                   |
| Client        | TS SDK with Supabase Realtime subscriptions       | `PgFlow.Client` with sync polling                    |
| CLI           | `pgflow compile` via control plane HTTP           | `mix pgflow.gen.flow` generates migrations directly  |
| Dashboard     | None (relies on Supabase dashboard)               | Built-in Phoenix LiveView dashboard                  |
| Realtime      | Supabase Realtime broadcast via `realtime.send()` | Telemetry events + structured logging                |
| Recovery      | pgmq visibility timeout + worker restart          | Explicit `StalledTaskRecovery` GenServer             |

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

| Type            | TypeScript           | Elixir                    |
| --------------- | -------------------- | ------------------------- |
| Single step     | `.step()`            | `step/2`, `step/3`.       |
| Array producer  | `.array()`           | `step/2` returning list.  |
| Parallel map    | `.map()`             | `map/2`, `map/3`          |
| Dependent map   | `.map({ array: })`   | `map :name, array: :step` |

## Step/Flow Options

| Option        | TypeScript      | Elixir              | Default                |
| ------------- | --------------- | ------------------- | ---------------------- |
| Identifier    | `slug`          | `queue:` / `slug:`  | —                      |
| Max retries   | `maxAttempts`   | `max_attempts:`     | 1                      |
| Retry delay   | `baseDelay`     | `base_delay:`       | 1s                     |
| Timeout       | `timeout`       | `timeout:`          | 30s (TS), 60s (Elixir) |
| Start delay   | `startDelay`    | `start_delay:`      | 0                      |

## Handler Context

| Field          | TypeScript                   | Elixir                        |
| -------------- | ---------------------------- | ----------------------------- |
| Run ID         | `context.stepTask.run_id`    | `ctx.run_id`                  |
| Step slug      | `context.stepTask.step_slug` | `ctx.step_slug`               |
| Task index     | `context.stepTask.task_index`| `ctx.task_index`              |
| Attempt        | `context.stepTask.attempt`   | `ctx.attempt`                 |
| Flow input     | `await context.flowInput`    | `Context.get_flow_input(ctx)` |
| DB access      | `context.sql` (postgres.js)  | `ctx.repo` (Ecto)             |
| Shutdown signal| `context.shutdownSignal`     | OTP supervision               |
| Environment    | `context.env`                | `Application.get_env/3`       |

Both lazy-load flow input to avoid unnecessary DB queries for non-root steps.

## Worker Model

### TypeScript: Stateless Edge Functions

```
Supabase Edge Function (triggered per batch)
  → register worker → heartbeat → processBatch → exit
  → Horizontal scaling via multiple function instances
  → Worker deprecation via is_deprecated flag
  → ensure_workers() pings functions via pg_cron
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

| Aspect                | TypeScript                       | Elixir                        |
| --------------------- | -------------------------------- | ----------------------------- |
| Lifecycle             | Ephemeral per invocation         | Persistent GenServer          |
| State                 | Stateless                        | In-memory (active_tasks map)  |
| Scaling               | Multiple edge function instances | One worker per flow/job       |
| Crash recovery        | New function invocation          | OTP supervisor restart        |
| Stale task recovery   | pgmq visibility timeout          | Explicit sweep every 15s      |
| Worker heartbeat      | `last_heartbeat_at` in DB        | DB registration + OTP monitor |
| Worker deprecation    | `deprecated_at` flag             | Not needed (OTP restarts)     |
| Graceful shutdown     | `shutdownSignal`                 | Lifecycle state machine       |

## Polling Protocol

Both use the same two-phase protocol:

1. Reserve messages from pgmq (makes them invisible)
2. `pgflow.start_tasks()` — atomically claim tasks, return details
3. Execute handler
4. `pgflow.complete_task()` or `pgflow.fail_task()`

**Difference in phase 1:**

| Aspect       | TypeScript                           | Elixir                              |
| ------------ | ------------------------------------ | ----------------------------------- |
| Read call    | `pgflow.read_with_poll()` (blocking) | `pgmq.read()` (non-blocking)        |
| Idle behavior| Blocks in DB up to `maxPollSeconds`  | Adaptive jittered backoff (1s-5s)   |
| Wake-up      | pgmq polling only                    | Polling or LISTEN/NOTIFY            |

TypeScript uses blocking `read_with_poll` because stateless edge functions have no event loop to schedule polls — the database must do the waiting. Elixir workers are long-running GenServers where `Process.send_after` provides native scheduling, so `pgmq.read()` returns instantly and the connection goes back to the Ecto pool. This avoids holding DB connections idle and enables adaptive backoff with cancellable timers (NOTIFY or completion events can wake the worker immediately).

Note that `read_with_poll` is not truly push-based — PostgreSQL runs an internal poll loop at `pollIntervalMs` (default 200ms) inside the blocking call:

```
Deno worker                    PostgreSQL
   |                              |
   |---- read_with_poll() ------->|
   |     (connection held)        |-- SELECT FROM q_* ... nothing
   |                              |-- wait 200ms
   |                              |-- SELECT FROM q_* ... nothing
   |                              |-- wait 200ms
   |                              |-- SELECT FROM q_* ... MESSAGE!
   |<--- [message] ---------------|
```

The real question is where the poll loop lives and what tradeoffs that creates:

|                              | Deno (pgmq)             | Elixir (polling)       | Elixir (notify)             |
| ---------------------------- | ----------------------- | ---------------------- | --------------------------- |
| **Poll loop location**       | Inside PostgreSQL       | GenServer process      | N/A (event-driven)          |
| **Busy latency**             | ~200ms (fixed)          | ~50ms (faster)         | Near-zero                   |
| **Idle latency**             | ~200ms (fixed)          | Up to 5s (slower)      | Near-zero + 30s fallback    |
| **DB connection during idle**| Held for up to 5s       | Not held               | Not held                    |
| **Truly push-based?**        | No                      | No                     | Yes                         |

## Feature Comparison

| Feature                    | TypeScript | Elixir | Notes                                          |
| -------------------------- | ---------- | ------ | ---------------------------------------------- |
| Single steps               | Yes        | Yes    | Identical semantics                            |
| Map steps                  | Yes        | Yes    | Identical                                      |
| Dependent map steps        | Yes        | Yes    | Identical                                      |
| Empty array cascade        | Yes        | Yes    | Same SQL logic                                 |
| Exponential backoff retry  | Yes        | Yes    | Same SQL function                              |
| Compile-time DAG validation| Yes        | Yes    | TS generics vs Kahn's algorithm                |
| Two-phase polling          | Yes        | Yes    | Same protocol                                  |
| start_delay option         | Yes        | Yes    | Per-step delay before queueing                 |
| Flow input lazy loading    | Yes        | Yes    | `context.flowInput` / `Context.get_flow_input` |
| Type safety                | Yes        | No     | TS generics flow through entire DAG            |
| Client SDK (browser)       | Yes        | No     | Supabase Realtime subscriptions                |
| Realtime events (WebSocket)| Yes        | No     | Elixir uses telemetry instead                  |
| Worker deprecation         | Yes        | No     | OTP supervisors handle restarts                |
| LISTEN/NOTIFY signal       | No         | Yes    | pgmq 1.8.0+ with `enable_notify_insert`        |
| Stalled task recovery      | No         | Yes    | StalledTaskRecovery GenServer                  |
| Jobs DSL                   | No         | Yes    | `use PgFlow.Job` with `perform` block          |
| Cron scheduling            | No         | Yes    | `cron: [schedule: "@hourly"]` via pg_cron      |
| Dashboard                  | No         | Yes    | LiveView: flows, jobs, crons, runs, workers    |
| Sync flow execution        | Via client | Yes    | `PgFlow.Client.start_flow_sync/3`              |
| Telemetry events           | No         | Yes    | 12 events across flow/step/task/worker         |
| Structured logging         | No         | Yes    | Fancy (dev) and simple (prod) formats          |
| Mix tasks                  | N/A        | Yes    | gen.flow, check_schema, copy_migrations, etc.  |

## Shared SQL Core

Both call the same PostgreSQL functions:

| Function                                 | Purpose                                    |
| ---------------------------------------- | ------------------------------------------ |
| `pgflow.create_flow`                     | Register flow + create pgmq queue          |
| `pgflow.add_step`                        | Add step with dependencies                 |
| `pgflow.start_flow`                      | Create run, enqueue root steps             |
| `pgflow.start_tasks`                     | Claim tasks, return details                |
| `pgflow.complete_task`                   | Save output, cascade to dependents         |
| `pgflow.fail_task`                       | Retry with backoff or fail permanently     |
| `pgflow.start_ready_steps`               | Activate steps with all deps satisfied     |
| `pgflow.cascade_complete_taskless_steps` | Handle empty array propagation             |
| `pgflow.maybe_complete_run`              | Complete run when all steps done           |

Database schema (7 tables): `flows`, `steps`, `deps` (definition) + `runs`, `step_states`, `step_tasks`, `workers` (runtime).

## Schema Divergences

The Elixir implementation adds the following extensions that are **not present** in the upstream TypeScript/Deno project:

| Change                  | Table          | Description                                                                        |
|-------------------------|----------------|------------------------------------------------------------------------------------|
| `flow_type` column      | `pgflow.flows` | Distinguishes background jobs from multi-step DAG workflows in the dashboard.     |
| Extension SQL functions | `pgflow`       | `register_worker`, `mark_worker_stopped`, `recover_stalled_tasks`, `flow_exists`, `get_flow_input`, `get_step_output` — installed via `mix pgflow.gen.extensions_migration`. |

These additions are backward-compatible: existing flow records default to `flow_type = 'flow'`, and extension functions don't modify core pgflow tables. TypeScript workers can safely ignore them.

## Compilation

**TypeScript:** CLI sends HTTP request to ControlPlane edge function, which extracts the flow shape and generates SQL. Written to `supabase/migrations/`.

**Elixir:** `mix pgflow.gen.flow MyFlow` reads `__pgflow_definition__/0` at compile time and generates an Ecto migration with the same SQL calls. Also compiles flows at worker startup via `FlowCompiler`.
