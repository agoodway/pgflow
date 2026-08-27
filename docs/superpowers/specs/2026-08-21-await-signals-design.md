# Awaiting Signals

Date: 2026-08-21
Branch: `feat/await-signals`
Inspired by: [Oban Pro — Awaiting Signals](https://oban.pro/docs/pro/Oban.Pro.Worker.html#module-awaiting-signals)

## Goal

Let PgFlow Job and Flow handlers pause mid-execution for an external decision (human approval, webhook callback, out-of-band event), free the worker slot while waiting, and resume when a signal arrives — without holding a process or DB connection open.

This replicates Oban Pro's `await_signal` / `signal` semantics on top of PgFlow's step/task model.

## Non-goals (v1)

- Named signal keys decoupled from `step_slug` (Temporal/Restate style).
- Oban `{:cancel, reason}` return tuples (handlers raise or complete/fail explicitly).
- Erlang-term payloads with atoms/tuples/structs round-tripping (JSONB maps/lists only).
- Dashboard/demo polish for `waiting` beyond whatever falls out of status display.
- Upstream TypeScript pgflow SQL parity (Elixir-owned park + signal store first; upstream sync later if desired).
- Multiple sequential `await_signal` calls in one handler execution (one outstanding await per task).
- Changing conditional step execution or skip semantics except where a waiting step is skipped.

## Context

PgFlow executes handlers to completion, then SQL (`complete_task` / `fail_task`) advances the DAG. There is no Oban-style snooze today. Existing `signal_strategy` / `PgFlow.Signal.Notify` only wake workers for pgmq inserts — unrelated to this feature.

Oban's model:

1. `await_signal/1` inside `process/1`
2. Short live wait (`wait_timeout`, default 5s), then park/snooze
3. `signal/2` from anywhere delivers a durable payload (last write wins; early signals buffer)
4. Job re-enters `process/1` from the top; `await_signal` consumes the buffer
5. `wait_for` deadline yields `{:error, :timeout}`

We adopt the same user-facing lifecycle, parked on PgFlow tasks addressed by `run_id` + `step_slug` (+ optional `task_index` for maps).

## Architecture

**Approach: Elixir park + signal store**

1. Handler calls `PgFlow.Context.await_signal/2`.
2. If a buffered signal exists → consume and return `{:ok, payload}`.
3. Else live-wait up to `wait_timeout`.
4. Else **park**: task status → `waiting`, persist `wait_deadline_at`, archive the pgmq message, free the worker.
5. External code calls `PgFlow.signal(run_id, step_slug, payload)` (or `/4` with `task_index`).
6. Signal upserts the payload. If the task is `waiting`, re-queue it (`queued` + pgmq send).
7. Worker re-enters the handler from the top; `await_signal` returns the payload.
8. A sweeper re-queues `waiting` tasks past `wait_deadline_at` so `await_signal` returns `{:error, :timeout}`.

Parking must be transactional with message archive so we never leave a task `started` without a live worker or a recoverable wait record. Stalled-task recovery **ignores** `waiting` tasks.

```text
Handler                Executor/Worker           task_signals / step_tasks        External
   |                         |                            |                         |
   | await_signal            |                            |                         |
   |------------------------>|                            |                         |
   |                         | check buffer               |                         |
   |                         |--------------------------->|                         |
   |                         | live wait (optional)       |                         |
   |                         | park: waiting + archive    |                         |
   |                         |--------------------------->|                         |
   |                         | slot free                  |                         |
   |                         |                            |    signal(payload)      |
   |                         |                            |<------------------------|
   |                         |                            | upsert; if waiting      |
   |                         |                            | re-queue task           |
   |                         | poll / notify              |                         |
   |                         |<---------------------------|                         |
   | re-enter handler        |                            |                         |
   |<------------------------|                            |                         |
   | await_signal -> {:ok,p} | consume row                |                         |
```

## Components

### API

```elixir
# Inside Job perform or Flow step handler
case PgFlow.Context.await_signal(ctx, wait_for: {24, :hours}, wait_timeout: 5_000) do
  {:ok, %{"decision" => "approved"}} -> charge_card(input)
  {:ok, %{"decision" => "rejected"}} -> raise "rejected"
  {:error, :timeout} -> raise "no decision"
end

# From a controller, another step, IEx, webhook, etc.
PgFlow.signal(run_id, :approval, %{"decision" => "approved"})
PgFlow.signal(run_id, :process_item, 3, %{"decision" => "approved"})
```

| Function | Role |
|---|---|
| `PgFlow.Context.await_signal/2` | Pause current task; must run inside a handler with a real `Context` |
| `PgFlow.signal/3` | Deliver payload to `run_id` + `step_slug` (`task_index` 0) |
| `PgFlow.signal/4` | Same with explicit `task_index` for map tasks |

`signal` is fire-and-forget and returns `:ok`. Targeting a missing or terminal task is a no-op.

### Options (`await_signal`)

| Option | Default | Meaning |
|---|---|---|
| `:wait_for` | `:infinity` | Total wait budget from first park. Integer seconds or `{n, :seconds \| :minutes \| :hours \| :days}` |
| `:wait_timeout` | `5_000` | Milliseconds to block in-process before parking. `0` parks immediately |

Deadline is persisted on first park so subsequent re-entries honor the same deadline.

### Signal store

New table `pgflow.task_signals` (Elixir migration; not vendored from upstream TS):

| Column | Type | Notes |
|---|---|---|
| `run_id` | uuid | FK-ish to run |
| `step_slug` | text | |
| `task_index` | int | Default 0; part of PK |
| `payload` | jsonb | Nullable until signalled; JSON maps/lists only in v1 |
| `wait_deadline_at` | timestamptz | Nullable when `:wait_for` is `:infinity` |
| `timed_out` | boolean | Default `false`; sweeper sets `true` before re-queue |
| `inserted_at` / `updated_at` | timestamptz | |

Primary key: `(run_id, step_slug, task_index)`.

Rows may exist before park (early buffer: payload set, deadline null) or after park (deadline set, payload null until signal).

### Task status

Add `waiting` as a valid `step_tasks.status`.

`step_states.status` stays `started` while any task is `queued`, `started`, or `waiting`. No new step-level status in v1 — the run remains non-terminal because remaining tasks have not completed. Dashboard may later derive a “waiting” display from task rows; that is out of scope.

### Worker / executor

- `await_signal/2` parks by throwing an internal `{:pgflow_await, meta}` that **only** `Executor` catches. Handlers never return this tuple; mid-function pause requires non-local exit.
- Live wait (v1): poll `task_signals` on a short interval inside the handler process until `wait_timeout` elapses. No dedicated NOTIFY channel in v1 (optional later).
- Park path: single transaction → `waiting` + archive pgmq msg + upsert `wait_deadline_at` on `task_signals`.
- Resume path: `signal` sets payload and clears `timed_out`; if status is `waiting`, transition to `queued` and `pgmq.send` so the normal poll path picks it up.
- Stalled recovery: select only `started` (not `waiting`).
- Attempt counter: parking does **not** consume a retry attempt; re-entry after signal uses the same attempt until the handler completes or fails normally.

### Timeout sweeper

Periodic process (alongside or inside stalled-task recovery):

1. Find `waiting` tasks with `wait_deadline_at < now()`.
2. Set `timed_out = true` on the signal row and re-queue the task.
3. On re-entry, `await_signal` sees `timed_out`, returns `{:error, :timeout}`, and deletes the row.

### Jobs and Flows

Same primitive. Jobs are single-step flows; callers signal the job's perform step slug (or the `@job` queue slug when the perform name was omitted).

## Data flow (happy paths)

### Early signal

1. Run starts; webhook calls `signal` before the step runs.
2. Row buffered with payload.
3. Handler reaches `await_signal` → immediate `{:ok, payload}`, no park.

### Live wait

1. Handler awaits; signal arrives within `wait_timeout`.
2. In-process delivery via short-interval poll of `task_signals` → `{:ok, payload}`, no park.

### Park and resume

1. Live wait expires; task parks as `waiting`.
2. Later `signal` upserts payload and re-queues.
3. Handler re-enters from the top; code before `await_signal` runs again (must be idempotent).
4. `await_signal` consumes payload → `{:ok, payload}`; handler finishes → `complete_task`.

### Timeout

1. Parked past deadline; sweeper re-queues with timeout marker.
2. `await_signal` → `{:error, :timeout}`; handler chooses raise/fail or alternate completion.

## Error handling

| Case | Behavior |
|---|---|
| Signal to unknown / completed / failed / skipped | No-op, `:ok` |
| Concurrent signals | Last write wins |
| Crash during live-wait (before park commits) | Task remains `started` → stalled recovery |
| Skip / cascade while `waiting` | Clear signal row; do not re-queue; abandon wait |
| Handler error after resume | Normal `fail_task` / retry |
| Map tasks | Use `signal/4`; `signal/3` targets `task_index` 0 only |

## Testing

- Unit: buffered consume; live-wait then park; signal wake; deadline → `{:error, :timeout}`; last-write-wins.
- Integration: Job + Flow park → external signal → completion with payload-driven branch.
- Races: signal before await; during live-wait; double signal.
- Recovery: sweeper; stalled recovery ignores `waiting`; skip while waiting.
- Test helper pattern: signal before running the worker so drains do not hang on parks (Oban drain analogue).

## Implementation sketch (ordered)

1. Migration: `waiting` status check constraint (if present) + `pgflow.task_signals`.
2. Queries: park, signal upsert, consume, list expired waits, requeue waiting task.
3. `Context.await_signal/2` + `PgFlow.signal/3,4`.
4. Executor/Server park and resume wiring; stalled recovery exclusion.
5. Timeout sweeper.
6. Tests (unit + integration).
7. Docs: README / ARCHITECTURE note distinguishing await-signals from `signal_strategy`.

## Open questions resolved in brainstorming

| Topic | Decision |
|---|---|
| Shape | Oban-style mid-handler await for Jobs **and** Flows |
| Addressing | `run_id` + `step_slug` (+ `task_index` for maps) |
| Durability | Full Oban parity: buffer, live wait, park, `wait_for`, timeout |
| Resume | Re-enter handler from the top |
| Storage | Elixir park + `task_signals` store (Approach A) |
| Payload | JSONB only in v1 (implied by approving components without term encoding) |
| Cancel API | Not in v1 |
| Dashboard | Not a v1 goal |
