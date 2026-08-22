# Await-Signals Demo

Date: 2026-08-22
Branch: `feat/await-signals`
Depends on: [2026-08-21-await-signals-design.md](2026-08-21-await-signals-design.md)

## Goal

Give the Phoenix demo a third homepage tab that teaches `PgFlow.Context.await_signal/2` and `PgFlow.signal/3` the same way Article teaches the DAG and Onboarding teaches conditionals: a live graph, an event log, and copy-pasteable handler source.

A visitor starts a run, watches `await_approval` park as `waiting`, clicks Approve or Reject, and sees the run complete or fail. The highlighted Elixir is the canonical example.

## Non-goals

- Dashboard `waiting` badge, filter, or run-show polish.
- Timeout / early-buffer / last-write-wins controls on the homepage (timeout stays in the handler source only).
- A standalone `/approval` route or invoice chrome.
- Webhook simulator or extra HTTP endpoint.
- Extending ArticleFlow or OnboardingFlow.
- A Job-based demo (this is a Flow, like the other two tabs).
- Browser/end-to-end tests that boot a worker inside `demo/`.

## Context

The demo LiveView (`FlowDemoLive`) already switches Article vs Onboarding, renders a DAG, highlights `FlowDSL` segments, and applies PubSub tuples (`:task_started`, `:task_completed`, `:task_failed`, `:step_skipped`). Park currently emits no telemetry, so without a new event the graph would leave the node on “running” and never show Approve / Reject.

Library park/resume already works. This spec adds the smallest library hook the homepage needs, plus the third flow and tab.

## Architecture

**Approach: third tab on the existing homepage, plus `task_waiting` telemetry.**

```
Visitor                FlowDemoLive                 ApprovalFlow / worker              Postgres
   |                        |                              |                            |
   | Start                  |                              |                            |
   |----------------------->| start_flow(ApprovalFlow)     |                            |
   |                        |----------------------------->| create_order complete      |
   |                        | task_started/completed       |                            |
   |                        | await_approval starts        |                            |
   |                        | parks, task_waiting          | status = waiting           |
   | Approve / Reject       |                              |                            |
   |----------------------->| PgFlow.signal(run_id,        |                            |
   |                        |   :await_approval, payload)  |                            |
   |                        |----------------------------->| re-queue if waiting        |
   |                        | task_started (re-entry)      |                            |
   |                        | complete or fail             | charge / run terminal      |
```

### Flow

New module `PgflowDemo.Flows.ApprovalFlow`:

```
create_order → await_approval → charge
```

| Step | Role |
|---|---|
| `create_order` | Returns the canned order map. No IO. Completes immediately. |
| `await_approval` | `await_signal` with `wait_timeout: 0` (park at once) and `wait_for: {1, :hour}` (won't expire during a sitting). `max_attempts: 1` so Reject does not retry. |
| `charge` | Depends on `await_approval`. Fake receipt map. Runs only if approval completed. |

Start input is fixed: `%{"order_id" => "ord_demo", "amount" => 42}`. No extra form.

Handler (canonical snippet shown in `FlowDSL`):

```elixir
step :await_approval, depends_on: [:create_order], max_attempts: 1 do
  fn _deps, ctx ->
    case PgFlow.Context.await_signal(ctx, wait_for: {1, :hour}, wait_timeout: 0) do
      {:ok, %{"decision" => "approved"}} -> %{"decision" => "approved"}
      {:ok, _} -> raise "rejected"
      {:error, :timeout} -> raise "no decision"
    end
  end
end
```

`PgFlow.signal/3` targets step slug `:await_approval` (the step name, not a separate signal key).

Register the module on `PgFlow.Supervisor` next to Article and Onboarding. Compile with `mix pgflow.gen.flow_migration PgflowDemo.Flows.ApprovalFlow`.

### Library hook

On park (`{:await_parked}` in `Worker.Server`), emit:

```text
[:pgflow, :worker, :task, :waiting]
```

Metadata matches other task events: `run_id`, `flow_slug`, `step_slug`, `task_index`, `worker_id`.

`PgFlow.Telemetry.PubSub` attaches that event and broadcasts:

```elixir
{:task_waiting, %{step_slug: step_slug, task_index: task_index, timestamp: datetime}}
```

on the existing per-run topic. Do not treat park as `:task_failed`. Do not change `LiveClient` or the dashboard in this spec.

### Homepage

`FlowDemoLive` + `FlowDSL`:

- Tab **Approval** beside Article and Onboarding.
- DAG: three nodes in a vertical chain, same SVG conventions.
- `FlowDSL` compile-time segments for preamble + the three steps.
- Start: one button, no URL/plan form.
- When `steps[:await_approval] == :waiting` and `run_id` is set, show **Approve** and **Reject**. Clicks send `PgFlow.signal(run_id, :await_approval, %{"decision" => "approved" | "rejected"})`. Disable/hide the buttons as soon as the node leaves `:waiting`.
- Event log: a `waiting` line on `task_waiting`; a second **Started** on re-entry after signal (that repeat is the teaching beat).
- Node color for `:waiting` distinct from running / completed / failed / skipped.
- Elapsed timer keeps ticking while the run is started (including waiting).

Reset and tab-switch use the existing unsubscribe/reset path. A parked row left behind in Postgres is not cleaned up by the UI; it expires after the 1-hour deadline or sits until someone signals.

## Data flow

1. Start → `PgFlow.start_flow(ApprovalFlow, %{"order_id" => "ord_demo", "amount" => 42})` → subscribe `"pgflow:run:<run_id>"`.
2. `create_order`: `task_started` / `task_completed`.
3. `await_approval`: `task_started` (node running) → park → `task_waiting` (node waiting, buttons on).
4. Approve: signal approved → re-queue → `task_started` (re-entry) → `await_signal` returns `{:ok, %{"decision" => "approved"}}` → `task_completed` → `charge` → run completed.
5. Reject: signal rejected → re-queue → handler raises `"rejected"` → `task_failed` → run failed. `charge` stays pending.

Early buffer (signal before Start) is possible at the library layer and is not offered in the UI.

## Error handling

| Case | Behavior |
|---|---|
| Reject | Raise `"rejected"` → `fail_task` → run `failed`. `charge` never starts. |
| Signal with no run / already terminal | Library `:ok` no-op. Buttons only render while the step is `:waiting`. |
| Double-click Approve | Last write wins. Second signal is a no-op once the task left `waiting`. UI hides buttons on the first `task_started` after waiting. |
| Reset / switch tab mid-wait | Existing reset. Task remains `waiting` in Postgres until signal or 1-hour deadline. |
| Timeout | Handler raises `"no decision"`. Not reachable from homepage controls. |
| Missing `task_waiting` event | Graph would stick on running and hide buttons — the telemetry hook is in scope for that reason. |

## Testing

Library (pgflow, not demo):

- Parking a task emits `[:pgflow, :worker, :task, :waiting]`.
- PubSub maps it to `{:task_waiting, %{step_slug, task_index, ...}}` (same style as `test/pgflow/telemetry/pubsub_test.exs`).

Demo LiveView (`demo/test/pgflow_demo_web/live/flow_demo_live_test.exs` pattern: synthetic `{:pgflow, run_id, event}` to the LiveView pid):

- Approval tab renders.
- After `:task_waiting` for `await_approval`, Approve / Reject are visible.
- After `:task_started` or `:task_failed` for that step, the buttons are gone.
- Reject path: `:task_failed` then `:run_failed` matches existing failure rendering.

Do not boot `Worker.Server` inside demo tests. Do not add dashboard tests. Do not add a demo-app copy of the library park/resume suite.

## File map

| File | Change |
|---|---|
| `lib/pgflow/worker/server.ex` | Emit `[:pgflow, :worker, :task, :waiting]` on `{:await_parked}` |
| `lib/pgflow/telemetry/pubsub.ex` | Attach and broadcast `{:task_waiting, ...}` |
| `test/pgflow/telemetry/pubsub_test.exs` (or worker test) | Assert the new event |
| `demo/lib/pgflow_demo/flows/approval_flow.ex` | New flow |
| `demo/lib/pgflow_demo/application.ex` | Register flow |
| `demo/lib/pgflow_demo_web/live/flow_demo_live.ex` | Tab, graph, start, buttons, `task_waiting` handler, waiting style |
| `demo/lib/pgflow_demo_web/components/flow_dsl.ex` | Segments for ApprovalFlow |
| `demo/priv/repo/migrations/*_compile_approval_flow.exs` | Generated compile migration |
| `demo/test/pgflow_demo_web/live/flow_demo_live_test.exs` | Tab + waiting buttons |

## Open questions resolved

| Topic | Decision |
|---|---|
| Audience | Homepage visitor + developer reading highlighted source |
| Placement | New third flow/tab, not a step on Article or Onboarding |
| Story | Human approval: `create_order → await_approval → charge` |
| Homepage behaviors | Park + Approve + Reject only |
| Dashboard | Out of scope |
| How the UI learns waiting | New `task_waiting` telemetry/PubSub event |
| Signal address | `run_id` + step slug `:await_approval` |
