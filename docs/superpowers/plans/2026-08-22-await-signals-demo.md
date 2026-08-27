# Await-Signals Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Approval tab to the Phoenix demo that parks `await_approval` as `waiting`, lets the visitor Approve or Reject via `PgFlow.signal/3`, and shows the canonical handler source.

**Architecture:** Emit `[:pgflow, :worker, :task, :waiting]` when the worker parks, bridge it to `{:task_waiting, ...}` on the existing per-run PubSub topic, and wire a third homepage flow (`create_order → await_approval → charge`) into `FlowDemoLive` / `FlowDSL` the same way Onboarding was added.

**Tech Stack:** Elixir, Phoenix LiveView, PgFlow (`Context.await_signal/2`, `Client.signal/3`), ExUnit (`PgFlow.Telemetry.PubSubTest`, `PgflowDemoWeb.ConnCase`).

**Spec:** `docs/superpowers/specs/2026-08-22-await-signals-demo-design.md`

## Global Constraints

- Third homepage tab only — no dashboard `waiting` polish, no `/approval` route, no webhook simulator.
- Homepage behaviors: park + Approve + Reject. Timeout stays in handler source (`wait_for: {1, :hour}`, `wait_timeout: 0`).
- Signal address is `run_id` + step slug `:await_approval`.
- `await_approval` has `max_attempts: 1` so Reject does not retry.
- Do not boot `Worker.Server` inside `demo/` tests.
- Do not treat park as `:task_failed`. Do not change `LiveClient`.
- Follow existing demo patterns (`FlowDemoLive` tabs, `FlowDSL` compile-time segments, synthetic PubSub messages in LiveView tests).

## File map

| File | Responsibility |
|---|---|
| `lib/pgflow/worker/server.ex` | Emit `[:pgflow, :worker, :task, :waiting]` on `{:await_parked}` |
| `lib/pgflow/telemetry/pubsub.ex` | Attach that event; broadcast `{:task_waiting, %{step_slug, task_index, timestamp}}` |
| `lib/pgflow/telemetry.ex` | Document the new event; include it in the default logger event list |
| `test/pgflow/telemetry/pubsub_test.exs` | Assert PubSub mapping |
| `test/pgflow/await_signals_test.exs` | Assert park emits the telemetry event |
| `demo/lib/pgflow_demo/flows/approval_flow.ex` | New flow |
| `demo/lib/pgflow_demo/application.ex` | Register `ApprovalFlow` |
| `demo/priv/repo/migrations/*_compile_approval_flow.exs` | Generated compile migration |
| `demo/lib/pgflow_demo_web/live/flow_demo_live.ex` | Tab, graph, start, buttons, `task_waiting` handler, waiting styles |
| `demo/lib/pgflow_demo_web/components/flow_dsl.ex` | Segments + waiting highlight class |
| `demo/test/pgflow_demo_web/live/flow_demo_live_test.exs` | Tab + waiting UI |
| `demo/README.md` | Mention the Approval tab |

---

### Task 1: `task_waiting` telemetry + PubSub

**Files:**
- Modify: `lib/pgflow/worker/server.ex` (`apply_task_result/5` for `{:await_parked}`)
- Modify: `lib/pgflow/telemetry/pubsub.ex` (`@task_events` + `handle_event/4`)
- Modify: `lib/pgflow/telemetry.ex` (event list in moduledoc and `attach_default_logger/0`)
- Test: `test/pgflow/telemetry/pubsub_test.exs`
- Test: `test/pgflow/await_signals_test.exs`

**Interfaces:**
- Consumes: existing `apply_task_result({:await_parked}, task_meta, ...)` and `emit_telemetry/3`
- Produces: telemetry event `[:pgflow, :worker, :task, :waiting]` with metadata `%{run_id, flow_slug, step_slug, task_index, worker_id}`; PubSub payload `{:task_waiting, %{step_slug :: String.t() | atom(), task_index :: integer(), timestamp :: DateTime.t()}}` on `"pgflow:run:<run_id>"` and `"pgflow:tasks"`

- [ ] **Step 1: Write the failing PubSub test**

Add to the `"task events"` describe in `test/pgflow/telemetry/pubsub_test.exs` (same attach setup as `task:start`):

```elixir
test "task:waiting broadcasts to per-run and global topics", %{pubsub: pubsub} do
  run_id = Ecto.UUID.generate()
  Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")
  Phoenix.PubSub.subscribe(pubsub, "pgflow:tasks")

  :telemetry.execute(
    [:pgflow, :worker, :task, :waiting],
    %{},
    %{run_id: run_id, step_slug: "await_approval", task_index: 0}
  )

  assert_receive {:pgflow, ^run_id, {:task_waiting, payload}}
  assert payload.step_slug == "await_approval"
  assert payload.task_index == 0
  assert %DateTime{} = payload.timestamp

  assert_receive {:pgflow, ^run_id, {:task_waiting, _}}
end
```

- [ ] **Step 2: Run the PubSub test to verify it fails**

Run: `mix test test/pgflow/telemetry/pubsub_test.exs --only line:<line_of_new_test>`
Expected: FAIL — no `handle_event` clause / no broadcast for `[:pgflow, :worker, :task, :waiting]` (assert_receive timeout).

- [ ] **Step 3: Bridge the event in PubSub**

In `lib/pgflow/telemetry/pubsub.ex`:

1. Add `[:pgflow, :worker, :task, :waiting]` to `@task_events`.
2. Add a clause next to `handle_event([:pgflow, :worker, :task, :start], ...)`:

```elixir
def handle_event([:pgflow, :worker, :task, :waiting], _measurements, metadata, config) do
  run_id = normalize_uuid(metadata.run_id)

  payload =
    {:task_waiting,
     %{
       step_slug: metadata.step_slug,
       task_index: metadata.task_index,
       timestamp: DateTime.utc_now()
     }}

  broadcast(config.pubsub, run_id, payload, :task)
end
```

In `lib/pgflow/telemetry.ex` moduledoc under Task Execution, add:

```text
- `[:pgflow, :worker, :task, :waiting]` — Task parked on `await_signal` (worker slot freed)
```

Add `[:pgflow, :worker, :task, :waiting]` to the `attach_default_logger/0` events list. `handle_event/4` already has a catch-all no-op; do not add a dedicated logger clause.

- [ ] **Step 4: Run the PubSub test to verify it passes**

Run: `mix test test/pgflow/telemetry/pubsub_test.exs`
Expected: PASS (including the new test).

- [ ] **Step 5: Write the failing worker emission test**

In `test/pgflow/await_signals_test.exs`, in `"parks then resumes when signalled"`, attach waiting telemetry the same way the test already attaches exception telemetry, and after `wait_until` sees status `"waiting"`, assert the event was received:

```elixir
:telemetry_test.attach_event_handlers(self(), [
  [:pgflow, :worker, :task, :exception],
  [:pgflow, :worker, :task, :waiting]
])

# ... start worker, start run, wait_until waiting ...

assert_received {[:pgflow, :worker, :task, :waiting], _, _,
                 %{step_slug: "approval", run_id: ^run_id}}
```

Use the actual step slug of that test flow (`"approval"` in `ApprovalFlow` in this file — do not confuse it with the demo's `:await_approval`).

- [ ] **Step 6: Run the worker test to verify it fails**

Run: `mix test test/pgflow/await_signals_test.exs`
Expected: FAIL — `assert_received` no matching `[:pgflow, :worker, :task, :waiting]`.

- [ ] **Step 7: Emit telemetry on park**

In `lib/pgflow/worker/server.ex`, change `apply_task_result/5` for `{:await_parked}` to use `task_meta` (it is currently ignored) and emit before freeing the slot:

```elixir
defp apply_task_result({:await_parked}, task_meta, new_active_tasks, _was_at_capacity, state) do
  emit_telemetry([:worker, :task, :waiting], %{}, %{
    worker_id: state.worker_id,
    flow_slug: state.flow_slug,
    run_id: task_meta.run_id,
    step_slug: task_meta.step_slug,
    task_index: task_meta.task_index
  })

  state = %{state | active_tasks: new_active_tasks}

  if Lifecycle.can_accept_work?(state.lifecycle) do
    schedule_immediate_poll(state)
  else
    state
  end
end
```

Do not emit `:exception` on this path. Do not call `fail_task` / `complete_task`.

- [ ] **Step 8: Run worker + PubSub tests**

Run:

```bash
mix test test/pgflow/await_signals_test.exs test/pgflow/telemetry/pubsub_test.exs
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add lib/pgflow/worker/server.ex lib/pgflow/telemetry/pubsub.ex lib/pgflow/telemetry.ex \
  test/pgflow/telemetry/pubsub_test.exs test/pgflow/await_signals_test.exs
git commit -m "feat: Emit task_waiting telemetry when a task parks"
```

---

### Task 2: `ApprovalFlow` + compile migration + supervisor

**Files:**
- Create: `demo/lib/pgflow_demo/flows/approval_flow.ex`
- Modify: `demo/lib/pgflow_demo/application.ex` (`flows:` list)
- Create: `demo/priv/repo/migrations/<timestamp>_compile_approval_flow.exs` (via mix task)
- Test: `demo/test/pgflow_demo/approval_flow_test.exs`

**Interfaces:**
- Consumes: `PgFlow.Flow`, `PgFlow.Context.await_signal/2`
- Produces: module `PgflowDemo.Flows.ApprovalFlow` with `__pgflow_slug__() == :approval_flow` and steps `:create_order`, `:await_approval` (`depends_on: [:create_order]`, `max_attempts: 1`), `:charge` (`depends_on: [:await_approval]`)

- [ ] **Step 1: Write the failing definition test**

Create `demo/test/pgflow_demo/approval_flow_test.exs`:

```elixir
defmodule PgflowDemo.ApprovalFlowTest do
  use ExUnit.Case, async: true

  alias PgflowDemo.Flows.ApprovalFlow

  test "defines a three-step approval chain" do
    defn = ApprovalFlow.__pgflow_definition__()

    assert ApprovalFlow.__pgflow_slug__() == :approval_flow
    assert Enum.map(defn.steps, & &1.slug) == [:create_order, :await_approval, :charge]

    await = Enum.find(defn.steps, &(&1.slug == :await_approval))
    assert await.depends_on == [:create_order]
    assert await.max_attempts == 1

    charge = Enum.find(defn.steps, &(&1.slug == :charge))
    assert charge.depends_on == [:await_approval]
  end
end
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd demo && mix test test/pgflow_demo/approval_flow_test.exs`
Expected: FAIL — `PgflowDemo.Flows.ApprovalFlow` is undefined.

- [ ] **Step 3: Implement the flow**

Create `demo/lib/pgflow_demo/flows/approval_flow.ex` with this exact source (line numbers feed Task 3's `FlowDSL` segments):

```elixir
defmodule PgflowDemo.Flows.ApprovalFlow do
  @moduledoc """
  Demo flow that parks for a human approval signal.

  DAG Structure:
  ```
  create_order → await_approval → charge
  ```
  """

  use PgFlow.Flow

  @flow queue: :approval_flow, max_attempts: 3, base_delay: 1, timeout: 30

  step :create_order do
    fn input, _ctx ->
      %{
        "order_id" => input["order_id"],
        "amount" => input["amount"]
      }
    end
  end

  step :await_approval, depends_on: [:create_order], max_attempts: 1 do
    fn _deps, ctx ->
      case PgFlow.Context.await_signal(ctx, wait_for: {1, :hour}, wait_timeout: 0) do
        {:ok, %{"decision" => "approved"}} -> %{"decision" => "approved"}
        {:ok, _} -> raise "rejected"
        {:error, :timeout} -> raise "no decision"
      end
    end
  end

  step :charge, depends_on: [:await_approval] do
    fn deps, _ctx ->
      %{
        "charged" => true,
        "order_id" => deps["create_order"]["order_id"],
        "amount" => deps["create_order"]["amount"],
        "decision" => deps["await_approval"]["decision"]
      }
    end
  end
end
```

In `demo/lib/pgflow_demo/application.ex`, add `PgflowDemo.Flows.ApprovalFlow` to the `flows:` list next to Article and Onboarding.

- [ ] **Step 4: Run the definition test**

Run: `cd demo && mix test test/pgflow_demo/approval_flow_test.exs`
Expected: PASS.

- [ ] **Step 5: Generate the compile migration**

From `demo/`:

```bash
mix pgflow.gen.flow_migration PgflowDemo.Flows.ApprovalFlow
```

Confirm the generated `up` includes `create_flow('approval_flow', ...)` and `add_step` for `create_order`, `await_approval` (deps `create_order`, max_attempts 1), and `charge` (deps `await_approval`). Do not hand-write the SQL; if the generator is wrong, fix the flow options and regenerate.

- [ ] **Step 6: Commit**

```bash
git add demo/lib/pgflow_demo/flows/approval_flow.ex demo/lib/pgflow_demo/application.ex \
  demo/priv/repo/migrations/*compile_approval_flow.exs demo/test/pgflow_demo/approval_flow_test.exs
git commit -m "feat: Add ApprovalFlow demo for await_signal"
```

---

### Task 3: Homepage Approval tab

**Files:**
- Modify: `demo/lib/pgflow_demo_web/live/flow_demo_live.ex`
- Modify: `demo/lib/pgflow_demo_web/components/flow_dsl.ex`
- Modify: `demo/test/pgflow_demo_web/live/flow_demo_live_test.exs`
- Modify: `demo/README.md`

**Interfaces:**
- Consumes: `{:task_waiting, %{step_slug: _, task_index: _}}` PubSub tuple; `PgflowDemo.Flows.ApprovalFlow`; `Client.signal/3` (same repo resolution as `Client.start_flow/2`)
- Produces: tab key `:approval`; graph steps `:create_order`, `:await_approval`, `:charge`; events `"signal_approval"` with `phx-value-decision` `"approved"` | `"rejected"`; node/log state `:waiting`

- [ ] **Step 1: Write failing LiveView tests**

Add to `demo/test/pgflow_demo_web/live/flow_demo_live_test.exs` (synthetic messages, no worker):

```elixir
test "Approval tab renders a start form without the article URL field", %{conn: conn} do
  {:ok, view, _html} = live(conn, "/")

  html = view |> element("#tab-approval") |> render_click()
  assert html =~ "Start Flow"
  refute html =~ ~s(name="url")
  assert html =~ "create_order" or html =~ "await_approval"
end

test "task_waiting for await_approval shows Approve and Reject", %{conn: conn} do
  {:ok, view, _html} = live(conn, "/")
  view |> element("#tab-approval") |> render_click()

  send(
    view.pid,
    {:pgflow, "fake-run-id", {:task_waiting, %{step_slug: "await_approval", task_index: 0}}}
  )

  html = render(view)
  assert html =~ "Waiting"
  assert has_element?(view, "#approval-approve")
  assert has_element?(view, "#approval-reject")
end

test "task_started after waiting hides Approve and Reject", %{conn: conn} do
  {:ok, view, _html} = live(conn, "/")
  view |> element("#tab-approval") |> render_click()

  send(
    view.pid,
    {:pgflow, "fake-run-id", {:task_waiting, %{step_slug: "await_approval", task_index: 0}}}
  )

  assert has_element?(view, "#approval-approve")

  send(
    view.pid,
    {:pgflow, "fake-run-id", {:task_started, %{step_slug: "await_approval", task_index: 0}}}
  )

  html = render(view)
  refute has_element?(view, "#approval-approve")
  refute has_element?(view, "#approval-reject")
  assert html =~ "Started"
end
```

Buttons may render from `:waiting` alone (no `run_id`). The click handler must no-op when `run_id` is nil so these tests do not need Postgres.

- [ ] **Step 2: Run the LiveView tests to verify they fail**

Run: `cd demo && mix test test/pgflow_demo_web/live/flow_demo_live_test.exs`
Expected: FAIL — no `#tab-approval` / no `task_waiting` handler / no buttons.

- [ ] **Step 3: Wire FlowDSL segments**

In `demo/lib/pgflow_demo_web/components/flow_dsl.ex`:

- Alias `ApprovalFlow`.
- `@approval_source_path "lib/pgflow_demo/flows/approval_flow.ex"` and `@external_resource`.
- Segment defs from the Task 2 source (adjust if `mix format` moved lines; read the file):

```elixir
@approval_segment_defs [
  %{id: :preamble, lines: 1..14, clickable: false},
  %{id: :create_order, lines: 16..24, clickable: true},
  %{id: :await_approval, lines: 26..34, clickable: true},
  %{id: :charge, lines: 36..46, clickable: true}
]
```

- `@processed_approval_segments process_segments.(@approval_source_path, @approval_segment_defs)`
- `def get_segments(ApprovalFlow), do: @processed_approval_segments`

Add `status_class(:waiting, _), do: "bg-amber-500/20"` so the highlighted source tracks the parked step.

- [ ] **Step 4: Wire FlowDemoLive**

In `demo/lib/pgflow_demo_web/live/flow_demo_live.ex`:

**Maps** (next to article/onboarding):

```elixir
@flow_modules %{
  article: PgflowDemo.Flows.ArticleFlow,
  onboarding: PgflowDemo.Flows.OnboardingFlow,
  approval: PgflowDemo.Flows.ApprovalFlow
}

# inside @flows:
approval: %{
  slug: :approval_flow,
  steps: [
    %{slug: :create_order, label: "Order", x: 100, y: 40},
    %{slug: :await_approval, label: "Approve", x: 100, y: 110},
    %{slug: :charge, label: "Charge", x: 100, y: 180}
  ],
  edges: [
    {:create_order, :await_approval},
    {:await_approval, :charge}
  ]
}
```

**Parse / start:**

```elixir
defp parse_flow_key("article"), do: :article
defp parse_flow_key("onboarding"), do: :onboarding
defp parse_flow_key("approval"), do: :approval
defp parse_flow_key(_), do: nil

@impl true
def handle_event("start_flow", _params, %{assigns: %{selected_flow: :approval}} = socket) do
  start_selected_flow(socket, :approval_flow, %{
    "order_id" => "ord_demo",
    "amount" => 42
  })
end
```

Keep the onboarding and article `start_flow` clauses above/beside this one (onboarding already matches on `selected_flow`).

**Waiting handler** (before the catch-all `handle_info(_msg, socket)`):

```elixir
@impl true
def handle_info(
      {:pgflow, _run_id, {:task_waiting, %{step_slug: step_slug, task_index: task_index}}},
      socket
    ) do
  case to_step_atom(step_slug, socket.assigns.steps_config) do
    nil ->
      {:noreply, socket}

    step_atom ->
      steps = update_step_status(socket.assigns.steps, step_atom, :waiting)

      socket =
        socket
        |> assign(:steps, steps)
        |> assign(:active_edges, MapSet.new())
        |> assign(:highlighted_step, step_atom)
        |> push_event("scroll_dsl_pane", %{step: to_string(step_atom)})
        |> add_log(
          :info,
          "Waiting",
          "#{format_step_label(step_atom)} [task #{task_index}]",
          step_atom
        )

      {:noreply, socket}
  end
end
```

**Signal clicks:**

```elixir
@impl true
def handle_event("signal_approval", %{"decision" => decision}, socket)
    when decision in ["approved", "rejected"] do
  run_id = socket.assigns.run_id

  if run_id && socket.assigns.steps[:await_approval] == :waiting do
    _ = Client.signal(run_id, :await_approval, %{"decision" => decision})
  end

  {:noreply, socket}
end
```

**Styles** (distinct from purple running / green completed):

```elixir
defp step_color(:waiting), do: "#F59E0B"
defp node_stroke(:waiting), do: "#FBBF24"
defp node_label_fill(:waiting), do: "#FBBF24"
```

In the SVG node loop, add a waiting mark next to the running spinner (amber pause bars or the word is optional; a dashed amber inner circle is enough):

```elixir
<%= if status == :waiting do %>
  <circle
    cx={step.x}
    cy={step.y}
    r="4"
    fill="none"
    stroke="white"
    stroke-width="1"
    stroke-dasharray="2 2"
  />
<% end %>
```

**HEEx:** add a third tab button `#tab-approval` (`phx-value-flow="approval"`). Add an approval start form (`:if={@selected_flow == :approval}`) with Start Flow / Reset copied from onboarding but without plan/email fields. Below that form, when `@selected_flow == :approval` and `Map.get(@steps, :await_approval) == :waiting`:

```elixir
<div id="approval-actions" class="mt-4 flex gap-3">
  <button
    id="approval-approve"
    type="button"
    phx-click="signal_approval"
    phx-value-decision="approved"
    class="px-6 py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-semibold rounded-xl"
  >
    Approve
  </button>
  <button
    id="approval-reject"
    type="button"
    phx-click="signal_approval"
    phx-value-decision="rejected"
    class="px-6 py-3 bg-red-600 hover:bg-red-500 text-white font-semibold rounded-xl"
  >
    Reject
  </button>
</div>
```

Run status stays `:running` while waiting so the existing timer keeps ticking and Start stays hidden.

- [ ] **Step 5: README**

In `demo/README.md`, mention the Approval tab next to the article DAG (one sentence: park with Approve / Reject; not `signal_strategy`).

- [ ] **Step 6: Run LiveView tests**

Run: `cd demo && mix test test/pgflow_demo_web/live/flow_demo_live_test.exs test/pgflow_demo/approval_flow_test.exs`
Expected: PASS. Existing skip/failure LiveView tests still pass.

- [ ] **Step 7: Commit**

```bash
git add demo/lib/pgflow_demo_web/live/flow_demo_live.ex \
  demo/lib/pgflow_demo_web/components/flow_dsl.ex \
  demo/test/pgflow_demo_web/live/flow_demo_live_test.exs \
  demo/README.md
git commit -m "feat: Add Approval tab that parks and signals await_approval"
```

---

## Spec coverage checklist

| Spec requirement | Task |
|---|---|
| `[:pgflow, :worker, :task, :waiting]` on park | 1 |
| PubSub `{:task_waiting, ...}` | 1 |
| `ApprovalFlow` DAG + handler snippet | 2 |
| Supervisor registration + compile migration | 2 |
| Homepage tab, graph, DSL, Start | 3 |
| Approve / Reject → `signal(run_id, :await_approval, ...)` | 3 |
| Node/log `:waiting`; re-entry `task_started` hides buttons | 3 |
| No dashboard / no timeout UI / no worker in demo tests | Global (omitted) |

## Placeholder / consistency notes

- Demo step slug is `:await_approval`. Library `await_signals_test.exs` still uses its own `:approval` step — do not rename that test flow.
- `Client.signal/3` must exist from the await-signals library work; do not add a second client API.
- `FlowDSL` line ranges must match the formatted `approval_flow.ex`; re-read the file after `mix format` if the generator or formatter shifts lines.
- `step_states.status` stays `started` while a task is `waiting`. The LiveView waiting state comes from `task_waiting`, not from `merge_step_statuses/3`.
