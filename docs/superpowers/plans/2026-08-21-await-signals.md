# Awaiting Signals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Job and Flow handlers pause mid-execution with `Context.await_signal/2`, free the worker while parked as `waiting`, and resume via `PgFlow.signal/3,4` with Oban-parity buffering and timeouts.

**Architecture:** Elixir-owned helpers V05 adds `waiting` to `step_tasks.valid_status` and a `pgflow.task_signals` store plus SQL park/signal/consume/expire functions. `await_signal` live-waits then parks (archive pgmq + status `waiting`) and throws an internal token the worker catches without failing the task. Resume re-queues through normal `pgmq` + `start_tasks`. A sweeper re-queues expired waits so `await_signal` returns `{:error, :timeout}`.

**Tech Stack:** Elixir, Ecto, PostgreSQL / pgmq, ExUnit (`PgFlow.IntegrationCase` / worker server tests), EctoEvolver helpers migrations.

**Spec:** `docs/superpowers/specs/2026-08-21-await-signals-design.md`

## Global Constraints

- Oban-style mid-handler await for **Jobs and Flows** (same `Context` API).
- Address signals by `run_id` + `step_slug` (+ `task_index`; default `0`).
- Full Oban parity: early buffer (last write wins), `wait_timeout` live wait, park, persisted `wait_for`, `{:error, :timeout}`.
- Resume re-enters the handler from the top; pre-await code must be idempotent.
- JSONB payloads only (maps/lists); no Erlang-term encoding.
- No `{:cancel, reason}` API in v1.
- No dashboard/demo polish in v1.
- Do not broaden `recover_stalled_tasks` to `waiting` — it must keep `st.status = 'started'`.
- Parking must not consume a retry attempt (`attempts_count` restored on resume via park decrement + `start_tasks` increment).
- Distinguish await-signals from existing `signal_strategy` / `PgFlow.Signal.Notify` (pgmq wake only).
- One outstanding await per task (no sequential multi-await state machine).
- Prefer new focused modules (`Queries.Signals`, sweeper GenServer) over growing `flows.ex` / `server.ex` without bound.
- Spec mentions Executor catching the park throw; production dispatch is `Worker.Server` (Executor is unit-tested only). Implement the catch in `Server.dispatch_task/2`.

---

## File map

| File | Responsibility |
|---|---|
| `priv/pgflow_helpers/sql/versions/v05/v05_up.sql` | Widen `valid_status`; create `task_signals`; park/signal/consume/expire SQL |
| `priv/pgflow_helpers/sql/versions/v05/v05_down.sql` | Reverse V05 |
| `lib/pgflow/migrations/versions/v05.ex` | EctoEvolver version module |
| `lib/pgflow/helpers_migration.ex` | Register V05 |
| `lib/pgflow/queries/signals.ex` | Elixir RPC wrappers for signal SQL |
| `lib/pgflow/context.ex` | `await_signal/2`; optional `flow_slug` / `message_id` fields |
| `lib/pgflow/client.ex` | `signal/3`, `signal/4` |
| `lib/pgflow.ex` | `defdelegate signal/...` |
| `lib/pgflow/worker/server.ex` | Catch park token; populate Context `flow_slug`/`message_id`; do not fail_task on park |
| `lib/pgflow/worker/waiting_task_recovery.ex` | Sweeper for `wait_deadline_at` expiry |
| `lib/pgflow/config.ex` | `:waiting_recovery_interval` |
| `lib/pgflow/supervisor.ex` | Start waiting sweeper |
| `test/pgflow/migrations/versions/v05_test.exs` | Registration / up-down contract |
| `test/pgflow/queries/signals_test.exs` | SQL wrapper integration |
| `test/pgflow/context_await_signal_test.exs` | Unit/integration for await + buffer |
| `test/pgflow/await_signals_test.exs` | Job + Flow end-to-end park/resume/timeout |
| `docs/ARCHITECTURE.md`, `README.md` | Note await-signals vs `signal_strategy` |

---

### Task 1: Helpers V05 — `waiting` status + `task_signals` + SQL functions

**Files:**
- Create: `priv/pgflow_helpers/sql/versions/v05/v05_up.sql`
- Create: `priv/pgflow_helpers/sql/versions/v05/v05_down.sql`
- Create: `lib/pgflow/migrations/versions/v05.ex`
- Modify: `lib/pgflow/helpers_migration.ex` (append `V05` to `versions:`)
- Test: `test/pgflow/migrations/versions/v05_test.exs`

**Interfaces:**
- Consumes: helpers V04 installed; core `step_tasks.valid_status` includes `queued|started|completed|failed`
- Produces: SQL functions (all in `pgflow` schema via `$SCHEMA$`):
  - `park_waiting_task(run_id uuid, step_slug text, task_index int, wait_deadline_at timestamptz)` → voids; sets task `waiting`, decrements `attempts_count`, archives `message_id`, upserts signal row deadline
  - `signal_task(run_id uuid, step_slug text, task_index int, payload jsonb)` → void; upserts payload, clears `timed_out`; if task `waiting`, requeues (`queued` + `pgmq.send` + new `message_id`)
  - `consume_task_signal(run_id uuid, step_slug text, task_index int)` → TABLE(payload jsonb, timed_out boolean); deletes row when returning a result
  - `expire_waiting_tasks()` → bigint count; marks `timed_out`, requeues waiting past deadline

- [ ] **Step 1: Write the failing registration test**

Create `test/pgflow/migrations/versions/v05_test.exs`:

```elixir
defmodule PgFlow.Migrations.Versions.V05Test do
  use ExUnit.Case, async: true

  @up_path "priv/pgflow_helpers/sql/versions/v05/v05_up.sql"
  @down_path "priv/pgflow_helpers/sql/versions/v05/v05_down.sql"

  describe "registration" do
    test "v05 is the current helpers version" do
      assert PgFlow.HelpersMigration.current_version() == 5
    end
  end

  describe "up SQL defines required objects" do
    test "widens valid_status and creates task_signals + functions" do
      up = File.read!(@up_path)
      assert up =~ "waiting"
      assert up =~ "task_signals"
      assert up =~ "park_waiting_task"
      assert up =~ "signal_task"
      assert up =~ "consume_task_signal"
      assert up =~ "expire_waiting_tasks"
      assert File.exists?(@down_path)
    end
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test test/pgflow/migrations/versions/v05_test.exs`
Expected: FAIL — version still 4 and/or files missing.

- [ ] **Step 3: Implement V05 SQL and version module**

`lib/pgflow/migrations/versions/v05.ex` (mirror `v04.ex`):

```elixir
defmodule PgFlow.Migrations.Versions.V05 do
  @moduledoc """
  Adds awaiting-signals: `waiting` task status, `task_signals` store, park/signal/consume/expire.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "05",
    sql_path: "pgflow_helpers/sql/versions"
end
```

Register in `helpers_migration.ex` `versions:` list after `V04`.

`v05_up.sql` must (use `$SCHEMA$` like other helpers files; split statements with `--SPLIT--` if required by the splitter — follow v03/v04 conventions in-tree):

1. Drop/re-add `pgflow.step_tasks` constraint `valid_status` to include `'waiting'`.
2. Create table:

```sql
CREATE TABLE IF NOT EXISTS $SCHEMA$.task_signals (
  run_id uuid NOT NULL,
  step_slug text NOT NULL,
  task_index integer NOT NULL DEFAULT 0,
  payload jsonb NULL,
  wait_deadline_at timestamptz NULL,
  timed_out boolean NOT NULL DEFAULT false,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, step_slug, task_index)
);
```

3. `park_waiting_task`: in one function body:
   - Lock `step_tasks` where status = `'started'`
   - `pgmq.archive(flow_slug, message_id)` when `message_id` present
   - `UPDATE` status=`'waiting'`, `started_at=NULL`, `message_id=NULL`, `attempts_count = GREATEST(attempts_count - 1, 0)`
   - Upsert `task_signals` setting `wait_deadline_at` (do not clear an existing `payload`)

4. `signal_task`:
   - Upsert payload, `timed_out=false`, `updated_at=now()`
   - If task status = `'waiting'`: `pgmq.send(flow_slug, jsonb_build_object('flow_slug',...,'run_id',...,'step_slug',...,'task_index',...))`, set status=`'queued'`, `message_id`=new id, `queued_at=now()`
   - No-op when task missing or terminal (`completed`/`failed`) — still upsert buffer only if you want early signals before the task row exists; **v1 rule:** if no `step_tasks` row yet, still upsert `task_signals` so early signals work; only requeue when status is `waiting`

5. `consume_task_signal`:
   - If row has `timed_out=true`, delete row, return `(NULL, true)`
   - Else if `payload IS NOT NULL`, delete row, return `(payload, false)`
   - Else return no rows

6. `expire_waiting_tasks`:
   - Find `waiting` tasks joined to `task_signals` where `wait_deadline_at < now()`
   - Set `timed_out=true`, requeue like `signal_task` (send + queued) **without** clearing timed_out
   - Return count

`v05_down.sql`: drop functions, drop `task_signals`, restore `valid_status` without `waiting`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mix test test/pgflow/migrations/versions/v05_test.exs`
Expected: PASS (file-level). Then with DB up:

```bash
mix ecto.drop && mix ecto.create
mix test test/pgflow/migrations/versions/v05_test.exs
```

If helpers apply on test boot via `test_helper.exs`, also run a one-off query test in Task 2.

- [ ] **Step 5: Commit**

```bash
git add priv/pgflow_helpers/sql/versions/v05 lib/pgflow/migrations/versions/v05.ex \
  lib/pgflow/helpers_migration.ex test/pgflow/migrations/versions/v05_test.exs
git commit -m "feat: Add helpers V05 for awaiting signals schema"
```

---

### Task 2: `PgFlow.Queries.Signals` wrappers

**Files:**
- Create: `lib/pgflow/queries/signals.ex`
- Test: `test/pgflow/queries/signals_test.exs`

**Interfaces:**
- Consumes: V05 SQL functions
- Produces:
  - `park_waiting_task(repo, run_id, step_slug, task_index, wait_deadline_at :: DateTime.t() | nil) :: :ok | {:error, term()}`
  - `signal_task(repo, run_id, step_slug, task_index, payload :: map() | list()) :: :ok | {:error, term()}`
  - `consume_task_signal(repo, run_id, step_slug, task_index) :: {:ok, map() | list()} | {:error, :timeout} | :empty | {:error, term()}`
  - `expire_waiting_tasks(repo) :: {:ok, non_neg_integer()} | {:error, term()}`

- [ ] **Step 1: Write failing integration tests**

Use `PgFlow.DataCase` or the same DB setup as `queries_test.exs` / `IntegrationCase`. Minimal flow:

```elixir
defmodule PgFlow.Queries.SignalsTest do
  use PgFlow.IntegrationCase, async: false

  alias PgFlow.Queries.Signals

  setup do
    # compile a one-step flow, start_flow, manually set task started if needed —
    # or drive through start_tasks after reading queue. Prefer:
    # 1) create flow+step via IntegrationCase helpers
    # 2) start_flow
    # 3) read+start_tasks so status is started
    :ok
  end

  test "signal before await buffers; consume returns payload" do
    # after start_flow, before worker:
    assert :ok = Signals.signal_task(repo(), run_id, "step", 0, %{"decision" => "approved"})
    assert {:ok, %{"decision" => "approved"}} = Signals.consume_task_signal(repo(), run_id, "step", 0)
    assert :empty = Signals.consume_task_signal(repo(), run_id, "step", 0)
  end

  test "park then signal requeues task as queued" do
    # given started task:
    assert :ok = Signals.park_waiting_task(repo(), run_id, "step", 0, nil)
    assert get_task_status(run_id, "step", 0) == "waiting"
    assert :ok = Signals.signal_task(repo(), run_id, "step", 0, %{"ok" => true})
    assert get_task_status(run_id, "step", 0) == "queued"
  end

  test "expire marks timeout and requeues" do
    deadline = DateTime.add(DateTime.utc_now(), -60, :second)
    assert :ok = Signals.park_waiting_task(repo(), run_id, "step", 0, deadline)
    assert {:ok, n} = Signals.expire_waiting_tasks(repo())
    assert n >= 1
    assert {:error, :timeout} = Signals.consume_task_signal(repo(), run_id, "step", 0)
  end
end
```

Copy `compile_flow/1`, `start_flow_run/2`, and repo access from `test/pgflow/worker/server_test.exs`. Read status with `IntegrationCase.get_task_details(run_id, step_slug).status` (or the local `get_task_details/3` helper in that file). To reach `started` without a full worker: `Flows.read/4` then `Flows.start_tasks/4` with a known `worker_id` UUID, same two-phase protocol as `Server`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test test/pgflow/queries/signals_test.exs`
Expected: FAIL — `Queries.Signals` undefined.

- [ ] **Step 3: Implement `lib/pgflow/queries/signals.ex`**

```elixir
defmodule PgFlow.Queries.Signals do
  @moduledoc false
  alias Ecto.Adapters.SQL

  def park_waiting_task(repo, run_id, step_slug, task_index, wait_deadline_at) do
    sql = "SELECT pgflow.park_waiting_task($1, $2, $3, $4)"
    case SQL.query(repo, sql, [uuid(run_id), step_slug, task_index, wait_deadline_at]) do
      {:ok, _} -> :ok
      {:error, err} -> {:error, err}
    end
  end

  def signal_task(repo, run_id, step_slug, task_index, payload) do
    sql = "SELECT pgflow.signal_task($1, $2, $3, $4::jsonb)"
    case SQL.query(repo, sql, [uuid(run_id), step_slug, task_index, payload]) do
      {:ok, _} -> :ok
      {:error, err} -> {:error, err}
    end
  end

  def consume_task_signal(repo, run_id, step_slug, task_index) do
    sql = "SELECT payload, timed_out FROM pgflow.consume_task_signal($1, $2, $3)"
    case SQL.query(repo, sql, [uuid(run_id), step_slug, task_index]) do
      {:ok, %{rows: []}} -> :empty
      {:ok, %{rows: [[_payload, true]]}} -> {:error, :timeout}
      {:ok, %{rows: [[payload, false]]}} when not is_nil(payload) -> {:ok, payload}
      {:ok, %{rows: [[nil, false]]}} -> :empty
      {:error, err} -> {:error, err}
    end
  end

  def expire_waiting_tasks(repo) do
    sql = "SELECT pgflow.expire_waiting_tasks()"
    case SQL.query(repo, sql, []) do
      {:ok, %{rows: [[count]]}} -> {:ok, count}
      {:error, err} -> {:error, err}
    end
  end

  defp uuid(id) when is_binary(id) do
    case Ecto.UUID.dump(id) do
      {:ok, bin} -> bin
      :error -> id
    end
  end
end
```

Match UUID handling used in `Queries.Flows` (`parse_uuid`). Prefer copying that private helper pattern exactly.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/pgflow/queries/signals_test.exs`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add lib/pgflow/queries/signals.ex test/pgflow/queries/signals_test.exs
git commit -m "feat: Add Queries.Signals for park/signal/consume/expire"
```

---

### Task 3: `Context.await_signal/2` + `PgFlow.signal/3,4`

**Files:**
- Modify: `lib/pgflow/context.ex`
- Modify: `lib/pgflow/client.ex`
- Modify: `lib/pgflow.ex`
- Test: `test/pgflow/context_await_signal_test.exs`
- Test: extend or add thin tests in `test/pgflow/client_test.exs` for `signal/3`

**Interfaces:**
- Consumes: `Queries.Signals`
- Produces:
  - `@spec await_signal(t(), keyword()) :: {:ok, map() | list()} | {:error, :timeout}`
  - Options: `:wait_for` (`:infinity` | integer seconds | `{n, :seconds|:minutes|:hours|:days}`), `:wait_timeout` (ms, default `5_000`)
  - On park: calls `park_waiting_task` then `throw {:pgflow_await, :parked}`
  - `@spec signal(String.t(), atom() | String.t(), map() | list()) :: :ok`
  - `@spec signal(String.t(), atom() | String.t(), non_neg_integer(), map() | list()) :: :ok`

- [ ] **Step 1: Write failing tests**

```elixir
defmodule PgFlow.ContextAwaitSignalTest do
  use PgFlow.IntegrationCase, async: false

  alias PgFlow.Context

  test "returns buffered payload without parking" do
    # start_flow; Signals.signal_task(...); build Context with repo/run/step/index
    ctx = context_for(run_id, :approval)
    assert {:ok, %{"decision" => "yes"}} =
             Context.await_signal(ctx, wait_for: {1, :hour}, wait_timeout: 0)
  end

  test "parks when no signal and wait_timeout is 0" do
    ctx = context_for(run_id, :approval)
    # Ensure task is started first
    catch_throw do
      Context.await_signal(ctx, wait_timeout: 0)
    end
    assert catch_throw(Context.await_signal(ctx, wait_timeout: 0)) == {:pgflow_await, :parked}
    assert get_task_status(run_id, "approval", 0) == "waiting"
  end
end
```

Also test `PgFlow.signal/3` delegates and last-write-wins (signal twice, consume once → last payload).

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test test/pgflow/context_await_signal_test.exs`
Expected: FAIL — `await_signal` undefined.

- [ ] **Step 3: Implement API**

In `context.ex`:

- Add optional struct fields `flow_slug: nil`, `message_id: nil` (not enforce_keys).
- Implement `await_signal/2`:
  1. Normalize opts.
  2. `case Signals.consume_task_signal(...)` → return ok/timeout.
  3. Live-wait loop: sleep 50–100ms, re-consume, until `wait_timeout` elapsed.
  4. Compute `wait_deadline_at` from `:wait_for` on first park only (SQL upsert should not overwrite an existing deadline — enforce in `park_waiting_task` SQL with `COALESCE(existing, excluded)`).
  5. `Signals.park_waiting_task(...)`; `throw {:pgflow_await, :parked}`.

In `client.ex`:

```elixir
def signal(run_id, step_slug, payload) when is_map(payload) or is_list(payload) do
  signal(run_id, step_slug, 0, payload)
end

def signal(run_id, step_slug, task_index, payload)
    when (is_map(payload) or is_list(payload)) and is_integer(task_index) do
  repo = Config.repo!() # or however Client resolves repo today — copy start_flow’s repo lookup
  slug = to_string(step_slug)
  case PgFlow.Queries.Signals.signal_task(repo, run_id, slug, task_index, payload) do
    :ok -> :ok
    {:error, _} -> :ok  # fire-and-forget no-op semantics for missing targets; log if useful
  end
end
```

**Important:** Match existing `Client` repo resolution (`Application.get_env` / `Config`). Do not invent a new config key. If `signal_task` errors on missing FK, treat as `:ok` per spec no-op **only** when the error is “no target”; prefer SQL that never raises for missing tasks.

`lib/pgflow.ex`:

```elixir
defdelegate signal(run_id, step_slug, payload), to: Client
defdelegate signal(run_id, step_slug, task_index, payload), to: Client
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/pgflow/context_await_signal_test.exs test/pgflow/client_test.exs`
Expected: PASS for new cases; no regressions in client_test.

- [ ] **Step 5: Commit**

```bash
git add lib/pgflow/context.ex lib/pgflow/client.ex lib/pgflow.ex \
  test/pgflow/context_await_signal_test.exs test/pgflow/client_test.exs
git commit -m "feat: Add Context.await_signal and PgFlow.signal API"
```

---

### Task 4: Worker catches park without failing the task

**Files:**
- Modify: `lib/pgflow/worker/server.ex` (`dispatch_task/2` handler wrapper + result handling)
- Test: `test/pgflow/await_signals_test.exs` (first e2e cases) and/or extend `test/pgflow/worker/server_test.exs`

**Interfaces:**
- Consumes: `{:pgflow_await, :parked}` throw from handler; `Context` with `flow_slug` + `message_id`
- Produces: on park, worker removes `active_tasks` entry, cancels timeout timer, does **not** call `fail_task` / `complete_task`, does **not** `delete_message` (already archived in park SQL)

- [ ] **Step 1: Write failing e2e test**

```elixir
defmodule PgFlow.AwaitSignalsTest do
  use PgFlow.IntegrationCase, async: false

  defmodule ApprovalFlow do
    use PgFlow.Flow
    @flow slug: :await_approval_flow, max_attempts: 3, timeout: 30

    step :approval do
      fn input, ctx ->
        case PgFlow.Context.await_signal(ctx, wait_for: {1, :hour}, wait_timeout: 0) do
          {:ok, %{"decision" => "approved"}} -> Map.put(input, "charged", true)
          {:ok, _} -> raise "rejected"
          {:error, :timeout} -> raise "no decision"
        end
      end
    end
  end

  test "parks then resumes when signalled" do
    compile_flow(ApprovalFlow)
    worker = start_worker(ApprovalFlow)
    run_id = start_flow_run("await_approval_flow", %{"order_id" => 1})

    wait_until(fn -> get_task_status(run_id, "approval", 0) == "waiting" end)

    assert :ok = PgFlow.signal(run_id, :approval, %{"decision" => "approved"})

    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "completed"
  end
end
```

Copy `compile_flow` / `start_worker` / `wait_for_run_completion` from `server_test.exs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test test/pgflow/await_signals_test.exs:line_of_parks_test`
Expected: FAIL — park currently treated as error (`catch` formats throw as failure) or run fails.

- [ ] **Step 3: Wire Server**

In `dispatch_task/2` when building `%Context{}`, set:

```elixir
context = %Context{
  run_id: run_id,
  step_slug: step_slug_atom,
  task_index: task_index,
  attempt: attempt,
  repo: state.repo,
  flow_input: flow_input || :not_loaded,
  flow_slug: state.flow_slug,
  message_id: msg_id
}
```

Change the Task try/catch:

```elixir
try do
  result = handler.(handler_input, context)
  {:ok, result}
catch
  :throw, {:pgflow_await, :parked} ->
    {:await_parked}

  kind, reason ->
    {:error, Exception.format(kind, reason, __STACKTRACE__)}
end
```

In `handle_info({ref, result}, ...)` (success path ~L325), branch:

```elixir
{:await_parked} ->
  cancel_task_timeout(task_meta)
  # slot freed; message already archived by park_waiting_task
  %{state | active_tasks: Map.delete(state.active_tasks, ref)}
  |> maybe_poll_immediately()
```

Keep existing `{:ok, output}` → `handle_task_success` and error → `handle_task_failure`.

Emit a quiet log or telemetry `[:pgflow, :task, :waiting]` if cheap; optional in v1.

- [ ] **Step 4: Run test to verify it passes**

Run: `mix test test/pgflow/await_signals_test.exs`
Expected: PASS for park+resume.

- [ ] **Step 5: Commit**

```bash
git add lib/pgflow/worker/server.ex lib/pgflow/context.ex test/pgflow/await_signals_test.exs
git commit -m "feat: Park awaiting tasks without failing the worker attempt"
```

---

### Task 5: Timeout sweeper (`WaitingTaskRecovery`)

**Files:**
- Create: `lib/pgflow/worker/waiting_task_recovery.ex`
- Modify: `lib/pgflow/config.ex` — add `:waiting_recovery_interval` (default `15_000`)
- Modify: `lib/pgflow/supervisor.ex` — start child
- Test: extend `test/pgflow/await_signals_test.exs`

**Interfaces:**
- Consumes: `Queries.Signals.expire_waiting_tasks/1`
- Produces: GenServer on interval calling expire; after requeue, handler’s `await_signal` returns `{:error, :timeout}`

- [ ] **Step 1: Write failing timeout test**

```elixir
test "wait_for deadline yields {:error, :timeout} and handler can fail the run" do
  defmodule TimeoutFlow do
    use PgFlow.Flow
    @flow slug: :await_timeout_flow, max_attempts: 1, timeout: 30

    step :gate do
      fn _input, ctx ->
        case PgFlow.Context.await_signal(ctx, wait_for: 1, wait_timeout: 0) do
          {:ok, _} -> %{"ok" => true}
          {:error, :timeout} -> raise "no decision"
        end
      end
    end
  end

  compile_flow(TimeoutFlow)
  start_worker(TimeoutFlow)
  run_id = start_flow_run("await_timeout_flow", %{})
  wait_until(fn -> get_task_status(run_id, "gate", 0) == "waiting" end)

  # Force expiry without waiting wall clock:
  repo().query!("UPDATE pgflow.task_signals SET wait_deadline_at = now() - interval '1 second'")
  assert {:ok, _} = PgFlow.Queries.Signals.expire_waiting_tasks(repo())

  {:ok, status} = wait_for_run_completion(run_id)
  assert status == "failed"
end
```

- [ ] **Step 2: Run test to verify failure mode**

If expire already works from Task 2, this may pass without the GenServer — still implement the GenServer and assert it is supervised. Add:

```elixir
test "WaitingTaskRecovery is started under PgFlow.Supervisor" do
  assert Process.whereis(PgFlow.Worker.WaitingTaskRecovery)
end
```

(Only valid when tests start the full supervisor — if not, unit-test `init/1` schedules `:recover` like `stalled_task_recovery_test.exs`.)

- [ ] **Step 3: Implement sweeper**

Copy structure from `stalled_task_recovery.ex`:

```elixir
defmodule PgFlow.Worker.WaitingTaskRecovery do
  use GenServer
  alias PgFlow.Queries.Signals

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  def init(config) do
    repo = Keyword.fetch!(config, :repo)
    interval = Keyword.get(config, :waiting_recovery_interval, 15_000)
    state = %{repo: repo, interval: interval}
    Process.send_after(self(), :recover, interval)
    {:ok, state}
  end

  def handle_info(:recover, state) do
    _ = Signals.expire_waiting_tasks(state.repo)
    Process.send_after(self(), :recover, state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
```

Wire config validation + supervisor child next to `StalledTaskRecovery`.

- [ ] **Step 4: Run tests**

Run: `mix test test/pgflow/await_signals_test.exs test/pgflow/worker/stalled_task_recovery_test.exs`
Expected: PASS; stalled recovery still ignores `waiting`.

- [ ] **Step 5: Commit**

```bash
git add lib/pgflow/worker/waiting_task_recovery.ex lib/pgflow/config.ex \
  lib/pgflow/supervisor.ex test/pgflow/await_signals_test.exs
git commit -m "feat: Sweep expired waiting tasks for await_signal timeouts"
```

---

### Task 6: Job coverage, races, docs

**Files:**
- Modify: `test/pgflow/await_signals_test.exs` (Job + race cases)
- Modify: `docs/ARCHITECTURE.md`
- Modify: `README.md` (short feature blurb + example)
- Modify: `lib/pgflow.ex` moduledoc if it lists features

**Interfaces:**
- Consumes: completed Tasks 1–5
- Produces: Job path proven; early-signal and last-write-wins covered; docs distinguish await-signals from `signal_strategy`

- [ ] **Step 1: Write failing Job + race tests**

```elixir
defmodule ApprovalJob do
  use PgFlow.Job
  @job queue: :await_approval_job, max_attempts: 2, timeout: 30

  perform :approve do
    fn _input, ctx ->
      case PgFlow.Context.await_signal(ctx, wait_timeout: 0, wait_for: {1, :hour}) do
        {:ok, %{"decision" => "approved"}} -> %{"done" => true}
        other -> raise "unexpected #{inspect(other)}"
      end
    end
  end
end

test "job parks and resumes" do
  # compile job migration path used in job tests — follow job_test.exs / server patterns
end

test "early signal before handler runs" do
  compile_flow(ApprovalFlow)
  run_id = start_flow_run("await_approval_flow", %{})
  assert :ok = PgFlow.signal(run_id, :approval, %{"decision" => "approved"})
  start_worker(ApprovalFlow)
  {:ok, "completed"} = wait_for_run_completion(run_id)
end

test "last write wins" do
  run_id = start_flow_run(...)
  PgFlow.signal(run_id, :approval, %{"decision" => "rejected"})
  PgFlow.signal(run_id, :approval, %{"decision" => "approved"})
  start_worker(...)
  # completes with approved
end
```

- [ ] **Step 2: Run tests to verify gaps**

Run: `mix test test/pgflow/await_signals_test.exs`
Expected: FAIL on any not-yet-handled Job compile/start differences.

- [ ] **Step 3: Fix Job path + docs**

Jobs are single-step flows — same await/signal. Ensure job slug/step slug used in `signal/3` matches perform name (`:approve` above).

Docs paragraph (ARCHITECTURE):

```markdown
## Awaiting signals

Handlers may call `PgFlow.Context.await_signal/2` to park a task as `waiting`
until `PgFlow.signal/3` delivers a JSON payload. This is unrelated to
`signal_strategy: :notify`, which only wakes workers on pgmq inserts.
```

README: short example mirroring the spec.

- [ ] **Step 4: Full verification**

Run:

```bash
mix test test/pgflow/await_signals_test.exs \
  test/pgflow/queries/signals_test.exs \
  test/pgflow/context_await_signal_test.exs \
  test/pgflow/migrations/versions/v05_test.exs \
  test/pgflow/worker/server_test.exs \
  test/pgflow/worker/stalled_task_recovery_test.exs
mix format
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add test/pgflow/await_signals_test.exs docs/ARCHITECTURE.md README.md lib/pgflow.ex
git commit -m "feat: Cover Job await-signals and document the feature"
```

---

## Spec coverage checklist

| Spec requirement | Task |
|---|---|
| `Context.await_signal/2` | 3 |
| `PgFlow.signal/3,4` | 3 |
| `task_signals` table + `timed_out` | 1 |
| `waiting` status + CHECK | 1 |
| Live wait then park | 3–4 |
| Early buffer / last write wins | 2, 6 |
| Re-enter handler from top | 4 (natural via requeue + start_tasks) |
| Attempt not consumed by park | 1 (`attempts_count` decrement) |
| Stalled recovery ignores `waiting` | 1, 5 (no predicate change) |
| Timeout → `{:error, :timeout}` | 2, 5 |
| Jobs + Flows | 4, 6 |
| Docs vs `signal_strategy` | 6 |
| No dashboard / no term encoding / no cancel | Global constraints (omitted on purpose) |

## Placeholder / consistency notes

- UUID param encoding must match `Queries.Flows.parse_uuid/1` — copy, do not invent a third variant.
- `pgmq.send` payload shape must stay `{flow_slug, run_id, step_slug, task_index}` as in core `start_ready_steps`.
- Internal park token is exactly `{:pgflow_await, :parked}` (throw) everywhere.
- Repo resolution for `PgFlow.signal` must match `Client.start_flow` / `Config`.
