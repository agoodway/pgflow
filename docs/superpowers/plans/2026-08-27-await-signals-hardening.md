# Await-Signals Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make PR #6's await-signals feature safe under concurrency, retries, terminal transitions, upgrades, rollback, and delivery failures while preserving durable early buffering and worker-slot release.

**Architecture:** Replace the split consume/park protocol with one PostgreSQL-owned, attempt-fenced transition that returns typed outcomes. Retain accepted signal outcomes until the task becomes terminal, compute and enforce deadlines in PostgreSQL, and integrate `waiting` into task/run cleanup. Expose honest Elixir return values, fail closed on incompatible helper versions, and keep the demo and public documentation aligned with the resulting contract.

**Tech Stack:** Elixir 1.19 / OTP 28, Ecto and EctoEvolver, PostgreSQL / pgmq, Phoenix LiveView, ExUnit integration tests.

**Source design:** `docs/superpowers/specs/2026-08-21-await-signals-design.md`

## Global Constraints

- Work from PR head `82dd00e4d9d27a77ee7a5ac4506a3cb29b50330a` or re-review the diff before execution if the head changes.
- PostgreSQL remains the source of truth for park, signal, expiry, retry replay, and terminal cleanup.
- Every SQL path locks in the order `step_tasks` first, then `task_signals`; do not wrap the two current RPCs in an Elixir transaction as a substitute.
- Fence handler-owned operations with both `Context.attempt` and `Context.message_id`; `message_id` alone is not unique across retries or stalled recovery.
- One outstanding `await_signal/2` per task is the V1 contract. Multiple sequential awaits remain out of scope.
- A handler resumes from the top. Code before `await_signal/2` must be idempotent.
- Retain the accepted payload or timeout through ordinary retries; delete it only when the task/step/run becomes terminal.
- Early signals are valid only for an existing active run and a step state in `created` or `started`.
- Requeue only when the run and step state are both `started` and the task is `waiting`.
- JSON payloads are objects or arrays only. SQL `NULL`, JSON `null`, scalars, and unbounded direct-RPC input are rejected.
- PostgreSQL computes persisted deadlines from `now()`; application-node wall clocks do not define expiry.
- Awaiting inside a caller-owned `Repo.transaction/1` is rejected before reading or parking.
- Keep `recover_stalled_tasks` restricted to `status = 'started'`; waiting expiry remains in `WaitingTaskRecovery`.
- V05 rollback must refuse to start when a wait or buffered signal exists. It must not synthesize queue messages or discard active signal state.
- Existing consumers require a new Ecto wrapper migration. Rolling that wrapper back targets helper V04, not helper version zero.
- Prefer `SECURITY INVOKER` for the new functions. Do not grant signal functions to `PUBLIC` through a definer function.
- Do not alter Inbox, another application's database, or any running PostgreSQL service. Database gates run only in CI or against the dedicated `PgFlow.TestRepo` at `localhost:54323` / `pgflow_test` after verifying that target explicitly.
- Do not start, stop, restart, remove, or reconfigure containers or database services during execution without separate authorization.
- Do not create a commit unless Chase explicitly authorizes that specific commit during execution. Each task ends with a review checkpoint instead.
- Do not add AI attribution to code, documentation, commits, or the PR.

---

## File Map

| File | Responsibility |
|---|---|
| `priv/pgflow_helpers/sql/versions/v05/v05_up.sql` | Signal table constraints, atomic await/signal/expiry functions, lifecycle cleanup, indexes |
| `priv/pgflow_helpers/sql/versions/v05/v05_down.sql` | Lossless preflight and safe V05-to-V04 rollback |
| `lib/pgflow/queries/signals.ex` | Typed wrappers for await, signal, expiry, and waiting-task discovery |
| `lib/pgflow/context.ex` | Option validation, transaction guard, live wait, fenced final park |
| `lib/pgflow/await_signal_transaction_error.ex` | Descriptive exception for unsupported caller transactions |
| `lib/pgflow/client.ex`, `lib/pgflow.ex` | Honest signal results and public waiting-task query |
| `lib/pgflow/worker/server.ex` | Control outcomes, graceful shutdown, no-op handling for stale/terminal attempts |
| `lib/pgflow/worker/waiting_task_recovery.ex` | Bounded deadline sweeps |
| `lib/pgflow/config.ex` | Waiting recovery batch size validation |
| `lib/pgflow/schema_compatibility.ex` | Runtime and Mix-task helper-version/object checks |
| `lib/pgflow/supervisor.ex` | Fail-closed compatibility check before workers start |
| `lib/mix/tasks/pgflow.check_schema.ex` | V05-aware operator check |
| `lib/mix/tasks/pgflow.gen.helpers_migration.ex` | Initial-install and version-aware consumer wrapper generator |
| `test/pgflow/queries/signals_test.exs` | SQL transition, ownership, lifecycle, deadline tests |
| `test/pgflow/context_await_signal_test.exs` | Context validation, transaction, consume-or-park tests |
| `test/pgflow/await_signals_test.exs` | Worker retry, map, shutdown, and full recovery tests |
| `test/pgflow/migrations/helpers_upgrade_test.exs` | Real V04-to-V05-to-V04 migration tests |
| `test/mix/tasks/pgflow.gen.helpers_migration_test.exs` | Initial-install preservation, upgrade content, validation, and rollback-target tests |
| `test/support/db/test_helpers.sql` | Signal-aware test reset |
| `demo/lib/pgflow_demo/flows/approval_flow.ex` | Correct linear approval data flow |
| `demo/lib/pgflow_demo_web/live/flow_demo_live.ex` | Run-scoped events and signal delivery feedback |
| `demo/test/pgflow_demo/approval_flow_test.exs` | Approval handler output contract |
| `demo/test/pgflow_demo_web/live/flow_demo_live_test.exs` | Stale-event and approval-control behavior |
| `README.md`, `docs/ARCHITECTURE.md`, `docs/ELIXIR_VS_SUPABASE.md` | Public execution, compatibility, security, and upgrade contract |

---

### Task 1: Replace Split Consume/Park With One Attempt-Fenced SQL Transition

**Files:**
- Modify: `priv/pgflow_helpers/sql/versions/v05/v05_up.sql`
- Modify: `lib/pgflow/queries/signals.ex`
- Test: `test/pgflow/queries/signals_test.exs`

**Interfaces:**
- Consumes: `{run_id, step_slug, task_index, expected_attempt, expected_message_id}` from the dispatched `Context`
- Produces:
  - `pgflow.await_task_signal(uuid, text, integer, integer, bigint, bigint, boolean)` returning `(outcome text, payload jsonb)`
  - `PgFlow.Queries.Signals.await_task_signal/8`
  - Elixir outcomes `{:ok, payload} | :empty | :parked | :timeout | :stale | :terminal | :missing | {:error, term()}`

- [ ] **Step 1: Replace the happy-path-only query tests with counterfactual ownership tests**

Add these tests to `test/pgflow/queries/signals_test.exs`, reusing its `compile_one_step_flow/2` and `start_started_task/2` helpers:

```elixir
test "a signal committed before the final park is returned without parking" do
  compile_one_step_flow("signal_won_before_park", "approval")
  run_id = start_started_task("signal_won_before_park", %{})
  task = get_task_details(run_id, "approval", 0)

  assert {:ok, :buffered} =
           Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "approved"})

  assert {:ok, %{"decision" => "approved"}} =
           Signals.await_task_signal(
             repo(),
             run_id,
             "approval",
             0,
             task.attempts_count,
             task.message_id,
             nil,
             true
           )

  assert get_task_details(run_id, "approval", 0).status == "started"
end

test "a stale attempt cannot consume or park the current attempt" do
  compile_one_step_flow("stale_attempt_fence", "approval")
  run_id = start_started_task("stale_attempt_fence", %{})
  attempt_one = get_task_details(run_id, "approval", 0)

  TestRepo.query!(
    "UPDATE pgflow.step_tasks SET attempts_count = attempts_count + 1 WHERE run_id = $1 AND step_slug = $2",
    [Ecto.UUID.dump!(run_id), "approval"]
  )

  assert :stale =
           Signals.await_task_signal(
             repo(),
             run_id,
             "approval",
             0,
             attempt_one.attempts_count,
             attempt_one.message_id,
             nil,
             true
           )

  assert get_task_details(run_id, "approval", 0).status == "started"
end
```

Extend the test helper query so `get_task_details/3` selects `message_id` as well as status and attempts.

- [ ] **Step 2: Run the focused tests and confirm the old protocol fails**

Run only against the isolated PgFlow test database:

```bash
PGFLOW_REQUIRE_DB=1 mix test test/pgflow/queries/signals_test.exs
```

Expected before implementation: FAIL because `await_task_signal/8` and typed `signal_task/5` outcomes do not exist.

- [ ] **Step 3: Widen the signal row contract without deleting accepted outcomes**

In `v05_up.sql`, define `task_signals` with these additional constraints/fields:

```sql
CREATE TABLE IF NOT EXISTS $SCHEMA$.task_signals (
  run_id uuid NOT NULL,
  step_slug text NOT NULL,
  task_index integer NOT NULL DEFAULT 0,
  payload jsonb NULL,
  wait_deadline_at timestamptz NULL,
  timed_out boolean NOT NULL DEFAULT false,
  claimed_at timestamptz NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, step_slug, task_index),
  CONSTRAINT task_signals_task_index_nonnegative CHECK (task_index >= 0),
  CONSTRAINT task_signals_payload_shape CHECK (
    payload IS NULL OR jsonb_typeof(payload) IN ('object', 'array')
  ),
  CONSTRAINT task_signals_step_state_fkey
    FOREIGN KEY (run_id, step_slug)
    REFERENCES $SCHEMA$.step_states(run_id, step_slug)
    ON DELETE CASCADE
);
```

Do not keep `consume_task_signal` as a destructive `DELETE ... RETURNING` function.

- [ ] **Step 4: Implement the atomic await transition**

Add this signature and preserve the task-then-signal lock order:

```sql
CREATE OR REPLACE FUNCTION $SCHEMA$.await_task_signal(
  p_run_id uuid,
  p_step_slug text,
  p_task_index integer,
  p_expected_attempt integer,
  p_expected_message_id bigint,
  p_wait_for_seconds bigint,
  p_park boolean
)
RETURNS TABLE(outcome text, payload jsonb)
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  v_task pgflow.step_tasks%ROWTYPE;
  v_signal pgflow.task_signals%ROWTYPE;
  v_deadline timestamptz;
BEGIN
  SELECT * INTO v_task
  FROM pgflow.step_tasks
  WHERE run_id = p_run_id
    AND step_slug = p_step_slug
    AND task_index = p_task_index
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN QUERY SELECT 'missing'::text, NULL::jsonb;
    RETURN;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pgflow.runs r
    JOIN pgflow.step_states ss
      ON ss.run_id = r.run_id
     AND ss.step_slug = p_step_slug
    WHERE r.run_id = p_run_id
      AND r.status = 'started'
      AND ss.status = 'started'
  ) THEN
    RETURN QUERY SELECT 'terminal'::text, NULL::jsonb;
    RETURN;
  END IF;

  IF v_task.status <> 'started'
     OR v_task.attempts_count <> p_expected_attempt
     OR v_task.message_id IS DISTINCT FROM p_expected_message_id THEN
    RETURN QUERY SELECT 'stale'::text, NULL::jsonb;
    RETURN;
  END IF;

  SELECT * INTO v_signal
  FROM pgflow.task_signals
  WHERE run_id = p_run_id
    AND step_slug = p_step_slug
    AND task_index = p_task_index
  FOR UPDATE;

  IF FOUND AND v_signal.timed_out THEN
    UPDATE pgflow.task_signals
    SET claimed_at = COALESCE(claimed_at, now()), updated_at = now()
    WHERE run_id = p_run_id AND step_slug = p_step_slug AND task_index = p_task_index;
    RETURN QUERY SELECT 'timeout'::text, NULL::jsonb;
    RETURN;
  END IF;

  IF FOUND AND v_signal.payload IS NOT NULL THEN
    UPDATE pgflow.task_signals
    SET claimed_at = COALESCE(claimed_at, now()), updated_at = now()
    WHERE run_id = p_run_id AND step_slug = p_step_slug AND task_index = p_task_index;
    RETURN QUERY SELECT 'signal'::text, v_signal.payload;
    RETURN;
  END IF;

  IF FOUND AND v_signal.wait_deadline_at IS NOT NULL
           AND v_signal.wait_deadline_at <= now() THEN
    UPDATE pgflow.task_signals
    SET timed_out = true,
        claimed_at = COALESCE(claimed_at, now()),
        updated_at = now()
    WHERE run_id = p_run_id AND step_slug = p_step_slug AND task_index = p_task_index;
    RETURN QUERY SELECT 'timeout'::text, NULL::jsonb;
    RETURN;
  END IF;

  IF NOT p_park THEN
    RETURN QUERY SELECT 'empty'::text, NULL::jsonb;
    RETURN;
  END IF;

  v_deadline := CASE
    WHEN p_wait_for_seconds IS NULL THEN NULL
    ELSE now() + make_interval(secs => p_wait_for_seconds)
  END;

  INSERT INTO pgflow.task_signals (run_id, step_slug, task_index, wait_deadline_at)
  VALUES (p_run_id, p_step_slug, p_task_index, v_deadline)
  ON CONFLICT (run_id, step_slug, task_index) DO UPDATE
    SET wait_deadline_at = COALESCE(pgflow.task_signals.wait_deadline_at, EXCLUDED.wait_deadline_at),
        updated_at = now();

  IF v_task.message_id IS NOT NULL THEN
    PERFORM pgmq.archive(v_task.flow_slug, v_task.message_id);
  END IF;

  UPDATE pgflow.step_tasks
  SET status = 'waiting',
      started_at = NULL,
      message_id = NULL,
      attempts_count = GREATEST(attempts_count - 1, 0)
  WHERE run_id = p_run_id
    AND step_slug = p_step_slug
    AND task_index = p_task_index
    AND status = 'started'
    AND attempts_count = p_expected_attempt
    AND message_id IS NOT DISTINCT FROM p_expected_message_id;

  IF NOT FOUND THEN
    RETURN QUERY SELECT 'stale'::text, NULL::jsonb;
  ELSE
    RETURN QUERY SELECT 'parked'::text, NULL::jsonb;
  END IF;
END;
$$;
```

- [ ] **Step 5: Replace the Elixir wrapper with typed decoding**

In `lib/pgflow/queries/signals.ex`:

```elixir
@type await_outcome ::
        {:ok, map() | list()}
        | :empty
        | :parked
        | :timeout
        | :stale
        | :terminal
        | :missing
        | {:error, term()}

@spec await_task_signal(
        module(), String.t(), String.t(), non_neg_integer(), pos_integer(), integer(),
        pos_integer() | nil, boolean()
      ) :: await_outcome()
def await_task_signal(
      repo,
      run_id,
      step_slug,
      task_index,
      expected_attempt,
      expected_message_id,
      wait_for_seconds,
      park?
    ) do
  sql = """
  SELECT outcome, payload
  FROM pgflow.await_task_signal($1, $2, $3, $4, $5, $6, $7)
  """

  params = [
    parse_uuid(run_id),
    step_slug,
    task_index,
    expected_attempt,
    expected_message_id,
    wait_for_seconds,
    park?
  ]

  case SQL.query(repo, sql, params) do
    {:ok, %{rows: [["signal", payload]]}} -> {:ok, payload}
    {:ok, %{rows: [["empty", nil]]}} -> :empty
    {:ok, %{rows: [["parked", nil]]}} -> :parked
    {:ok, %{rows: [["timeout", nil]]}} -> :timeout
    {:ok, %{rows: [["stale", nil]]}} -> :stale
    {:ok, %{rows: [["terminal", nil]]}} -> :terminal
    {:ok, %{rows: [["missing", nil]]}} -> :missing
    {:error, reason} -> {:error, reason}
  end
end
```

- [ ] **Step 6: Run the focused SQL tests**

Run:

```bash
PGFLOW_REQUIRE_DB=1 mix test test/pgflow/queries/signals_test.exs
```

Expected: PASS with exact assertions for signal-wins, park-wins, stale ownership, and unchanged current task state.

- [ ] **Step 7: Review checkpoint**

Run `git diff --check` and inspect only the files in this task. Do not commit without separate authorization.

---

### Task 2: Wire Context and Worker Control Flow to Database-Confirmed Outcomes

**Files:**
- Create: `lib/pgflow/await_signal_transaction_error.ex`
- Modify: `lib/pgflow/context.ex`
- Modify: `lib/pgflow/worker/server.ex`
- Test: `test/pgflow/context_await_signal_test.exs`
- Test: `test/pgflow/worker/server_test.exs`

**Interfaces:**
- Consumes: `Signals.await_task_signal/8` from Task 1
- Produces: validated `await_signal/2`, transaction rejection, and worker control results `{:await_control, :parked | :stale | :terminal}`

- [ ] **Step 1: Write the failing transaction and validation tests**

Add to `test/pgflow/context_await_signal_test.exs`:

```elixir
test "rejects await_signal inside a caller-owned transaction" do
  compile_one_step_flow("await_transaction_guard", "approval")
  run_id = start_started_task("await_transaction_guard", %{})
  ctx = context_for(run_id, :approval)

  assert_raise PgFlow.AwaitSignalTransactionError, fn ->
    TestRepo.transaction(fn -> Context.await_signal(ctx, wait_timeout: 0) end)
  end

  assert get_task_details(run_id, "approval", 0).status == "started"
end

test "validates wait options before touching the database" do
  ctx = context_for(Ecto.UUID.generate(), :approval)

  assert_raise ArgumentError, ~r/wait_timeout must be a non-negative integer/, fn ->
    Context.await_signal(ctx, wait_timeout: -1)
  end

  assert_raise ArgumentError, ~r/wait_for must be :infinity or a positive duration/, fn ->
    Context.await_signal(ctx, wait_for: {1, :week})
  end
end
```

Update this test module's context helper so every handler-owned await carries the database dispatch identity:

```elixir
defp context_for(run_id, step_slug) do
  task = get_task_details(run_id, to_string(step_slug), 0)

  Context.new(
    run_id: run_id,
    step_slug: step_slug,
    task_index: 0,
    attempt: task.attempts_count,
    message_id: task.message_id,
    repo: TestRepo
  )
end
```

Change the buffered-payload test to call `start_started_task/2` before constructing the context. The public handler contract always has a started task and dispatch identity; do not preserve the old synthetic context with no task row.

- [ ] **Step 2: Run the focused Context tests and verify failure**

Run:

```bash
PGFLOW_REQUIRE_DB=1 mix test test/pgflow/context_await_signal_test.exs
```

Expected before implementation: FAIL because the exception and validation contract do not exist.

- [ ] **Step 3: Add the transaction exception**

Create `lib/pgflow/await_signal_transaction_error.ex`:

```elixir
defmodule PgFlow.AwaitSignalTransactionError do
  @moduledoc """
  Raised when `PgFlow.Context.await_signal/2` is called inside a caller-owned transaction.

  Parking exits the handler after committing its own database transition. An outer
  transaction would roll that transition back while the worker believed the task parked.
  """

  defexception message:
                 "PgFlow.Context.await_signal/2 cannot run inside Repo.transaction/1; " <>
                   "move the await outside the transaction"
end
```

- [ ] **Step 4: Normalize options and call the atomic function for every poll**

Replace destructive `consume/3` plus separate `park_and_throw/4` with:

```elixir
def await_signal(%__MODULE__{} = ctx, opts \\ []) when is_list(opts) do
  ensure_not_in_transaction!(ctx.repo)
  wait_timeout = normalize_wait_timeout!(Keyword.get(opts, :wait_timeout, @default_wait_timeout_ms))
  wait_for_seconds = normalize_wait_for!(Keyword.get(opts, :wait_for, :infinity))
  step_slug = to_string(ctx.step_slug)

  case await_once(ctx, step_slug, wait_for_seconds, false) do
    :empty -> live_wait_or_park(ctx, step_slug, wait_timeout, wait_for_seconds)
    outcome -> handle_await_outcome(ctx, outcome)
  end
end

defp ensure_not_in_transaction!(repo) do
  if repo.in_transaction?(), do: raise(PgFlow.AwaitSignalTransactionError)
end

defp normalize_wait_timeout!(value) when is_integer(value) and value >= 0, do: value

defp normalize_wait_timeout!(_value),
  do: raise(ArgumentError, "wait_timeout must be a non-negative integer number of milliseconds")

defp normalize_wait_for!(:infinity), do: nil
defp normalize_wait_for!(seconds) when is_integer(seconds) and seconds > 0, do: seconds
defp normalize_wait_for!({n, unit}) when is_integer(n) and n > 0 and unit in [:second, :seconds], do: n
defp normalize_wait_for!({n, unit}) when is_integer(n) and n > 0 and unit in [:minute, :minutes], do: n * 60
defp normalize_wait_for!({n, unit}) when is_integer(n) and n > 0 and unit in [:hour, :hours], do: n * 3_600
defp normalize_wait_for!({n, unit}) when is_integer(n) and n > 0 and unit in [:day, :days], do: n * 86_400

defp normalize_wait_for!(_value),
  do: raise(ArgumentError, "wait_for must be :infinity or a positive duration in seconds, minutes, hours, or days")

defp await_once(ctx, step_slug, wait_for_seconds, park?) do
  Signals.await_task_signal(
    ctx.repo,
    ctx.run_id,
    step_slug,
    ctx.task_index,
    ctx.attempt,
    ctx.message_id,
    wait_for_seconds,
    park?
  )
end
```

The live-wait loop calls `await_once(..., false)`. Its final call uses `await_once(..., true)`; there is no Elixir gap between a last consume and parking.

- [ ] **Step 5: Convert database-confirmed control outcomes into worker messages**

Use a single private helper:

```elixir
defp handle_await_outcome(_ctx, {:ok, payload}), do: {:ok, payload}
defp handle_await_outcome(_ctx, :timeout), do: {:error, :timeout}
defp handle_await_outcome(_ctx, :empty), do: :empty

defp handle_await_outcome(ctx, outcome) when outcome in [:parked, :stale, :terminal] do
  throw({:pgflow_await, outcome, ctx.attempt, ctx.message_id})
end

defp handle_await_outcome(_ctx, :missing), do: raise("await_signal task no longer exists")
defp handle_await_outcome(_ctx, {:error, reason}), do: raise("await_signal database error: #{inspect(reason)}")
```

In the worker task catch:

```elixir
:throw, {:pgflow_await, outcome, attempt, message_id}
when outcome in [:parked, :stale, :terminal] ->
  if attempt == context.attempt and message_id == context.message_id do
    {:await_control, outcome}
  else
    {:error, "invalid await control ownership"}
  end
```

Add `apply_task_result/5` clauses so `:parked` emits waiting telemetry while `:stale` and `:terminal` only remove the local task and never call `complete_task` or `fail_task`.

- [ ] **Step 6: Reuse the same result dispatcher during graceful shutdown**

In `wait_for_tasks/1`, replace the direct `handle_task_success/3` call with:

```elixir
state = apply_task_result(result, task_meta, new_active_tasks, false, state)
wait_for_tasks(state)
```

Add a worker test whose supervised handler parks while `WorkerSupervisor.stop_worker/1` is draining and assert there is no unexpected-result warning/failure transition.

- [ ] **Step 7: Run Context and worker tests**

Run:

```bash
PGFLOW_REQUIRE_DB=1 mix test \
  test/pgflow/context_await_signal_test.exs \
  test/pgflow/worker/server_test.exs
```

Expected: PASS; the transaction test leaves the task `started`, and shutdown preserves a committed `waiting` task.

- [ ] **Step 8: Review checkpoint**

Run `git diff --check`; do not commit without separate authorization.

---

### Task 3: Retain Claimed Outcomes Through Retry and Clean Them on Terminal Transitions

**Files:**
- Modify: `priv/pgflow_helpers/sql/versions/v05/v05_up.sql`
- Modify: `priv/pgflow_helpers/sql/versions/v05/v05_down.sql`
- Modify: `test/support/db/test_helpers.sql`
- Test: `test/pgflow/await_signals_test.exs`
- Test: `test/pgflow/queries/signals_test.exs`

**Interfaces:**
- Consumes: `task_signals.claimed_at` and non-destructive await from Task 1
- Produces: retry replay, terminal cleanup, parent-state signal rejection

- [ ] **Step 1: Write a failing post-signal retry test**

Add a flow whose first resumed execution raises after receiving approval and whose second execution must receive the same approval without another signal:

```elixir
defmodule RetryAfterSignalFlow do
  use PgFlow.Flow
  @flow slug: :retry_after_signal_flow, max_attempts: 2, base_delay: 0, timeout: 30

  step :approval do
    fn input, ctx ->
      {:ok, payload} = PgFlow.Context.await_signal(ctx, wait_timeout: 0, wait_for: {1, :hour})

      if ctx.attempt == 1 do
        raise "fail after signal"
      else
        Map.merge(input, payload)
      end
    end
  end
end

test "a claimed signal is replayed after a normal handler retry", %{task_supervisor: task_supervisor} do
  compile_flow(RetryAfterSignalFlow)
  start_worker(RetryAfterSignalFlow, task_supervisor)
  run_id = start_flow_run("retry_after_signal_flow", %{"order_id" => 1})

  assert :ok = wait_until(fn -> get_task_details(run_id, "approval", 0).status == "waiting" end)
  assert {:ok, :requeued} = PgFlow.signal(run_id, :approval, %{"decision" => "approved"})
  assert {:ok, "completed"} = wait_for_run_completion(run_id)
  assert get_task_details(run_id, "approval", 0).attempts_count == 2
end
```

- [ ] **Step 2: Run the retry test and verify the current delete behavior fails**

Run:

```bash
PGFLOW_REQUIRE_DB=1 mix test test/pgflow/await_signals_test.exs
```

Expected before implementation: FAIL because the retry parks again after the first consume.

- [ ] **Step 3: Make signal writes immutable after claim**

In `signal_task`, lock the task first and then the signal row. Return `already_delivered` when `claimed_at IS NOT NULL`; never overwrite a claimed payload or timeout. Permit early buffering only when the run is `started` and the step state is `created` or `started`. Return `missing` or `terminal` without inserting for invalid targets.

Use these SQL outcomes exactly:

```text
buffered
requeued
already_delivered
expired
terminal
missing
```

When the task is `waiting`, requeue in the same function transaction with `pgmq.send`, update `step_tasks` to `queued`, and return `requeued`. When the task is `queued` or `started`, store the unclaimed payload and return `buffered`.

- [ ] **Step 4: Add atomic terminal cleanup triggers**

Add helper-owned trigger functions rather than modifying the frozen core V01 bundle:

```sql
CREATE OR REPLACE FUNCTION $SCHEMA$.cleanup_terminal_step_signals()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
BEGIN
  IF NEW.status IN ('completed', 'failed', 'skipped')
     AND OLD.status IS DISTINCT FROM NEW.status THEN
    UPDATE pgflow.step_tasks
    SET status = 'failed',
        failed_at = COALESCE(failed_at, now()),
        error_message = COALESCE(error_message, 'abandoned: step became ' || NEW.status)
    WHERE run_id = NEW.run_id
      AND step_slug = NEW.step_slug
      AND status = 'waiting';

    DELETE FROM pgflow.task_signals
    WHERE run_id = NEW.run_id AND step_slug = NEW.step_slug;
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER cleanup_terminal_step_signals
AFTER UPDATE OF status ON $SCHEMA$.step_states
FOR EACH ROW EXECUTE FUNCTION $SCHEMA$.cleanup_terminal_step_signals();
```

Add the run-level cleanup for terminal branches whose other step states remain `started`:

```sql
CREATE OR REPLACE FUNCTION $SCHEMA$.cleanup_terminal_run_signals()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
BEGIN
  IF NEW.status IN ('completed', 'failed')
     AND OLD.status IS DISTINCT FROM NEW.status THEN
    UPDATE pgflow.step_tasks
    SET status = 'failed',
        failed_at = COALESCE(failed_at, now()),
        error_message = COALESCE(error_message, 'abandoned: run became ' || NEW.status)
    WHERE run_id = NEW.run_id AND status = 'waiting';

    DELETE FROM pgflow.task_signals WHERE run_id = NEW.run_id;
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER cleanup_terminal_run_signals
AFTER UPDATE OF status ON $SCHEMA$.runs
FOR EACH ROW EXECUTE FUNCTION $SCHEMA$.cleanup_terminal_run_signals();
```

Drop both triggers and trigger functions in `v05_down.sql` after its preflight and before dropping `task_signals`.

- [ ] **Step 5: Make test reset deterministic**

At the beginning of `pgflow_tests.reset_db()` add:

```sql
DELETE FROM pgflow.task_signals;
```

Change expiry assertions from `n >= 1` to exact counts and scope any deadline update by `run_id`, `step_slug`, and `task_index`.

- [ ] **Step 6: Add terminal sibling tests**

Create map cases in `test/pgflow/await_signals_test.exs` where task index `0` is waiting and index `1` exhausts with each of `fail`, `skip`, and `skip-cascade`. Assert:

```elixir
assert get_run_status(run_id) in ["completed", "failed"]
refute get_task_details(run_id, "approval", 0).status == "waiting"
assert %{rows: [[0]]} =
         TestRepo.query!("SELECT count(*) FROM pgflow.task_signals WHERE run_id = $1", [Ecto.UUID.dump!(run_id)])
```

Then signal the old address and assert the typed result is `{:ok, :terminal}` and no PGMQ message was created.

- [ ] **Step 7: Run retry and lifecycle tests**

Run:

```bash
PGFLOW_REQUIRE_DB=1 mix test \
  test/pgflow/await_signals_test.exs \
  test/pgflow/queries/signals_test.exs
```

Expected: PASS with exact cleanup counts and no duplicate signal requirement after retry.

- [ ] **Step 8: Review checkpoint**

Inspect the trigger order and retry behavior; do not commit without separate authorization.

---

### Task 4: Enforce Deadlines at Delivery and Bound Multi-Node Recovery

**Files:**
- Modify: `priv/pgflow_helpers/sql/versions/v05/v05_up.sql`
- Modify: `lib/pgflow/queries/signals.ex`
- Modify: `lib/pgflow/worker/waiting_task_recovery.ex`
- Modify: `lib/pgflow/config.ex`
- Test: `test/pgflow/queries/signals_test.exs`
- Test: `test/pgflow/await_signals_test.exs`
- Test: `test/pgflow/config_test.exs`

**Interfaces:**
- Produces: `expire_waiting_tasks(repo, limit)` and config `:waiting_recovery_batch_size` defaulting to `100`

- [ ] **Step 1: Write strict-deadline race tests**

Cover both counterexamples:

```elixir
test "a signal after the persisted deadline yields expired and cannot replace timeout" do
  run_id = start_and_park_expired_task("strict_deadline_signal")

  assert {:ok, :expired} = Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "approved"})
  assert :timeout = await_current_task(run_id, "approval")
end

test "a signal cannot clear a timeout already queued by recovery" do
  run_id = start_and_park_expired_task("timeout_marker_wins")
  assert {:ok, 1} = Signals.expire_waiting_tasks(repo(), 100)
  assert {:ok, :expired} = Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "approved"})
  assert :timeout = await_requeued_task(run_id, "approval")
end
```

The local helpers must query the exact task row and use its current attempt/message ownership rather than sleeping.

- [ ] **Step 2: Run the tests and verify late-signal failure**

Run:

```bash
PGFLOW_REQUIRE_DB=1 mix test test/pgflow/queries/signals_test.exs
```

Expected before implementation: FAIL because `signal_task` clears `timed_out`.

- [ ] **Step 3: Enforce expiry while holding the task lock**

In `signal_task`, before writing a payload:

```sql
IF v_signal.timed_out
   OR (v_signal.wait_deadline_at IS NOT NULL AND v_signal.wait_deadline_at <= now()) THEN
  UPDATE pgflow.task_signals
  SET timed_out = true, updated_at = now()
  WHERE run_id = p_run_id AND step_slug = p_step_slug AND task_index = p_task_index;

  IF v_has_task AND v_task.status = 'waiting' THEN
    SELECT send INTO v_msg_id
    FROM pgmq.send(v_task.flow_slug, jsonb_build_object(
      'flow_slug', v_task.flow_slug,
      'run_id', p_run_id,
      'step_slug', p_step_slug,
      'task_index', p_task_index
    ));

    UPDATE pgflow.step_tasks
    SET status = 'queued', message_id = v_msg_id, queued_at = now()
    WHERE run_id = p_run_id AND step_slug = p_step_slug AND task_index = p_task_index;
  END IF;

  RETURN QUERY SELECT 'expired'::text;
  RETURN;
END IF;
```

Never set `timed_out = false` over an existing timeout marker.

- [ ] **Step 4: Add the partial deadline index and bounded sweep**

Add:

```sql
CREATE INDEX task_signals_unresolved_deadline_idx
ON $SCHEMA$.task_signals (wait_deadline_at)
WHERE wait_deadline_at IS NOT NULL
  AND timed_out = false
  AND payload IS NULL;
```

Change the SQL function to `expire_waiting_tasks(p_limit integer)` and select candidates ordered by deadline with `LIMIT p_limit FOR UPDATE OF st SKIP LOCKED`. Join and require `runs.status = 'started'` and `step_states.status = 'started'` before requeueing.

- [ ] **Step 5: Thread the recovery batch size through Elixir**

Add to `PgFlow.Config`:

```elixir
waiting_recovery_batch_size: [
  type: :pos_integer,
  default: 100,
  doc: "Maximum waiting tasks expired per recovery transaction"
]
```

Store it in `WaitingTaskRecovery` state and call:

```elixir
Signals.expire_waiting_tasks(state.repo, state.batch_size)
```

- [ ] **Step 6: Run strict-deadline and recovery tests**

Run:

```bash
PGFLOW_REQUIRE_DB=1 mix test \
  test/pgflow/queries/signals_test.exs \
  test/pgflow/await_signals_test.exs \
  test/pgflow/config_test.exs
```

Expected: PASS; each batch expires at most the configured limit, and late signals cannot replace timeouts.

- [ ] **Step 7: Review checkpoint**

Inspect the expiry query for the partial-index predicate and `SKIP LOCKED`; do not commit without separate authorization.

---

### Task 5: Return Honest Public Outcomes and Expose Waiting State

**Files:**
- Modify: `lib/pgflow/queries/signals.ex`
- Modify: `lib/pgflow/client.ex`
- Modify: `lib/pgflow.ex`
- Test: `test/pgflow/client_test.exs`
- Test: `test/pgflow/queries/signals_test.exs`

**Interfaces:**
- Produces:
  - `PgFlow.signal/3,4 :: {:ok, signal_outcome()} | {:error, term()}`
  - `PgFlow.get_waiting_tasks/1 :: {:ok, [waiting_task()]} | {:error, term()}`

- [ ] **Step 1: Write public error and discovery tests**

Replace the unconditional-`:ok` tests with:

```elixir
test "returns missing for an unknown run without storing a row" do
  run_id = Ecto.UUID.generate()
  assert {:ok, :missing} = Client.signal(run_id, :process, %{"ok" => true})

  assert %{rows: [[0]]} =
           TestRepo.query!("SELECT count(*) FROM pgflow.task_signals WHERE run_id = $1", [Ecto.UUID.dump!(run_id)])
end

test "returns validation and configuration failures" do
  assert {:error, :invalid_run_id} = Client.signal("not-a-uuid", :process, %{"ok" => true})

  :persistent_term.erase({PgFlow, :repo})
  Application.delete_env(:pgflow, :repo)
  assert {:error, "Repo not configured"} = Client.signal(Ecto.UUID.generate(), :process, %{"ok" => true})
end

test "lists waiting tasks without exposing payloads" do
  {:ok, run_id} = Client.start_flow(ClientTestFlow, %{"value" => 1})
  park_client_test_task(run_id)

  assert {:ok, [%{step_slug: "process", task_index: 0, wait_deadline_at: deadline}]} =
           Client.get_waiting_tasks(run_id)

  assert is_nil(deadline) or match?(%DateTime{}, deadline)
end
```

- [ ] **Step 2: Run client tests and verify failure**

Run:

```bash
PGFLOW_REQUIRE_DB=1 mix test test/pgflow/client_test.exs
```

Expected before implementation: FAIL because errors are discarded and discovery does not exist.

- [ ] **Step 3: Decode signal outcomes instead of returning unconditional success**

Use this public type:

```elixir
@type signal_outcome :: :buffered | :requeued | :already_delivered | :expired | :terminal | :missing

@spec signal(String.t(), atom() | String.t(), non_neg_integer(), map() | list()) ::
        {:ok, signal_outcome()} | {:error, term()}
def signal(run_id, step_slug, task_index, payload)
    when (is_map(payload) or is_list(payload)) and is_integer(task_index) and task_index >= 0 do
  with {:ok, _uuid} <- Ecto.UUID.cast(run_id),
       {:ok, repo} <- get_repo() do
    Signals.signal_task(repo, run_id, to_string(step_slug), task_index, payload)
  else
    :error -> {:error, :invalid_run_id}
    {:error, reason} -> {:error, reason}
  end
end
```

Remove the debug-only failure suppression and update `PgFlow` delegates/specs.

- [ ] **Step 4: Add waiting-task discovery to the query layer**

In `Signals`:

```elixir
@type waiting_task :: %{
        step_slug: String.t(),
        task_index: non_neg_integer(),
        wait_deadline_at: DateTime.t() | nil,
        waiting_since: DateTime.t()
      }

@spec list_waiting_tasks(module(), String.t()) :: {:ok, [waiting_task()]} | {:error, term()}
def list_waiting_tasks(repo, run_id) do
  sql = """
  SELECT st.step_slug, st.task_index, ts.wait_deadline_at, ts.inserted_at
  FROM pgflow.step_tasks st
  JOIN pgflow.task_signals ts
    ON ts.run_id = st.run_id
   AND ts.step_slug = st.step_slug
   AND ts.task_index = st.task_index
  WHERE st.run_id = $1 AND st.status = 'waiting'
  ORDER BY st.step_slug, st.task_index
  """

  case SQL.query(repo, sql, [parse_uuid(run_id)]) do
    {:ok, %{rows: rows}} ->
      {:ok,
       Enum.map(rows, fn [step_slug, task_index, deadline, inserted_at] ->
         %{
           step_slug: step_slug,
           task_index: task_index,
           wait_deadline_at: deadline,
           waiting_since: inserted_at
         }
       end)}

    {:error, reason} ->
      {:error, reason}
  end
end
```

Delegate through `Client.get_waiting_tasks/1` and `PgFlow.get_waiting_tasks/1`. Do not expose payload or `claimed_at`.

- [ ] **Step 5: Run public API tests**

Run:

```bash
PGFLOW_REQUIRE_DB=1 mix test \
  test/pgflow/client_test.exs \
  test/pgflow/queries/signals_test.exs
```

Expected: PASS with no durable row for missing/terminal targets and no swallowed infrastructure errors.

- [ ] **Step 6: Review checkpoint**

Review all public types and documentation signatures together; do not commit without separate authorization.

---

### Task 6: Ship a Version-Aware Upgrade, Compatibility Check, and Safe Rollback

**Files:**
- Modify: `lib/mix/tasks/pgflow.gen.helpers_migration.ex`
- Modify: `test/mix/tasks/pgflow.gen.helpers_migration_test.exs`
- Create: `lib/pgflow/schema_compatibility.ex`
- Create: `test/pgflow/schema_compatibility_test.exs`
- Create: `test/pgflow/migrations/helpers_upgrade_test.exs`
- Modify: `priv/pgflow_helpers/sql/versions/v05/v05_up.sql`
- Modify: `priv/pgflow_helpers/sql/versions/v05/v05_down.sql`
- Modify: `lib/pgflow/supervisor.ex`
- Modify: `lib/mix/tasks/pgflow.check_schema.ex`
- Create via generator during execution: `demo/priv/repo/migrations/*_upgrade_pgflow_helpers_to_v05.exs`

**Interfaces:**
- Produces: `mix pgflow.gen.helpers_migration --from-version 4`
- Produces: `PgFlow.SchemaCompatibility.check_await_signals/1` and `check_await_signals!/1`

- [ ] **Step 1: Extend the existing migration-generator test**

Preserve the no-option initial-install assertions, then add a temporary
migration-directory case asserting `--from-version 4` generates content that includes:

```elixir
assert content =~ "PgFlow.HelpersMigration.up(version: 5)"
assert content =~ "PgFlow.HelpersMigration.down(version: 4)"
refute content =~ "PgFlow.HelpersMigration.down()"
```

Also assert zero, negative, current/higher, invalid, and positional arguments
raise before writing. Preserve `--migrations-path` and `-p` behavior.

- [ ] **Step 2: Consolidate version-aware output into the existing generator**

The generated migration body is:

```elixir
defmodule MyApp.Repo.Migrations.UpgradePgflowHelpersToV05 do
  use Ecto.Migration

  def up, do: PgFlow.HelpersMigration.up(version: 5)
  def down, do: PgFlow.HelpersMigration.down(version: 4)
end
```

Treat `--from-version` as optional. With no option, preserve the existing
`add_pgflow_helpers` initial-install migration with `up()` and `down()` exactly.
When supplied, parse it as an integer lower than `current_version()`, use
`Mix.Tasks.Pgflow.Helpers.generate_timestamp/0` plus `get_app_module/0`, and
name the file `upgrade_pgflow_helpers_to_v05.exs` using the current helper
version rendered with two digits. Do not add a separate one-off Mix task.

- [ ] **Step 3: Add safe rollback preflight before any DROP**

The first executable statement in `v05_down.sql` must be:

```sql
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pgflow.task_signals)
     OR EXISTS (SELECT 1 FROM pgflow.step_tasks WHERE status = 'waiting') THEN
    RAISE EXCEPTION
      'cannot roll pgflow helpers V05 back to V04 while task signals or waiting tasks exist; resolve or cancel them first';
  END IF;
END;
$$;
```

Only after that preflight may the down migration drop triggers, functions, index, table, and restore `valid_status`.

- [ ] **Step 4: Add real V04-to-V05-to-V04 migration tests**

In `helpers_upgrade_test.exs`, define Ecto migration modules with exact targets:

```elixir
defmodule UpToV04 do
  use Ecto.Migration
  def up, do: PgFlow.HelpersMigration.up(version: 4)
end

defmodule UpToV05 do
  use Ecto.Migration
  def up, do: PgFlow.HelpersMigration.up(version: 5)
end

defmodule DownToV04 do
  use Ecto.Migration
  def up, do: PgFlow.HelpersMigration.down(version: 4)
end
```

Test that V05 creates `task_signals` and the new functions, that an empty V05 rolls back to V04, and that rollback with a buffered signal raises before any V05 object is removed.

- [ ] **Step 5: Add runtime compatibility checking**

Create:

```elixir
defmodule PgFlow.SchemaCompatibility do
  @moduledoc "Checks that the configured repository has the helper objects required by this PgFlow release."

  @required_helper_version 5

  @spec check_await_signals(module()) :: :ok | {:error, String.t()}
  def check_await_signals(repo) do
    version =
      EctoEvolver.Adapters.Postgres.get_version(
        repo,
        "pgflow",
        {:view, "extensions_version"}
      )

    cond do
      version < @required_helper_version ->
        {:error,
         "PgFlow helpers V05 are required but the database is at V#{String.pad_leading(to_string(version), 2, "0")}; run `mix pgflow.gen.helpers_migration --from-version #{version}` and apply the generated helpers upgrade migration"}

      not required_objects_exist?(repo) ->
        {:error, "PgFlow helpers report V05 but await-signals objects are missing"}

      true ->
        :ok
    end
  end

  @spec check_await_signals!(module()) :: :ok
  def check_await_signals!(repo) do
    case check_await_signals(repo) do
      :ok -> :ok
      {:error, message} -> raise message
    end
  end
end
```

Implement the object check with exact signatures:

```elixir
defp required_objects_exist?(repo) do
  sql = """
  SELECT
    to_regclass('pgflow.task_signals') IS NOT NULL,
    to_regprocedure(
      'pgflow.await_task_signal(uuid,text,integer,integer,bigint,bigint,boolean)'
    ) IS NOT NULL,
    to_regprocedure('pgflow.signal_task(uuid,text,integer,jsonb)') IS NOT NULL,
    to_regprocedure('pgflow.expire_waiting_tasks(integer)') IS NOT NULL
  """

  case repo.query(sql) do
    {:ok, %{rows: [[true, true, true, true]]}} -> true
    _ -> false
  end
end
```

- [ ] **Step 6: Fail closed before starting workers and reuse the check in Mix**

Call `SchemaCompatibility.check_await_signals!(repo)` in `PgFlow.Supervisor.init/1` after extracting the repo and before constructing worker children. Extend `pgflow.check_schema` required tables/functions and route its V05 result through the shared module.

- [ ] **Step 7: Generate the demo upgrade wrapper**

From `demo/`, run only the generator:

```bash
mix pgflow.gen.helpers_migration --from-version 4
```

Record the generated path and verify its down target is `4`. Do not invent or handwrite the timestamped filename.

- [ ] **Step 8: Stage the widened status constraint without a long validation scan**

In V05 up, add the widened constraint as `NOT VALID` after dropping the old narrower constraint. Because every pre-V05 row already satisfied the narrower constraint, the new constraint is enforced for new writes without scanning historical rows under the migration's access-exclusive lock. Document a separately committed `VALIDATE CONSTRAINT` operator step rather than validating inside the same Ecto transaction.

- [ ] **Step 9: Run generator, compatibility, and migration gates**

Run the non-DB tests first:

```bash
mix test \
  test/mix/tasks/pgflow.gen.helpers_migration_test.exs \
  test/pgflow/schema_compatibility_test.exs
```

Then, only on an isolated PgFlow test database:

```bash
PGFLOW_REQUIRE_DB=1 mix test --only migration test/pgflow/migrations/helpers_upgrade_test.exs
```

Expected: PASS, including rollback refusal before destructive statements.

- [ ] **Step 10: Review checkpoint**

Inspect generated migration targets and rollback ordering; do not commit without separate authorization.

---

### Task 7: Repair the Approval Demo and Make LiveView Events Run-Scoped

**Files:**
- Modify: `demo/lib/pgflow_demo/flows/approval_flow.ex`
- Modify: `demo/lib/pgflow_demo_web/live/flow_demo_live.ex`
- Modify: `demo/test/pgflow_demo/approval_flow_test.exs`
- Modify: `demo/test/pgflow_demo_web/live/flow_demo_live_test.exs`

**Interfaces:**
- Consumes: typed `Client.signal/3` and `Client.get_waiting_tasks/1`
- Produces: correct receipt data, stale-event rejection, decision submission state, user-visible delivery errors

- [ ] **Step 1: Write the failing approval output test**

Update `approval_flow_test.exs` to assert the linear handoff:

```elixir
test "await_approval carries order data to charge" do
  charge_handler = ApprovalFlow.__pgflow_handler__(:charge)

  assert ApprovalFlow.__pgflow_definition__().steps
         |> Enum.find(&(&1.slug == :charge))
         |> Map.fetch!(:depends_on) == [:await_approval]

  approved = %{
    "order_id" => "ord_demo",
    "amount" => 42,
    "decision" => "approved"
  }

  assert charge_handler.(%{"await_approval" => approved}, nil) == %{
           "charged" => true,
           "order_id" => "ord_demo",
           "amount" => 42,
           "decision" => "approved"
         }
end
```

- [ ] **Step 2: Carry order data through the approval result**

Change the handler to:

```elixir
step :await_approval, depends_on: [:create_order], max_attempts: 1 do
  fn deps, ctx ->
    order = deps["create_order"]

    case PgFlow.Context.await_signal(ctx, wait_for: {1, :hour}, wait_timeout: 0) do
      {:ok, %{"decision" => "approved"}} -> Map.put(order, "decision", "approved")
      {:ok, _} -> raise "rejected"
      {:error, :timeout} -> raise "no decision"
    end
  end
end

step :charge, depends_on: [:await_approval] do
  fn deps, _ctx ->
    approved = deps["await_approval"]

    %{
      "charged" => true,
      "order_id" => approved["order_id"],
      "amount" => approved["amount"],
      "decision" => approved["decision"]
    }
  end
end
```

Regenerate the compiled flow migration using the repository's existing flow migration task; do not edit generated flow SQL by hand.

- [ ] **Step 3: Write stale-event and malformed-decision tests**

Add LiveView tests that start with `run_id: "run-b"`, send events for `"run-a"`, and assert no status/error/output assign changes. Cover `task_started`, `task_waiting`, `task_completed`, `step_skipped`, `run_completed`, and `run_failed`.

Also assert:

```elixir
render_click(view, "signal_approval", %{"decision" => "forged"})
assert has_element?(view, "#approval-actions")
```

- [ ] **Step 4: Guard every run-scoped event**

Add:

```elixir
defp current_run?(socket, run_id), do: is_binary(run_id) and run_id == socket.assigns.run_id
```

Every `{:pgflow, run_id, event}` handler returns `{:noreply, socket}` unless `current_run?/2` is true. Do not rely on unsubscribe removing messages already delivered to the mailbox.

- [ ] **Step 5: Show signal delivery state and prevent double submission**

Add `:approval_submitted` and `:approval_error` assigns. In the valid event:

```elixir
case Client.signal(run_id, :await_approval, %{"decision" => decision}) do
  {:ok, outcome} when outcome in [:buffered, :requeued, :already_delivered] ->
    {:noreply, assign(socket, approval_submitted: true, approval_error: nil)}

  {:ok, outcome} ->
    {:noreply, assign(socket, approval_submitted: false, approval_error: "Signal was not delivered: #{outcome}")}

  {:error, reason} ->
    {:noreply, assign(socket, approval_submitted: false, approval_error: "Signal delivery failed: #{inspect(reason)}")}
end
```

Hide/disable both buttons as soon as `approval_submitted` is true. Add a separate catch-all `signal_approval` clause returning the unchanged socket for invalid input.

- [ ] **Step 6: Replace the demo's private waiting query**

Use `Client.get_waiting_tasks(run_id)` in reconciliation and map its returned task maps through `apply_waiting_task_statuses/3`. Remove the direct `PgFlow.Schema.StepTask` query and its rescue block.

- [ ] **Step 7: Run demo tests only in an isolated demo test database**

Do not run against the database used by a running demo server. In CI or a dedicated disposable cluster, run:

```bash
cd demo
mix test \
  test/pgflow_demo/approval_flow_test.exs \
  test/pgflow_demo_web/live/flow_demo_live_test.exs
```

Expected: PASS with real receipt fields, stale events ignored, and delivery failures visible.

- [ ] **Step 8: Review checkpoint**

Inspect the generated migration, LiveView event guards, and user-visible error states; do not commit without separate authorization.

---

### Task 8: Publish the Actual Contract and Restore Quality Gates

**Files:**
- Modify: `README.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/ELIXIR_VS_SUPABASE.md`
- Modify: `lib/pgflow/context.ex`
- Modify: `lib/pgflow/client.ex`
- Modify: `lib/pgflow/queries/signals.ex`
- Modify: `lib/mix/tasks/pgflow.gen.helpers_migration.ex`
- Modify: `demo/README.md`
- Modify: `demo/priv/repo/migrations/20260822093816_compile_approval_flow.exs`
- Modify: `demo/priv/repo/migrations/20260822113249_compile_send_email.exs`

**Interfaces:**
- Produces: public documentation matching the hardened implementation and zero Doctor/format failures

- [ ] **Step 1: Add the public await contract next to the API**

The `Context.await_signal/2` docs must state all of the following explicitly:

```text
- The handler restarts from the top after park/resume; pre-await effects must be idempotent.
- V1 supports one await point per task.
- Caller-owned Repo transactions are unsupported and raise PgFlow.AwaitSignalTransactionError.
- The first PostgreSQL-computed wait_for deadline survives retries.
- An accepted signal or timeout is replayed on ordinary handler retry until terminal completion/failure.
- wait_timeout is in-process polling time and must not exceed the handler's configured task timeout.
```

Document every supported singular/plural duration unit exactly as implemented.

- [ ] **Step 2: Correct signal delivery, authorization, and compatibility documentation**

Update README and architecture examples to match tagged return values:

```elixir
case PgFlow.signal(run_id, :approval, %{"decision" => "approved"}) do
  {:ok, outcome} when outcome in [:buffered, :requeued] -> :accepted
  {:ok, :already_delivered} -> :idempotent_success
  {:ok, outcome} when outcome in [:expired, :terminal, :missing] -> {:not_delivered, outcome}
  {:error, reason} -> {:retry, reason}
end
```

State that application controllers/webhooks must authenticate the caller, authorize tenant/run ownership, validate payload shape, and enforce a size limit before calling PgFlow.

Clarify that await-signals is an Elixir helper extension: TypeScript/Deno workers can share the core schema, but they cannot execute an awaited Elixir handler without equivalent runtime support.

- [ ] **Step 3: Correct installation and upgrade guidance**

Replace the claim that dependency upgrades require no new migrations with:

```text
Initial installation uses `mix pgflow.setup`. Existing installations must generate and apply a new version-aware helpers upgrade migration whenever the PgFlow release notes increase the helpers version. For V05, run `mix pgflow.gen.helpers_migration --from-version 4` and apply it before starting the new worker release.
```

Document that V05 rollback refuses active waits/signals and requires an operator drain first.

- [ ] **Step 4: Add `@doc` to every public Signals function**

Document parameters, typed outcomes, ownership fencing, and whether payload data is returned. Do not mark public functions `@doc false` merely to satisfy Doctor.

- [ ] **Step 5: Format root and demo code**

Run:

From the repository root, run `mix format`. Then change the working directory to `demo/` and run `mix format` there as a separate shell call.

This must format both existing generated demo migrations rather than editing whitespace manually.

- [ ] **Step 6: Run documentation and formatting gates**

Run:

From the repository root:

```bash
mix format --check-formatted
mix doctor --raise
```

From `demo/`:

```bash
mix format --check-formatted
```

Expected: all commands exit `0`; `PgFlow.Queries.Signals` reports documented public functions.

- [ ] **Step 7: Review checkpoint**

Search the public docs for stale `always returns :ok`, unrestricted cross-language parity, and old migration guidance. Do not commit without separate authorization.

---

### Task 9: Complete the Isolated Merge-Readiness Gate

**Files:**
- Modify only when a gate exposes a defect in an in-scope file from Tasks 1-8

**Interfaces:**
- Produces: evidence-backed merge-readiness report with non-DB, DB, migration, demo, and residual manual gates separated

- [ ] **Step 1: Prove the intended database target before any DB-backed command**

Run read-only checks:

```bash
rg -n 'hostname:|port:|database:' config/test.exs
pg_isready -h localhost -p 54323
```

Proceed only when the target is the dedicated `pgflow_test` database on port `54323` or an isolated CI database. If unavailable, stop the DB portion and report it unverified. Do not start or modify containers/services.

- [ ] **Step 2: Run deterministic focused DB tests**

```bash
PGFLOW_REQUIRE_DB=1 mix test \
  test/pgflow/queries/signals_test.exs \
  test/pgflow/context_await_signal_test.exs \
  test/pgflow/await_signals_test.exs \
  test/pgflow/client_test.exs \
  test/pgflow/schema_compatibility_test.exs
```

Expected: zero failures and no excluded integration tests.

- [ ] **Step 3: Run the destructive migration suite only on the isolated PgFlow database**

```bash
PGFLOW_REQUIRE_DB=1 mix test --only migration test/pgflow/migrations/helpers_upgrade_test.exs
```

Expected: V04-to-V05-to-V04 passes when empty; active-signal rollback refusal is asserted without leaving the schema partially downgraded.

- [ ] **Step 4: Run the complete root gates**

```bash
mix test --exclude pg_cron
mix format --check-formatted
mix doctor --raise
mix quality
```

Expected: zero failures and exit `0` from every command. Record excluded counts explicitly; a skipped integration suite is not green.

- [ ] **Step 5: Run demo gates only against an isolated demo database**

Verify the demo test configuration does not point at a database used by a running demo or Inbox process. Then run:

```bash
cd demo
mix test
mix quality
```

Expected: zero failures and exit `0`. If isolation cannot be proven, leave these gates unverified rather than touching the shared database.

- [ ] **Step 6: Run static Git checks**

```bash
git diff --check
git status --short --branch
git merge-tree --write-tree origin/main HEAD
```

Expected: no whitespace errors, only intended worktree changes, and a successful merge-tree write.

- [ ] **Step 7: Perform final counterfactual review**

For each scenario below, point to a passing test and the exact SQL/Elixir guard:

```text
signal before park
park before signal
signal after deadline
signal after timeout requeue
handler failure after accepted signal
stale attempt reaches await after a newer attempt starts
outer Repo transaction
terminal map sibling
graceful shutdown during park
existing V04 consumer upgrade
rollback with active wait
missing repo and malformed UUID
stale LiveView event after reset/new run
```

Any scenario lacking both code evidence and a passing isolated test remains a merge blocker.

- [ ] **Step 8: Final review checkpoint**

Prepare the merge-readiness report. Do not create a commit, push, or modify the PR without separate authorization.

---

## Explicit Fast-Follows Outside This Plan

- Multiple named/sequential signals or continuation checkpoints.
- A separate durable `task:resumed` telemetry event; redispatch continues emitting `task:start`.
- Demo run restoration through URL/session after a browser reconnect.
- Broader pre-existing attempt fencing for ordinary `complete_task` / `fail_task` paths unrelated to await-signals.
- Refactoring duplicated test helpers beyond the helpers directly touched by this feature.
- Unrelated `enqueue_in` telemetry timing changes.

## Completion Criteria

The plan is complete only when:

1. Signal-before-park and park-before-signal are both deterministic and lossless.
2. Stale attempts cannot read, park, fail, or complete the current await transition.
3. Accepted payloads/timeouts replay across retry and disappear on terminal state.
4. Deadlines are PostgreSQL-owned and late signals cannot override expiry.
5. Missing, terminal, expired, validation, and infrastructure outcomes are distinguishable to callers.
6. Existing V04 consumers have a tested V05 upgrade and V04 rollback path.
7. Rollback with active signal state fails before destructive work.
8. The demo receipt contains real order data and cannot react to stale-run messages.
9. Root and demo quality gates pass.
10. Database-backed evidence comes from an isolated PgFlow database, never Inbox or another running application.
