defmodule PgFlow.ContextAwaitSignalUnitTest do
  use ExUnit.Case, async: true

  alias PgFlow.Context
  alias PgFlow.Queries.Signals

  defmodule OutsideTransactionRepo do
    def in_transaction?, do: false
  end

  defmodule InsideTransactionRepo do
    def in_transaction?, do: true
  end

  defp context(repo) do
    Context.new(
      run_id: Ecto.UUID.generate(),
      step_slug: :approval,
      task_index: 0,
      attempt: 2,
      message_id: 42,
      repo: repo
    )
  end

  test "rejects await_signal while the caller owns a transaction" do
    assert_raise PgFlow.AwaitSignalTransactionError, fn ->
      Context.await_signal(context(InsideTransactionRepo), wait_timeout: 0)
    end
  end

  test "validates wait options before querying for a signal" do
    ctx = context(OutsideTransactionRepo)

    assert_raise ArgumentError, ~r/wait_timeout must be a non-negative integer/, fn ->
      Context.await_signal(ctx, wait_timeout: -1)
    end

    assert_raise ArgumentError, ~r/wait_for must be :infinity or a positive duration/, fn ->
      Context.await_signal(ctx, wait_for: {1, :week})
    end
  end

  test "rejects a hand-built context without worker dispatch identity" do
    ctx = %{context(OutsideTransactionRepo) | message_id: nil}

    assert_raise ArgumentError, ~r/requires a worker-issued context with a message_id/, fn ->
      Context.await_signal(ctx, wait_timeout: 0)
    end
  end

  test "routes every poll and the final park through the atomic await query" do
    source = File.read!(Path.expand("../../lib/pgflow/context.ex", __DIR__))

    assert source =~ "Signals.await_task_signal("
    assert source =~ "await_once(ctx, step_slug, wait_for_seconds, true)"
    refute source =~ "Signals.consume_task_signal("
    refute source =~ "Signals.park_waiting_task("
  end

  test "binds database-confirmed control outcomes to the dispatch identity" do
    source = File.read!(Path.expand("../../lib/pgflow/context.ex", __DIR__))

    assert source =~ "outcome in [:parked, :stale, :terminal]"
    assert source =~ "throw({:pgflow_await, outcome, ctx.attempt, ctx.message_id})"
  end

  test "unexpected successful await rows return a typed decoder error" do
    rows = [["future_outcome", nil]]

    assert {:error, {:unexpected_await_outcome, ^rows}} =
             Signals.decode_await_result({:ok, %{rows: rows}})
  end

  test "public await docs require worker context and forbid catching control throws" do
    source = File.read!(Path.expand("../../lib/pgflow/context.ex", __DIR__))

    assert source =~ "requires the worker-issued context"
    assert source =~ "must not catch PgFlow's internal await control throw"
  end

  test "public signal docs define overwrite and claim immutability" do
    client_source = File.read!(Path.expand("../../lib/pgflow/client.ex", __DIR__))
    readme = File.read!(Path.expand("../../README.md", __DIR__))

    for source <- [client_source, readme] do
      assert source =~ "last write wins"
      assert source =~ "immutable after claim"
    end
  end
end

defmodule PgFlow.ContextAwaitSignalTest do
  @moduledoc """
  Integration tests for `PgFlow.Context.await_signal/2` and `PgFlow.signal/3,4`.
  Drives the shipped API, not a copy of consume/park SQL.
  """
  use PgFlow.IntegrationCase, async: false

  alias PgFlow.Context
  alias PgFlow.Queries.{Flows, Workers}

  @moduletag timeout: 30_000
  @moduletag :integration

  setup do
    :persistent_term.put({PgFlow, :repo}, TestRepo)

    on_exit(fn ->
      :persistent_term.erase({PgFlow, :repo})
    end)

    :ok
  end

  defp compile_one_step_flow(flow_slug, step_slug) do
    create_flow(flow_slug)
    add_step(flow_slug, step_slug)
    flow_slug
  end

  defp start_started_task(flow_slug, input) do
    run_id = start_flow_run(flow_slug, input)
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
    {:ok, messages} = Flows.read(TestRepo, flow_slug, 30, 10)
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    {:ok, _} = Flows.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)
    run_id
  end

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

  test "returns buffered payload without parking" do
    compile_one_step_flow("await_buffer_flow", "approval")
    run_id = start_started_task("await_buffer_flow", %{"order_id" => 1})

    assert {:ok, :buffered} = PgFlow.signal(run_id, :approval, %{"decision" => "yes"})

    ctx = context_for(run_id, :approval)

    assert {:ok, %{"decision" => "yes"}} =
             Context.await_signal(ctx, wait_for: {1, :hour}, wait_timeout: 0)

    assert get_task_details(run_id, "approval", 0).status != "waiting"
  end

  test "parks when no signal and wait_timeout is 0" do
    compile_one_step_flow("await_park_flow", "approval")
    run_id = start_started_task("await_park_flow", %{"order_id" => 1})
    ctx = context_for(run_id, :approval)

    assert catch_throw(Context.await_signal(ctx, wait_timeout: 0)) ==
             {:pgflow_await, :parked, ctx.attempt, ctx.message_id}

    assert get_task_details(run_id, "approval", 0).status == "waiting"
  end

  test "last write wins: two signals then await yields the last payload" do
    compile_one_step_flow("await_lww_flow", "approval")
    run_id = start_started_task("await_lww_flow", %{})

    assert {:ok, :buffered} =
             PgFlow.signal(run_id, :approval, %{"decision" => "rejected"})

    assert {:ok, :buffered} =
             PgFlow.signal(run_id, :approval, 0, %{"decision" => "approved"})

    ctx = context_for(run_id, :approval)
    assert {:ok, %{"decision" => "approved"}} = Context.await_signal(ctx, wait_timeout: 0)
  end

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
    ctx =
      Context.new(
        run_id: Ecto.UUID.generate(),
        step_slug: :approval,
        task_index: 0,
        attempt: 1,
        message_id: 1,
        repo: TestRepo
      )

    assert_raise ArgumentError, ~r/wait_timeout must be a non-negative integer/, fn ->
      Context.await_signal(ctx, wait_timeout: -1)
    end

    assert_raise ArgumentError, ~r/wait_for must be :infinity or a positive duration/, fn ->
      Context.await_signal(ctx, wait_for: {1, :week})
    end
  end
end
