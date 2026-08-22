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
    Context.new(
      run_id: run_id,
      step_slug: step_slug,
      task_index: 0,
      attempt: 1,
      repo: TestRepo
    )
  end

  test "returns buffered payload without parking" do
    compile_one_step_flow("await_buffer_flow", "approval")
    run_id = start_flow_run("await_buffer_flow", %{"order_id" => 1})

    assert :ok = PgFlow.signal(run_id, :approval, %{"decision" => "yes"})

    ctx = context_for(run_id, :approval)

    assert {:ok, %{"decision" => "yes"}} =
             Context.await_signal(ctx, wait_for: {1, :hour}, wait_timeout: 0)

    assert get_task_details(run_id, "approval", 0).status != "waiting"
  end

  test "parks when no signal and wait_timeout is 0" do
    compile_one_step_flow("await_park_flow", "approval")
    run_id = start_started_task("await_park_flow", %{"order_id" => 1})
    ctx = context_for(run_id, :approval)

    assert catch_throw(Context.await_signal(ctx, wait_timeout: 0)) == {:pgflow_await, :parked}
    assert get_task_details(run_id, "approval", 0).status == "waiting"
  end

  test "last write wins: two signals then await yields the last payload" do
    compile_one_step_flow("await_lww_flow", "approval")
    run_id = start_flow_run("await_lww_flow", %{})

    assert :ok = PgFlow.signal(run_id, :approval, %{"decision" => "rejected"})
    assert :ok = PgFlow.signal(run_id, :approval, 0, %{"decision" => "approved"})

    ctx = context_for(run_id, :approval)
    assert {:ok, %{"decision" => "approved"}} = Context.await_signal(ctx, wait_timeout: 0)
  end
end
