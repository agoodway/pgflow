defmodule PgFlow.Worker.ScalingTest do
  use ExUnit.Case

  use PgFlow.Test.WaitHelpers

  import PgFlow.Test.FlowFixtures, only: [compile_flow: 1, start_flow_run: 2, worker_config: 3]

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Queries
  alias PgFlow.Test.GatedFlow
  alias PgFlow.TestRepo
  alias PgFlow.Worker.Server

  @moduletag timeout: 30_000
  @moduletag :integration

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    :persistent_term.put({GatedFlow, :test_pid}, self())

    task_supervisor = start_supervised!(Task.Supervisor)
    _flow_slug = compile_flow(GatedFlow)

    # ExUnit stops supervised workers and the task supervisor before on_exit runs.
    on_exit(fn ->
      :persistent_term.erase({GatedFlow, :test_pid})
      Sandbox.mode(TestRepo, :manual)
    end)

    {:ok, task_supervisor: task_supervisor}
  end

  describe "scaling up starts queued tasks immediately" do
    test "with signal_strategy polling", %{task_supervisor: task_supervisor} do
      assert_scaling_up_immediately(task_supervisor, :polling)
    end

    test "with signal_strategy notify", %{task_supervisor: task_supervisor} do
      assert_scaling_up_immediately(task_supervisor, :notify)
    end
  end

  test "scaling down lets active tasks finish and limits new claims", %{
    task_supervisor: task_supervisor
  } do
    flow_slug = Atom.to_string(GatedFlow.__pgflow_definition__().slug)

    run_ids =
      for n <- 1..5 do
        start_flow_run(flow_slug, %{"n" => n})
      end

    worker_pid = start_worker(GatedFlow, task_supervisor, max_concurrency: 3)

    handler1 = receive_started()
    handler2 = receive_started()
    handler3 = receive_started()

    assert {:ok, _} = Server.scale(worker_pid, max_concurrency: 1)
    refute_receive {:task_started, _, _}, 300

    send(handler1, :release)
    refute_receive {:task_started, _, _}, 300

    send(handler2, :release)
    refute_receive {:task_started, _, _}, 300

    send(handler3, :release)
    handler4 = receive_started()
    refute_receive {:task_started, _, _}, 300

    send(handler4, :release)
    handler5 = receive_started()
    send(handler5, :release)

    for run_id <- run_ids do
      assert_run_completed(run_id)
    end

    Server.stop(worker_pid)
  end

  test "scale changes batch_size", %{task_supervisor: task_supervisor} do
    worker_pid =
      start_worker(GatedFlow, task_supervisor, batch_size: 10, max_concurrency: 1)

    assert {:ok, %{batch_size: 2, configured_batch_size: 10}} =
             Server.scale(worker_pid, batch_size: 2)

    Server.stop(worker_pid)
  end

  test "a stopping worker refuses to scale", %{task_supervisor: task_supervisor} do
    flow_slug = Atom.to_string(GatedFlow.__pgflow_definition__().slug)
    _run_id = start_flow_run(flow_slug, %{"n" => 1})

    worker_pid = start_worker(GatedFlow, task_supervisor, max_concurrency: 2)

    handler = receive_started()

    stop_task = Task.async(fn -> Server.stop(worker_pid) end)

    wait_until(fn -> Server.check(worker_pid).lifecycle == :stopping end)

    assert {:error, :not_running} = Server.scale(worker_pid, max_concurrency: 5)
    assert Server.check(worker_pid).max_concurrency == 2

    send(handler, :release)
    Task.await(stop_task)
  end

  defp assert_scaling_up_immediately(task_supervisor, signal_strategy) do
    flow_slug = Atom.to_string(GatedFlow.__pgflow_definition__().slug)

    run_ids =
      for n <- 1..3 do
        start_flow_run(flow_slug, %{"n" => n})
      end

    worker_opts =
      case signal_strategy do
        :polling ->
          [
            signal_strategy: :polling,
            min_poll_interval: 60_000,
            max_poll_interval: 60_000
          ]

        :notify ->
          [
            signal_strategy: :notify,
            notify_fallback_interval: 600_000
          ]
      end

    worker_pid =
      start_worker(GatedFlow, task_supervisor, Keyword.merge(worker_opts, max_concurrency: 1))

    if signal_strategy == :polling do
      send(worker_pid, :poll_now)
    end

    assert_receive {:task_started, handler1, _}, 5_000
    handlers = [handler1]

    assert {:ok, %{max_concurrency: 3}} = Server.scale(worker_pid, max_concurrency: 3)

    assert_receive {:task_started, handler2, _}, 1_000
    assert_receive {:task_started, handler3, _}, 1_000
    handlers = [handler2, handler3 | handlers]

    for handler <- handlers do
      send(handler, :release)
    end

    for run_id <- run_ids do
      assert_run_completed(run_id)
    end

    Server.stop(worker_pid)
  end

  defp receive_started do
    assert_receive {:task_started, handler, _}, 5_000
    handler
  end

  defp start_worker(flow_module, task_supervisor, opts) do
    pid =
      start_supervised!(%{
        id: {Server, make_ref()},
        start: {Server, :start_link, [worker_config(flow_module, task_supervisor, opts)]},
        restart: :temporary
      })

    Sandbox.allow(TestRepo, self(), pid)
    pid
  end

  defp assert_run_completed(run_id) do
    :ok = wait_for_run_completion(TestRepo, run_id, timeout: 10_000)
    assert {:ok, %{status: "completed"}} = Queries.Flows.get_run(TestRepo, run_id)
  end
end
