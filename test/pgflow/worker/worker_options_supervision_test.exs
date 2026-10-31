defmodule PgFlow.Worker.WorkerOptionsSupervisionTest do
  use ExUnit.Case, async: false

  use PgFlow.Test.WaitHelpers

  import PgFlow.Test.WorkerSupervisorSetup

  alias PgFlow.TestFlows.SimpleFlow
  alias PgFlow.TestRepo
  alias PgFlow.Worker.Server
  alias PgFlow.WorkerSupervisor

  @moduletag :integration
  @moduletag timeout: 30_000

  setup do
    start_worker_supervisor!(SimpleFlow, %{SimpleFlow => [max_concurrency: 3]})
  end

  test "configured worker_options set the worker's limits" do
    {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow)

    assert %{
             max_concurrency: 3,
             batch_size: 8,
             configured_max_concurrency: 3,
             configured_batch_size: 8
           } = Server.check(pid)
  end

  test "start_worker opts override worker_options" do
    {:ok, pid} =
      WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 2, batch_size: 1)

    assert %{max_concurrency: 2, batch_size: 1} = Server.check(pid)
  end

  test "a crashed worker restarts with its resolved options" do
    {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 2)
    Process.exit(pid, :kill)

    wait_until(fn ->
      case WorkerSupervisor.find_worker(SimpleFlow) do
        new_pid when is_pid(new_pid) and new_pid != pid -> true
        _ -> false
      end
    end)

    new_pid = WorkerSupervisor.find_worker(SimpleFlow)
    assert %{max_concurrency: 2} = Server.check(new_pid)
  end

  test "restarting an existing worker with the same or no options returns it" do
    {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 2)
    assert {:ok, ^pid} = WorkerSupervisor.start_worker(SimpleFlow)
    assert {:ok, ^pid} = WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 2)
  end

  test "restarting an existing worker with different options is rejected" do
    {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 2)

    assert {:error, {:conflicting_worker_options, ^pid}} =
             WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 5)
  end

  test "only supplied keys are compared for partial start options" do
    {:ok, pid} =
      WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 2, batch_size: 1)

    assert {:ok, ^pid} = WorkerSupervisor.start_worker(SimpleFlow, batch_size: 1)
  end

  test "only supplied keys are compared after runtime scale uses configured values" do
    {:ok, pid} =
      WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 2, batch_size: 1)

    {:ok, _} = Server.scale(pid, max_concurrency: 6)
    assert {:ok, ^pid} = WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 2)
  end

  test "concurrent first starts create one worker" do
    parent = self()
    count = 5

    tasks =
      for i <- 1..count do
        spawn(fn ->
          receive do
            :go -> :ok
          end

          result = WorkerSupervisor.start_worker(SimpleFlow)
          send(parent, {:result, i, result})
        end)
      end

    Enum.each(tasks, fn task -> send(task, :go) end)

    results =
      for _ <- 1..count do
        assert_receive {:result, _i, result}, 5_000
        result
      end

    assert Enum.all?(results, fn
             {:ok, pid} when is_pid(pid) -> true
             _ -> false
           end)

    pids = Enum.map(results, fn {:ok, pid} -> pid end)
    assert Enum.uniq(pids) == [hd(pids)]
    assert DynamicSupervisor.count_children(WorkerSupervisor).active == 1
  end

  test "start_worker_process ignores a start while a live worker is registered" do
    {:ok, _pid} = WorkerSupervisor.start_worker(SimpleFlow)

    assert :ignore = WorkerSupervisor.start_worker_process(SimpleFlow, TestRepo, [])
    assert DynamicSupervisor.count_children(WorkerSupervisor).active == 1
  end

  test "check_worker and scale_worker treat a worker killed mid-call as gone" do
    {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow)
    parent = self()
    :ok = :sys.suspend(pid)

    caller_check =
      spawn(fn -> send(parent, {:check, PgFlow.check_worker(SimpleFlow)}) end)

    caller_scale =
      spawn(fn ->
        send(parent, {:scale, PgFlow.scale_worker(SimpleFlow, max_concurrency: 2)})
      end)

    # Both calls are queued in the suspended worker's mailbox before it is killed.
    wait_until(fn ->
      {:messages, messages} = Process.info(pid, :messages)
      length(messages) >= 2
    end)

    Process.exit(pid, :kill)

    assert_receive {:check, check_result}, 5_000
    assert check_result == nil or (is_map(check_result) and check_result.pid != pid)

    assert_receive {:scale, scale_result}, 5_000

    assert scale_result == {:error, :not_found} or
             match?({:ok, %{pid: new_pid}} when new_pid != pid, scale_result)

    refute Process.alive?(caller_check)
    refute Process.alive?(caller_scale)
  end

  test "invalid start options raise" do
    assert_raise ArgumentError, fn ->
      WorkerSupervisor.start_worker(SimpleFlow, max_concurrency: 0)
    end

    assert_raise ArgumentError, fn ->
      WorkerSupervisor.start_worker(SimpleFlow, max_concurency: 4)
    end
  end

  test "repo: still works" do
    assert {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow, repo: TestRepo)
    assert is_pid(pid)
  end
end
