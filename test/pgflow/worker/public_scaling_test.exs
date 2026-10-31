defmodule PgFlow.Worker.PublicScalingTest do
  use ExUnit.Case, async: false

  import PgFlow.Test.WorkerSupervisorSetup

  alias PgFlow.TestFlows.SimpleFlow
  alias PgFlow.TestRepo
  alias PgFlow.WorkerSupervisor

  @moduletag :integration
  @moduletag timeout: 30_000

  setup do
    start_worker_supervisor!(SimpleFlow, %{SimpleFlow => [max_concurrency: 3]})
  end

  describe "scale_worker/2 validating options" do
    test "raises on invalid scale options before lookup" do
      assert_raise ArgumentError, ~r/requires :max_concurrency or :batch_size/, fn ->
        PgFlow.scale_worker(SimpleFlow, [])
      end

      assert_raise ArgumentError, ~r/max_concurrency/, fn ->
        PgFlow.scale_worker(SimpleFlow, max_concurrency: -1)
      end

      assert_raise ArgumentError, ~r/max_concurrency/, fn ->
        PgFlow.scale_worker(SimpleFlow, max_concurrency: 0)
      end

      assert_raise ArgumentError, fn ->
        PgFlow.scale_worker(SimpleFlow, batch_size: nil)
      end

      assert_raise ArgumentError, fn ->
        PgFlow.scale_worker(SimpleFlow, paused: true)
      end

      assert_raise ArgumentError, fn ->
        WorkerSupervisor.scale_worker(SimpleFlow, [])
      end
    end
  end

  describe "check_worker/1" do
    test "checking a worker that isn't running locally returns nil" do
      assert PgFlow.check_worker(SimpleFlow) == nil
    end
  end

  describe "scale_worker/2" do
    test "trying to scale workers that don't exist returns not_found" do
      assert {:error, :not_found} = PgFlow.scale_worker(SimpleFlow, max_concurrency: 2)
    end

    test "scaling workers up" do
      {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow)

      assert {:ok, %{module: SimpleFlow, pid: ^pid, max_concurrency: 6}} =
               PgFlow.scale_worker(SimpleFlow, max_concurrency: 6)

      assert %{
               module: SimpleFlow,
               max_concurrency: 6,
               configured_max_concurrency: 3
             } = PgFlow.check_worker(SimpleFlow)
    end

    test "scaling resets to configuration after a restart" do
      {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow)
      assert {:ok, _} = PgFlow.scale_worker(SimpleFlow, max_concurrency: 6)

      _new_pid = kill_and_await_restart!(SimpleFlow, pid)
      assert %{max_concurrency: 3} = PgFlow.check_worker(SimpleFlow)
    end

    test "worker options leave the persisted flow definition unchanged" do
      flow_slug = Atom.to_string(SimpleFlow.__pgflow_definition__().slug)
      before_flow = fetch_flow_opts(flow_slug)
      before_steps = fetch_steps_opts(flow_slug)

      {:ok, _pid} = WorkerSupervisor.start_worker(SimpleFlow)
      assert {:ok, _} = PgFlow.scale_worker(SimpleFlow, max_concurrency: 6)

      assert fetch_flow_opts(flow_slug) == before_flow
      assert fetch_steps_opts(flow_slug) == before_steps
      assert %{lifecycle: :running} = PgFlow.check_worker(SimpleFlow)
    end
  end

  describe ":timeout option" do
    test "raises on invalid or unknown call options" do
      assert_raise ArgumentError, ~r/timeout/, fn ->
        PgFlow.check_worker(SimpleFlow, timeout: -1)
      end

      assert_raise ArgumentError, fn ->
        PgFlow.check_worker(SimpleFlow, max_concurrency: 2)
      end

      assert_raise ArgumentError, ~r/timeout/, fn ->
        PgFlow.scale_worker(SimpleFlow, max_concurrency: 2, timeout: :soon)
      end

      assert_raise ArgumentError, ~r/requires :max_concurrency or :batch_size/, fn ->
        PgFlow.scale_worker(SimpleFlow, timeout: 1_000)
      end
    end

    test "check and scale accept an explicit timeout" do
      {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow)

      assert %{pid: ^pid, max_concurrency: 3} = PgFlow.check_worker(SimpleFlow, timeout: 1_000)

      assert {:ok, %{pid: ^pid, max_concurrency: 5}} =
               PgFlow.scale_worker(SimpleFlow, max_concurrency: 5, timeout: :infinity)
    end

    test "a timed-out call exits the caller and a timed-out scale still applies" do
      {:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow)
      :ok = :sys.suspend(pid)

      try do
        assert {:timeout, {GenServer, :call, _}} =
                 catch_exit(PgFlow.check_worker(SimpleFlow, timeout: 50))

        assert {:timeout, {GenServer, :call, _}} =
                 catch_exit(PgFlow.scale_worker(SimpleFlow, max_concurrency: 7, timeout: 50))
      after
        :sys.resume(pid)
      end

      assert %{max_concurrency: 7} = PgFlow.check_worker(SimpleFlow)
    end
  end

  defp fetch_flow_opts(flow_slug) do
    %{rows: [[max_attempts, base_delay, timeout]]} =
      TestRepo.query!(
        "SELECT opt_max_attempts, opt_base_delay, opt_timeout FROM pgflow.flows WHERE flow_slug = $1",
        [flow_slug]
      )

    {max_attempts, base_delay, timeout}
  end

  defp fetch_steps_opts(flow_slug) do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT step_slug, opt_max_attempts, opt_base_delay, opt_timeout, opt_start_delay
        FROM pgflow.steps
        WHERE flow_slug = $1
        ORDER BY step_slug
        """,
        [flow_slug]
      )

    rows
  end
end
