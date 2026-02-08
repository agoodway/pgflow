defmodule PgFlow.Telemetry.RunLifecycleTest do
  @moduledoc """
  Integration tests verifying that run lifecycle telemetry events fire at the
  correct times during actual flow execution.

  Tests cover:
  - [:pgflow, :run, :started] emitted by Client.start_flow/2
  - [:pgflow, :run, :completed] emitted by worker after all steps succeed
  - [:pgflow, :run, :failed] emitted by worker after a step permanently fails
  """
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.TestRepo
  alias PgFlow.Client
  alias PgFlow.Worker.Server

  @moduletag timeout: 30_000
  @moduletag :integration

  # ── Flow Modules ──────────────────────────────────────────────────

  defmodule SuccessFlow do
    use PgFlow.Flow
    @flow slug: :telemetry_success_flow, max_attempts: 3

    step :process do
      fn input, _ctx -> %{doubled: input["value"] * 2} end
    end
  end

  defmodule FailFlow do
    use PgFlow.Flow
    @flow slug: :telemetry_fail_flow, max_attempts: 1, base_delay: 1

    step :will_fail do
      fn _input, _ctx -> raise "permanent failure" end
    end
  end

  # ── Setup ─────────────────────────────────────────────────────────

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    :persistent_term.put({PgFlow, :repo}, TestRepo)

    {:ok, task_supervisor} = Task.Supervisor.start_link()

    on_exit(fn ->
      try do
        if Process.alive?(task_supervisor), do: Supervisor.stop(task_supervisor)
      catch
        :exit, _ -> :ok
      end

      :persistent_term.erase({PgFlow, :repo})
      Sandbox.mode(TestRepo, :manual)
    end)

    compile_flow(SuccessFlow)
    compile_flow(FailFlow)

    {:ok, task_supervisor: task_supervisor}
  end

  # ── Tests ─────────────────────────────────────────────────────────

  describe "[:pgflow, :run, :started]" do
    test "emitted when Client.start_flow/2 succeeds" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :run, :started]
        ])

      {:ok, run_id} = Client.start_flow(SuccessFlow, %{"value" => 1})

      assert_received {[:pgflow, :run, :started], ^ref, measurements, metadata}
      assert is_integer(measurements.system_time)
      assert metadata.flow_slug == "telemetry_success_flow"
      assert metadata.run_id == run_id
    end

    test "not emitted when Client.start_flow/2 fails" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :run, :started]
        ])

      {:error, _} = Client.start_flow("nonexistent_flow", %{})

      refute_received {[:pgflow, :run, :started], ^ref, _, _}
    end
  end

  describe "[:pgflow, :run, :completed]" do
    test "emitted when flow completes successfully", %{task_supervisor: task_supervisor} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :run, :completed]
        ])

      worker_pid = start_worker(SuccessFlow, task_supervisor)
      Process.sleep(100)

      {:ok, run_id} = Client.start_flow(SuccessFlow, %{"value" => 21})
      wait_for_run_completion(run_id)

      assert_receive {[:pgflow, :run, :completed], ^ref, measurements, metadata}, 5_000
      assert is_integer(measurements.system_time)
      assert metadata.flow_slug == "telemetry_success_flow"
      assert metadata.run_id == run_id
      assert metadata.output["process"]["doubled"] == 42

      Server.stop(worker_pid)
    end
  end

  describe "[:pgflow, :run, :failed]" do
    test "emitted when flow fails permanently", %{task_supervisor: task_supervisor} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :run, :failed]
        ])

      worker_pid = start_worker(FailFlow, task_supervisor)
      Process.sleep(100)

      {:ok, run_id} = Client.start_flow(FailFlow, %{})
      wait_for_run_completion(run_id, 10_000)

      assert_receive {[:pgflow, :run, :failed], ^ref, measurements, metadata}, 5_000
      assert is_integer(measurements.system_time)
      assert metadata.flow_slug == "telemetry_fail_flow"
      assert metadata.run_id == run_id
      assert is_binary(metadata.error)

      Server.stop(worker_pid)
    end
  end

  # ── Helpers ───────────────────────────────────────────────────────

  defp compile_flow(flow_module) do
    definition = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    max_attempts = definition.opts[:max_attempts] || 3
    base_delay = definition.opts[:base_delay] || 1
    timeout = definition.opts[:timeout] || 30

    TestRepo.query!(
      "SELECT pgflow.create_flow($1, $2, $3, $4)",
      [flow_slug, max_attempts, base_delay, timeout]
    )

    for step <- definition.steps do
      step_slug = Atom.to_string(step.slug)
      deps = Enum.map(step.depends_on, &Atom.to_string/1)
      step_type = Atom.to_string(step.step_type)

      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)",
        [
          flow_slug,
          step_slug,
          deps,
          step.max_attempts,
          step.base_delay,
          step.timeout,
          step.start_delay,
          step_type
        ]
      )
    end

    flow_slug
  end

  defp start_worker(flow_module, task_supervisor) do
    config = %{
      flow_module: flow_module,
      repo: TestRepo,
      task_supervisor: task_supervisor,
      max_concurrency: 10,
      batch_size: 10,
      signal_strategy: :polling,
      min_poll_interval: 50,
      max_poll_interval: 5_000,
      notify_fallback_interval: 30_000
    }

    {:ok, pid} = Server.start_link(config)
    Sandbox.allow(TestRepo, self(), pid)
    pid
  end

  defp wait_for_run_completion(run_id, timeout_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_loop(run_id, deadline)
  end

  defp wait_loop(run_id, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :timeout}
    else
      {:ok, run_id_bin} = Ecto.UUID.dump(run_id)

      %{rows: [[status]]} =
        TestRepo.query!(
          "SELECT status FROM pgflow.runs WHERE run_id = $1",
          [run_id_bin]
        )

      if status in ["completed", "failed"] do
        {:ok, status}
      else
        Process.sleep(50)
        wait_loop(run_id, deadline)
      end
    end
  end
end
