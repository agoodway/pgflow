defmodule PgFlow.Worker.StalledTaskRecoveryTest do
  @moduledoc """
  Tests for PgFlow.Worker.StalledTaskRecovery.

  Verifies that:
  - Stalled tasks (stuck in 'started' status) are recovered to 'queued'
  - The periodic timer fires correctly
  - The recovery query works against real data
  """
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.TestRepo
  alias PgFlow.Queries.Flows
  alias PgFlow.Queries.Workers, as: WorkerQueries
  alias PgFlow.Worker.StalledTaskRecovery

  @moduletag timeout: 30_000
  @moduletag :integration

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    on_exit(fn ->
      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  # Test flow module for stalled task tests
  defmodule StalledFlow do
    use PgFlow.Flow

    @flow slug: :stalled_flow, max_attempts: 3

    step :process do
      fn input, _ctx ->
        %{result: input["value"]}
      end
    end
  end

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

  defp start_flow_run(flow_slug, input) do
    %{rows: [[result]]} =
      TestRepo.query!(
        "SELECT pgflow.start_flow($1, cast($2 as text)::jsonb)",
        [flow_slug, Jason.encode!(input)]
      )

    case result do
      {run_id, _, _, _, _, _, _, _, _} ->
        Ecto.UUID.load!(run_id)

      _ ->
        raise "Unexpected result: #{inspect(result)}"
    end
  end

  describe "recover_stalled_tasks/2" do
    test "resets tasks stuck in 'started' status to 'queued'" do
      flow_slug = compile_flow(StalledFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      # Read messages from the queue to get msg_ids
      {:ok, messages} =
        Flows.read_with_poll(TestRepo, flow_slug, 30, 10,
          max_poll_seconds: 1,
          poll_interval_ms: 100
        )

      assert messages != []

      msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
      worker_id = Ecto.UUID.generate()

      # Register the worker first (FK constraint requires it)
      {:ok, _} = WorkerQueries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")

      # Call start_tasks to transition to 'started' status
      {:ok, _task_details} = Flows.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)

      # Verify task is in 'started' status
      {:ok, run_id_bin} = Ecto.UUID.dump(run_id)

      %{rows: [[status]]} =
        TestRepo.query!(
          "SELECT status FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = 'process'",
          [run_id_bin]
        )

      assert status == "started"

      # Backdate both queued_at and started_at to simulate a stalled task (2 minutes ago)
      # Must backdate queued_at too because of started_at_is_after_queued_at constraint
      TestRepo.query!(
        "UPDATE pgflow.step_tasks SET queued_at = NOW() - interval '3 minutes', started_at = NOW() - interval '2 minutes' WHERE run_id = $1",
        [run_id_bin]
      )

      # Recover with 60s threshold
      {:ok, count} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert count == 1

      # Verify task is back to 'queued'
      %{rows: [[new_status, started_at, last_worker_id]]} =
        TestRepo.query!(
          "SELECT status, started_at, last_worker_id FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = 'process'",
          [run_id_bin]
        )

      assert new_status == "queued"
      assert started_at == nil
      assert last_worker_id == nil
    end

    test "does not recover tasks within threshold" do
      flow_slug = compile_flow(StalledFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      {:ok, messages} =
        Flows.read_with_poll(TestRepo, flow_slug, 30, 10,
          max_poll_seconds: 1,
          poll_interval_ms: 100
        )

      msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
      worker_id = Ecto.UUID.generate()

      {:ok, _} = WorkerQueries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
      {:ok, _} = Flows.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)

      # Do NOT backdate — task was just started
      {:ok, count} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert count == 0

      # Verify task is still 'started'
      {:ok, run_id_bin} = Ecto.UUID.dump(run_id)

      %{rows: [[status]]} =
        TestRepo.query!(
          "SELECT status FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = 'process'",
          [run_id_bin]
        )

      assert status == "started"
    end
  end

  describe "GenServer periodic recovery" do
    test "periodic timer fires and recovers stalled tasks" do
      flow_slug = compile_flow(StalledFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      {:ok, messages} =
        Flows.read_with_poll(TestRepo, flow_slug, 30, 10,
          max_poll_seconds: 1,
          poll_interval_ms: 100
        )

      msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
      worker_id = Ecto.UUID.generate()

      {:ok, _} = WorkerQueries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
      {:ok, _} = Flows.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)

      # Backdate both queued_at and started_at to simulate a stalled task
      {:ok, run_id_bin} = Ecto.UUID.dump(run_id)

      TestRepo.query!(
        "UPDATE pgflow.step_tasks SET queued_at = NOW() - interval '3 minutes', started_at = NOW() - interval '2 minutes' WHERE run_id = $1",
        [run_id_bin]
      )

      # Start the recovery GenServer with a very short interval
      {:ok, pid} =
        StalledTaskRecovery.start_link(
          repo: TestRepo,
          recovery_interval: 100,
          stale_threshold: 60
        )

      # Wait for the timer to fire and recovery query to complete
      Process.sleep(500)

      # Verify recovery happened
      %{rows: [[status]]} =
        TestRepo.query!(
          "SELECT status FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = 'process'",
          [run_id_bin]
        )

      assert status == "queued"

      GenServer.stop(pid)
    end
  end
end
