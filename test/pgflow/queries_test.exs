defmodule PgFlow.QueriesTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.TestRepo
  alias PgFlow.Queries

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

  defmodule SimpleFlow do
    use PgFlow.Flow

    @flow slug: :simple_query_flow, max_attempts: 3

    step :process do
      fn input, _ctx ->
        %{result: input["value"]}
      end
    end
  end

  defmodule TwoStepFlow do
    use PgFlow.Flow

    @flow slug: :two_step_query_flow, max_attempts: 3

    step :first do
      fn input, _ctx ->
        %{first_result: input["value"]}
      end
    end

    step :second, depends_on: [:first] do
      fn deps, _ctx ->
        %{second_result: deps.first["first_result"] * 2}
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

  defp register_worker(flow_slug) do
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Queries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
    worker_id
  end

  defp read_and_start_tasks(flow_slug, worker_id) do
    {:ok, messages} =
      Queries.read_with_poll(TestRepo, flow_slug, 30, 10,
        max_poll_seconds: 1,
        poll_interval_ms: 100
      )

    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    {:ok, task_details} = Queries.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)
    {messages, task_details}
  end

  # ── get_step_output ────────────────────────────────────────────────

  describe "get_step_output/3" do
    test "returns output after task completion" do
      flow_slug = compile_flow(SimpleFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      worker_id = register_worker(flow_slug)
      {_messages, _task_details} = read_and_start_tasks(flow_slug, worker_id)

      output = %{"result" => 42}
      {:ok, _} = Queries.complete_task(TestRepo, run_id, "process", 0, output)

      {:ok, step_output} = Queries.get_step_output(TestRepo, run_id, "process")
      assert step_output == output
    end

    test "returns nil for step with no output yet" do
      flow_slug = compile_flow(SimpleFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      {:ok, step_output} = Queries.get_step_output(TestRepo, run_id, "process")
      assert step_output == nil
    end

    test "returns nil for nonexistent step" do
      flow_slug = compile_flow(SimpleFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      {:ok, step_output} = Queries.get_step_output(TestRepo, run_id, "nonexistent")
      assert step_output == nil
    end
  end

  # ── start_flow ─────────────────────────────────────────────────────

  describe "start_flow/3" do
    test "returns {:ok, run_id} as UUID string" do
      flow_slug = compile_flow(SimpleFlow)
      {:ok, run_id} = Queries.start_flow(TestRepo, flow_slug, %{"value" => 1})

      assert is_binary(run_id)
      assert {:ok, _} = Ecto.UUID.cast(run_id)
    end

    test "returns error for nonexistent flow slug" do
      result = Queries.start_flow(TestRepo, "nonexistent_flow", %{"value" => 1})
      assert {:error, _} = result
    end
  end

  # ── flow_exists? ───────────────────────────────────────────────────

  describe "flow_exists?/2" do
    test "returns {:ok, true} for existing flow" do
      flow_slug = compile_flow(SimpleFlow)
      assert {:ok, true} = Queries.flow_exists?(TestRepo, flow_slug)
    end

    test "returns {:ok, false} for nonexistent flow" do
      assert {:ok, false} = Queries.flow_exists?(TestRepo, "nonexistent_flow")
    end
  end

  # ── get_flow_input ─────────────────────────────────────────────────

  describe "get_flow_input/2" do
    test "returns input data for existing run" do
      flow_slug = compile_flow(SimpleFlow)
      input = %{"value" => 42, "nested" => %{"key" => "data"}}
      run_id = start_flow_run(flow_slug, input)

      assert {:ok, returned_input} = Queries.get_flow_input(TestRepo, run_id)
      assert returned_input == input
    end

    test "returns {:error, :not_found} for nonexistent run" do
      _flow_slug = compile_flow(SimpleFlow)
      nonexistent_id = Ecto.UUID.generate()

      assert {:error, :not_found} = Queries.get_flow_input(TestRepo, nonexistent_id)
    end
  end

  # ── read_with_poll ─────────────────────────────────────────────────

  describe "read_with_poll/5" do
    test "returns messages after starting a flow" do
      flow_slug = compile_flow(SimpleFlow)
      _run_id = start_flow_run(flow_slug, %{"value" => 42})

      {:ok, messages} =
        Queries.read_with_poll(TestRepo, flow_slug, 30, 10,
          max_poll_seconds: 1,
          poll_interval_ms: 100
        )

      assert messages != []
      [msg_id | _] = hd(messages)
      assert is_integer(msg_id)
    end

    test "returns empty list when queue is empty" do
      flow_slug = compile_flow(SimpleFlow)
      # Don't start a run — queue should be empty

      {:ok, messages} =
        Queries.read_with_poll(TestRepo, flow_slug, 30, 10,
          max_poll_seconds: 1,
          poll_interval_ms: 100
        )

      assert messages == []
    end
  end

  # ── start_tasks ────────────────────────────────────────────────────

  describe "start_tasks/4" do
    test "returns task details for valid msg_ids" do
      flow_slug = compile_flow(SimpleFlow)
      _run_id = start_flow_run(flow_slug, %{"value" => 42})

      worker_id = register_worker(flow_slug)

      {:ok, messages} =
        Queries.read_with_poll(TestRepo, flow_slug, 30, 10,
          max_poll_seconds: 1,
          poll_interval_ms: 100
        )

      msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
      {:ok, task_details} = Queries.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)

      assert task_details != []
    end
  end

  # ── complete_task ──────────────────────────────────────────────────

  describe "complete_task/5" do
    test "marks task as completed with output" do
      flow_slug = compile_flow(SimpleFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      worker_id = register_worker(flow_slug)
      {_messages, _task_details} = read_and_start_tasks(flow_slug, worker_id)

      output = %{"result" => 42}
      assert {:ok, _} = Queries.complete_task(TestRepo, run_id, "process", 0, output)
    end

    test "run status updates after completing all tasks" do
      flow_slug = compile_flow(SimpleFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      worker_id = register_worker(flow_slug)
      {_messages, _task_details} = read_and_start_tasks(flow_slug, worker_id)

      output = %{"result" => 42}
      {:ok, _} = Queries.complete_task(TestRepo, run_id, "process", 0, output)

      # Check run status via direct SQL
      %{rows: [[status]]} =
        TestRepo.query!(
          "SELECT status FROM pgflow.runs WHERE run_id = $1",
          [Ecto.UUID.dump!(run_id)]
        )

      assert status == "completed"
    end
  end

  # ── fail_task ──────────────────────────────────────────────────────

  describe "fail_task/5" do
    test "marks task as failed with error message" do
      flow_slug = compile_flow(SimpleFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      worker_id = register_worker(flow_slug)
      {_messages, _task_details} = read_and_start_tasks(flow_slug, worker_id)

      assert {:ok, _} = Queries.fail_task(TestRepo, run_id, "process", 0, "Something went wrong")
    end
  end

  # ── register_worker ────────────────────────────────────────────────

  describe "register_worker/4" do
    test "registers a new worker" do
      flow_slug = compile_flow(SimpleFlow)
      worker_id = Ecto.UUID.generate()

      assert {:ok, nil} = Queries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
    end

    test "upserts on conflict (call twice, no error)" do
      flow_slug = compile_flow(SimpleFlow)
      worker_id = Ecto.UUID.generate()

      assert {:ok, nil} = Queries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
      assert {:ok, nil} = Queries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
    end
  end

  # ── mark_worker_stopped ────────────────────────────────────────────

  describe "mark_worker_stopped/2" do
    test "sets stopped_at timestamp" do
      flow_slug = compile_flow(SimpleFlow)
      worker_id = Ecto.UUID.generate()
      {:ok, nil} = Queries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")

      assert {:ok, nil} = Queries.mark_worker_stopped(TestRepo, worker_id)

      # Verify stopped_at is set
      %{rows: [[stopped_at]]} =
        TestRepo.query!(
          "SELECT stopped_at FROM pgflow.workers WHERE worker_id = $1",
          [Ecto.UUID.dump!(worker_id)]
        )

      assert stopped_at != nil
    end
  end

  # ── delete_message ─────────────────────────────────────────────────

  describe "delete_message/3" do
    test "deletes existing message" do
      flow_slug = compile_flow(SimpleFlow)
      _run_id = start_flow_run(flow_slug, %{"value" => 42})

      {:ok, messages} =
        Queries.read_with_poll(TestRepo, flow_slug, 30, 10,
          max_poll_seconds: 1,
          poll_interval_ms: 100
        )

      [msg_id | _] = hd(messages)
      assert {:ok, true} = Queries.delete_message(TestRepo, flow_slug, msg_id)
    end

    test "returns {:ok, false} for nonexistent message" do
      flow_slug = compile_flow(SimpleFlow)
      assert {:ok, false} = Queries.delete_message(TestRepo, flow_slug, 999_999)
    end
  end

  # ── prune_data ─────────────────────────────────────────────────────

  describe "prune_data/3" do
    test "returns {:ok, result} with zero counts when no old data exists" do
      _flow_slug = compile_flow(SimpleFlow)

      assert {:ok, result} = Queries.prune_data(TestRepo, 24)

      assert result == %{
               deleted_runs: 0,
               deleted_step_states: 0,
               deleted_step_tasks: 0,
               deleted_workers: 0
             }
    end

    test "prunes completed runs older than retention period" do
      flow_slug = compile_flow(SimpleFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      worker_id = register_worker(flow_slug)
      {_messages, _task_details} = read_and_start_tasks(flow_slug, worker_id)

      # Complete the task to mark run as completed
      output = %{"result" => 42}
      {:ok, _} = Queries.complete_task(TestRepo, run_id, "process", 0, output)

      # Backdate the run to make it appear older than retention
      # Must also backdate started_at due to completed_at_is_after_started_at constraint
      TestRepo.query!(
        """
        UPDATE pgflow.runs
        SET started_at = NOW() - INTERVAL '49 hours',
            completed_at = NOW() - INTERVAL '48 hours'
        WHERE run_id = $1
        """,
        [Ecto.UUID.dump!(run_id)]
      )

      # Prune with 24 hour retention - should delete the 48-hour-old run
      assert {:ok, result} = Queries.prune_data(TestRepo, 24)
      assert result.deleted_runs == 1
      assert result.deleted_step_states >= 1
      assert result.deleted_step_tasks >= 1
    end

    test "respects flow_slugs filter option" do
      # Create two different flows
      flow_slug1 = compile_flow(SimpleFlow)
      flow_slug2 = compile_flow(TwoStepFlow)

      # Start runs in both flows
      run_id1 = start_flow_run(flow_slug1, %{"value" => 1})
      run_id2 = start_flow_run(flow_slug2, %{"value" => 2})

      # Complete both flows
      worker_id1 = register_worker(flow_slug1)
      {_m1, _t1} = read_and_start_tasks(flow_slug1, worker_id1)
      {:ok, _} = Queries.complete_task(TestRepo, run_id1, "process", 0, %{"result" => 1})

      worker_id2 = register_worker(flow_slug2)
      {_m2, _t2} = read_and_start_tasks(flow_slug2, worker_id2)
      {:ok, _} = Queries.complete_task(TestRepo, run_id2, "first", 0, %{"first_result" => 2})
      # Read and start the second step
      {_m3, _t3} = read_and_start_tasks(flow_slug2, worker_id2)
      {:ok, _} = Queries.complete_task(TestRepo, run_id2, "second", 0, %{"second_result" => 4})

      # Backdate both runs
      # Must also backdate started_at due to completed_at_is_after_started_at constraint
      TestRepo.query!(
        """
        UPDATE pgflow.runs
        SET started_at = NOW() - INTERVAL '49 hours',
            completed_at = NOW() - INTERVAL '48 hours'
        WHERE run_id = ANY($1)
        """,
        [[Ecto.UUID.dump!(run_id1), Ecto.UUID.dump!(run_id2)]]
      )

      # Prune only the first flow
      assert {:ok, result} = Queries.prune_data(TestRepo, 24, flow_slugs: [flow_slug1])
      assert result.deleted_runs == 1

      # Verify the second flow's run still exists
      %{rows: rows} =
        TestRepo.query!(
          "SELECT run_id FROM pgflow.runs WHERE run_id = $1",
          [Ecto.UUID.dump!(run_id2)]
        )

      assert length(rows) == 1
    end

    test "does not prune runs within retention period" do
      flow_slug = compile_flow(SimpleFlow)
      run_id = start_flow_run(flow_slug, %{"value" => 42})

      worker_id = register_worker(flow_slug)
      {_messages, _task_details} = read_and_start_tasks(flow_slug, worker_id)

      # Complete the task
      {:ok, _} = Queries.complete_task(TestRepo, run_id, "process", 0, %{"result" => 42})

      # Don't backdate - run is fresh
      # Prune with 24 hour retention - should NOT delete fresh run
      assert {:ok, result} = Queries.prune_data(TestRepo, 24)
      assert result.deleted_runs == 0
    end
  end

  # ── recover_stalled_tasks ──────────────────────────────────────────

  describe "recover_stalled_tasks/2" do
    test "returns {:ok, 0} when no stalled tasks exist" do
      _flow_slug = compile_flow(SimpleFlow)
      assert {:ok, 0} = Queries.recover_stalled_tasks(TestRepo, 60)
    end

    test "recovers tasks stalled beyond threshold" do
      flow_slug = compile_flow(SimpleFlow)
      _run_id = start_flow_run(flow_slug, %{"value" => 42})

      worker_id = register_worker(flow_slug)
      {_messages, task_details} = read_and_start_tasks(flow_slug, worker_id)

      # Get the task info to backdate it
      [_flow_slug_bin, run_id_bin, step_slug, _input, _msg_id, task_index | _] =
        hd(task_details)

      # Backdate started_at and queued_at to make task appear stalled
      # (DB constraint: started_at must be >= queued_at)
      TestRepo.query!(
        """
        UPDATE pgflow.step_tasks
        SET started_at = NOW() - INTERVAL '120 seconds',
            queued_at = NOW() - INTERVAL '121 seconds'
        WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
        """,
        [run_id_bin, step_slug, task_index]
      )

      # Recover with 60-second threshold
      assert {:ok, count} = Queries.recover_stalled_tasks(TestRepo, 60)
      assert count == 1

      # Verify task is reset to queued
      %{rows: [[status]]} =
        TestRepo.query!(
          "SELECT status FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = $2",
          [run_id_bin, step_slug]
        )

      assert status == "queued"
    end
  end
end
