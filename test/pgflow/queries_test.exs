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
  # NOTE: get_flow_input/2 has a known issue where it passes string UUIDs
  # to Postgrex which expects binary UUIDs. Tests added after the source
  # fix lands.

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

      assert length(messages) > 0
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

      assert length(task_details) > 0
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

  # ── recover_stalled_tasks ──────────────────────────────────────────

  describe "recover_stalled_tasks/2" do
    test "returns {:ok, 0} when no stalled tasks exist" do
      _flow_slug = compile_flow(SimpleFlow)
      assert {:ok, 0} = Queries.recover_stalled_tasks(TestRepo, 60)
    end
  end
end
