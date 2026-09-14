defmodule PgFlow.Upstream.JsonContractTest do
  @moduledoc """
  JSON scalar preservation and round-trip contracts for handler output and flow input.
  """
  use PgFlow.IntegrationCase, async: false

  alias PgFlow.Context
  alias PgFlow.Queries.Flows
  alias PgFlow.Queries.Workers, as: WorkerQueries
  alias PgFlow.TestRepo
  alias PgFlow.Worker.{Executor, TaskRow}

  @moduletag :integration

  setup do
    worker_id = Ecto.UUID.generate()
    {:ok, _} = WorkerQueries.register_worker(TestRepo, worker_id, "json_contract", "elixir:test")
    {:ok, worker_id: worker_id}
  end

  describe "handler output JSON round-trip" do
    test "preserves map output", %{worker_id: worker_id} do
      assert_round_trip!(worker_id, "map", %{"key" => "value", "n" => 1})
    end

    test "preserves array output", %{worker_id: worker_id} do
      assert_round_trip!(worker_id, "array", [1, "two", false])
    end

    test "preserves string output", %{worker_id: worker_id} do
      assert_round_trip!(worker_id, "string", "hello")
    end

    test "preserves integer output", %{worker_id: worker_id} do
      assert_round_trip!(worker_id, "integer", 42)
    end

    test "preserves zero and empty string outputs", %{worker_id: worker_id} do
      assert_round_trip!(worker_id, "zero", 0)
      assert_round_trip!(worker_id, "empty_string", "")
    end

    test "preserves fractional number output", %{worker_id: worker_id} do
      assert_round_trip!(worker_id, "fractional", 3.14)
    end

    test "preserves false output", %{worker_id: worker_id} do
      assert_round_trip!(worker_id, "false", false)
    end

    test "preserves nil output", %{worker_id: worker_id} do
      assert_round_trip!(worker_id, "nil", nil)
    end

    test "map step aggregates scalar outputs per task", %{worker_id: worker_id} do
      flow_slug = unique_flow!("map_scalars")

      create_flow(flow_slug)
      add_step(flow_slug, "fanout")
      add_step(flow_slug, "items", deps: ["fanout"], type: "map")

      run_id = start_flow_run(flow_slug, %{})
      complete_with_output!(run_id, flow_slug, worker_id, "fanout", 0, [false, nil, 7])

      for {msg_id, index, expected} <-
            Enum.with_index(task_message_ids(run_id, "items"))
            |> Enum.map(fn {msg_id, index} ->
              {msg_id, index, Enum.at([false, nil, 7], index)}
            end) do
        assert {:ok, [claimed]} =
                 Flows.start_tasks(TestRepo, flow_slug, [msg_id], worker_id, flow_slug)

        _row = TaskRow.decode(claimed)

        assert {:ok, ^expected} =
                 complete_with_output!(
                   run_id,
                   flow_slug,
                   worker_id,
                   "items",
                   index,
                   expected,
                   skip_start: true
                 )
      end

      outputs =
        run_id
        |> get_step_tasks()
        |> Enum.filter(&(&1.step_slug == "items"))
        |> Enum.sort_by(& &1.task_index)
        |> Enum.map(& &1.output)

      assert outputs == [false, nil, 7]
      refute Enum.any?(outputs, &raw_wrapped?/1)

      run = get_run(run_id)
      assert run.status == "completed"
      assert run.output["items"] == [false, nil, 7]
    end
  end

  describe "Context.get_flow_input/1 with JSON scalars" do
    test "preserves zero and empty string flow input", %{worker_id: worker_id} do
      for {label, value} <- [{"zero", 0}, {"empty_string", ""}] do
        flow_slug = unique_flow!(label)
        create_flow(flow_slug)
        add_step(flow_slug, "emit")
        run_id = start_flow_run(flow_slug, value)
        msg_id = task_message_id(run_id, "emit")

        assert {:ok, [claimed]} =
                 Flows.start_tasks(TestRepo, flow_slug, [msg_id], worker_id, flow_slug)

        row = TaskRow.decode(claimed)
        assert row.flow_input === value
        ctx = build_ctx(run_id, row)
        assert Context.get_flow_input(ctx) === value
        assert Context.flow_input_loaded?(ctx)
        loaded = Context.preload_flow_input(%{ctx | flow_input: :not_loaded})
        assert loaded.flow_input === value
      end
    end

    test "returns false flow input without treating it as :not_loaded", %{worker_id: worker_id} do
      flow_slug = unique_flow!("flow_input_false")

      create_flow(flow_slug)
      add_step(flow_slug, "emit")

      run_id = start_flow_run(flow_slug, false)
      msg_id = task_message_id(run_id, "emit")

      assert {:ok, [claimed]} =
               Flows.start_tasks(TestRepo, flow_slug, [msg_id], worker_id, flow_slug)

      row = TaskRow.decode(claimed)
      assert row.flow_input == false

      ctx = build_ctx(run_id, row)
      assert Context.get_flow_input(ctx) == false
      assert Context.flow_input_loaded?(ctx)

      assert {:ok, %{"flow_input" => false}} =
               complete_with_output!(
                 run_id,
                 flow_slug,
                 worker_id,
                 "emit",
                 0,
                 %{
                   "flow_input" => false
                 },
                 skip_start: true
               )
    end

    test "returns nil flow input as JSON null, not :not_loaded", %{worker_id: worker_id} do
      flow_slug = unique_flow!("flow_input_null")

      create_flow(flow_slug)
      add_step(flow_slug, "emit")

      run_id = start_flow_run(flow_slug, nil)
      msg_id = task_message_id(run_id, "emit")

      assert {:ok, [claimed]} =
               Flows.start_tasks(TestRepo, flow_slug, [msg_id], worker_id, flow_slug)

      row = TaskRow.decode(claimed)

      ctx = build_ctx(run_id, row)
      assert Context.get_flow_input(ctx) == nil

      loaded = Context.preload_flow_input(%{ctx | flow_input: :not_loaded})
      assert loaded.flow_input == nil
      assert Context.flow_input_loaded?(loaded)
    end
  end

  describe "invalid handler output" do
    test "fails the task with a stable serialization error", %{worker_id: worker_id} do
      flow_slug = unique_flow!("bad_json")

      create_flow(flow_slug)
      add_step(flow_slug, "bad")

      run_id = start_flow_run(flow_slug, %{})
      msg_id = task_message_id(run_id, "bad")

      assert {:ok, [_claimed]} =
               Flows.start_tasks(TestRepo, flow_slug, [msg_id], worker_id, flow_slug)

      assert {:error, "Handler output is not JSON encodable"} =
               persist_handler_output!(run_id, "bad", 0, %{pid: self()})

      task = task_by_index(run_id, "bad", 0)
      assert task.status in ["failed", "queued"]
      assert task.error_message == "Handler output is not JSON encodable"
      refute raw_wrapped?(task.output)
    end
  end

  defp assert_round_trip!(worker_id, name, value) do
    flow_slug = unique_flow!("scalar_#{name}")

    create_flow(flow_slug)
    add_step(flow_slug, "emit")

    run_id = start_flow_run(flow_slug, %{})
    assert {:ok, ^value} = complete_with_output!(run_id, flow_slug, worker_id, "emit", 0, value)

    task = task_by_index(run_id, "emit", 0)
    assert task.status == "completed"
    assert task.output == value
    refute raw_wrapped?(task.output)

    run = get_run(run_id)
    assert run.status == "completed"
    assert run.output["emit"] == value
    refute raw_wrapped?(run.output["emit"])
  end

  defp complete_with_output!(
         run_id,
         flow_slug,
         worker_id,
         step_slug,
         task_index,
         output,
         opts \\ []
       ) do
    unless Keyword.get(opts, :skip_start, false) do
      msg_id =
        if task_index == 0 and step_slug != "items" do
          task_message_id(run_id, step_slug)
        else
          Enum.at(task_message_ids(run_id, step_slug), task_index)
        end

      assert {:ok, [_claimed]} =
               Flows.start_tasks(TestRepo, flow_slug, [msg_id], worker_id, flow_slug)
    end

    persist_handler_output!(run_id, step_slug, task_index, output)
  end

  defp persist_handler_output!(run_id, step_slug, task_index, output) do
    case Executor.serialize_output(output) do
      {:ok, serialized} ->
        assert {:ok, row} =
                 Flows.complete_task(TestRepo, run_id, step_slug, task_index, serialized)

        assert Flows.step_task_status(row) == "completed"
        {:ok, serialized}

      {:error, reason} = err ->
        assert {:ok, row} =
                 Flows.fail_task(TestRepo, run_id, step_slug, task_index, reason)

        assert Flows.step_task_status(row) in ["failed", "queued"]
        err
    end
  end

  defp unique_flow!(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  defp task_message_id(run_id, step_slug) do
    hd(task_message_ids(run_id, step_slug))
  end

  defp task_message_ids(run_id, step_slug) do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT message_id FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = $2 AND message_id IS NOT NULL
        ORDER BY task_index
        """,
        [ensure_uuid_binary(run_id), step_slug]
      )

    List.flatten(rows)
  end

  defp task_by_index(run_id, step_slug, task_index) do
    %{rows: [[status, output, error_message]]} =
      TestRepo.query!(
        """
        SELECT status, output, error_message
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
        """,
        [ensure_uuid_binary(run_id), step_slug, task_index]
      )

    %{
      status: status,
      output: output,
      error_message: error_message,
      step_slug: step_slug,
      task_index: task_index
    }
  end

  defp build_ctx(run_id, row) do
    %Context{
      run_id: run_id,
      step_slug: :emit,
      task_index: row.task_index,
      attempt: row.attempt,
      repo: TestRepo,
      flow_input: Context.normalize_flow_input(row.flow_input)
    }
  end

  defp raw_wrapped?(output) when is_map(output), do: Map.has_key?(output, "_raw")
  defp raw_wrapped?(_), do: false
end
