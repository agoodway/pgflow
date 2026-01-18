defmodule PgFlow.Worker.PollerTest do
  @moduledoc """
  Unit tests for PgFlow.Worker.Poller helper functions.

  Tests the pure functions that parse and transform message data,
  without requiring database access.
  """
  use ExUnit.Case, async: true

  alias PgFlow.Worker.Poller

  describe "extract_message_tuples/1" do
    # Helper to build pgmq message format: [msg_id, read_ct, enqueued_at, vt, message_map]
    defp pgmq_message(msg_id, flow_slug, run_id, step_slug, task_index \\ 0) do
      [
        msg_id,
        1,
        ~U[2026-01-17 00:00:00Z],
        ~U[2026-01-17 00:00:30Z],
        %{
          "flow_slug" => flow_slug,
          "run_id" => run_id,
          "step_slug" => step_slug,
          "task_index" => task_index
        }
      ]
    end

    test "extracts tuples from valid message rows" do
      messages = [
        pgmq_message(1, "flow_a", "run-uuid-1", "step1"),
        pgmq_message(2, "flow_a", "run-uuid-2", "step2"),
        pgmq_message(3, "flow_b", "run-uuid-3", "step1")
      ]

      {:ok, tuples} = Poller.extract_message_tuples(messages)

      assert length(tuples) == 3
      assert {"flow_a", "run-uuid-1", "step1", 1} in tuples
      assert {"flow_a", "run-uuid-2", "step2", 2} in tuples
      assert {"flow_b", "run-uuid-3", "step1", 3} in tuples
    end

    test "returns empty list for empty messages" do
      {:ok, tuples} = Poller.extract_message_tuples([])

      assert tuples == []
    end

    test "filters out invalid message formats" do
      messages = [
        pgmq_message(1, "flow_a", "run-uuid-1", "step1"),
        ["invalid", "format"],
        pgmq_message(2, "flow_a", "run-uuid-2", "step2")
      ]

      {:ok, tuples} = Poller.extract_message_tuples(messages)

      # Should have 2 valid tuples, invalid one filtered out
      assert length(tuples) == 2
      assert {"flow_a", "run-uuid-1", "step1", 1} in tuples
      assert {"flow_a", "run-uuid-2", "step2", 2} in tuples
    end

    test "handles messages with different flow slugs" do
      messages = [
        pgmq_message(1, "flow_x", "run-1", "step_a"),
        pgmq_message(2, "flow_y", "run-2", "step_b"),
        pgmq_message(3, "flow_z", "run-3", "step_c")
      ]

      {:ok, tuples} = Poller.extract_message_tuples(messages)

      flow_slugs = Enum.map(tuples, fn {flow_slug, _, _, _} -> flow_slug end)
      assert flow_slugs == ["flow_x", "flow_y", "flow_z"]
    end
  end

  describe "merge_task_details/2" do
    # Pre-define atoms used in tests (required for String.to_existing_atom)
    @step_atoms [:process, :transform, :step1, :step2, :map_step, :step, :my_step_name]

    test "merges message tuples with task rows" do
      # Ensure atoms exist
      _ = @step_atoms

      message_tuples = [
        {"my_flow", "run-uuid-1", "process", 1},
        {"my_flow", "run-uuid-2", "transform", 2}
      ]

      # Task row format from pgflow.start_tasks:
      # [flow_slug, run_id, step_slug, input (deps), msg_id, task_index, flow_input]
      task_rows = [
        ["my_flow", "run-uuid-1", "process", "{}", 1, 0, "{\"key\": \"value1\"}"],
        ["my_flow", "run-uuid-2", "transform", "{}", 2, 0, "{\"key\": \"value2\"}"]
      ]

      {:ok, tasks} = Poller.merge_task_details(message_tuples, task_rows)

      assert length(tasks) == 2

      task1 = Enum.find(tasks, &(&1.run_id == "run-uuid-1"))
      assert task1.flow_slug == "my_flow"
      assert task1.step_slug == :process
      assert task1.task_index == 0
      assert task1.input == %{"run" => %{"key" => "value1"}}

      task2 = Enum.find(tasks, &(&1.run_id == "run-uuid-2"))
      assert task2.flow_slug == "my_flow"
      assert task2.step_slug == :transform
      assert task2.task_index == 0
      assert task2.input == %{"run" => %{"key" => "value2"}}
    end

    test "handles empty inputs" do
      {:ok, tasks} = Poller.merge_task_details([], [])
      assert tasks == []
    end

    test "filters out tasks without matching messages" do
      _ = @step_atoms

      message_tuples = [
        {"my_flow", "run-uuid-1", "step1", 1}
      ]

      # Task row with msg_id=999 has no matching message
      # Format: [flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input]
      task_rows = [
        ["my_flow", "run-uuid-1", "step1", "{}", 1, 0, "{}"],
        ["my_flow", "run-uuid-2", "step2", "{}", 999, 0, "{}"]
      ]

      {:ok, tasks} = Poller.merge_task_details(message_tuples, task_rows)

      # Only the task with matching message should be included
      assert length(tasks) == 1
      assert hd(tasks).run_id == "run-uuid-1"
    end

    test "preserves task_index from task rows" do
      _ = @step_atoms

      message_tuples = [
        {"flow", "run-1", "map_step", 1},
        {"flow", "run-1", "map_step", 2},
        {"flow", "run-1", "map_step", 3}
      ]

      # Format: [flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input]
      task_rows = [
        ["flow", "run-1", "map_step", "{}", 1, 0, "{\"item\": 1}"],
        ["flow", "run-1", "map_step", "{}", 2, 1, "{\"item\": 2}"],
        ["flow", "run-1", "map_step", "{}", 3, 2, "{\"item\": 3}"]
      ]

      {:ok, tasks} = Poller.merge_task_details(message_tuples, task_rows)

      indices = Enum.map(tasks, & &1.task_index) |> Enum.sort()
      assert indices == [0, 1, 2]
    end

    test "decodes JSON flow_input correctly" do
      _ = @step_atoms

      message_tuples = [{"flow", "run-1", "step", 1}]

      # Format: [flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input]
      task_rows = [
        [
          "flow",
          "run-1",
          "step",
          "{}",
          1,
          0,
          "{\"nested\": {\"value\": 42}, \"array\": [1, 2, 3]}"
        ]
      ]

      {:ok, [task]} = Poller.merge_task_details(message_tuples, task_rows)

      assert task.input == %{"run" => %{"nested" => %{"value" => 42}, "array" => [1, 2, 3]}}
    end

    test "converts step_slug to atom" do
      _ = @step_atoms

      message_tuples = [{"flow", "run-1", "my_step_name", 1}]
      # Format: [flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input]
      task_rows = [["flow", "run-1", "my_step_name", "{}", 1, 0, "{}"]]

      {:ok, [task]} = Poller.merge_task_details(message_tuples, task_rows)

      assert task.step_slug == :my_step_name
      assert is_atom(task.step_slug)
    end
  end

  describe "claim_tasks/3" do
    test "returns empty list for empty message tuples" do
      # This doesn't need a real repo since it short-circuits
      {:ok, tasks} = Poller.claim_tasks(nil, [], [])
      assert tasks == []
    end
  end
end
