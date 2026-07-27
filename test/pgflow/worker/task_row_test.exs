defmodule PgFlow.Worker.TaskRowTest do
  use ExUnit.Case, async: true

  alias PgFlow.Worker.TaskRow

  # A `pgflow.step_task_record` as Postgrex hands it back, minus the trailing
  # attempts_count that pgflow_helpers v03 adds.
  defp v02_row do
    ["my_flow", <<0::128>>, "my_step", %{"a" => 1}, 42, 0, %{"in" => true}]
  end

  describe "decode/1" do
    test "maps the seven base columns onto named fields" do
      assert %{
               flow_slug: "my_flow",
               run_id: <<0::128>>,
               step_slug: "my_step",
               input: %{"a" => 1},
               msg_id: 42,
               task_index: 0,
               flow_input: %{"in" => true}
             } = TaskRow.decode(v02_row())
    end

    test "carries the attempt through from an eight-column row" do
      assert %{attempt: 3} = TaskRow.decode(v02_row() ++ [3])
    end

    test "a pre-v03 seven-column row reports attempt 1" do
      assert %{attempt: 1} = TaskRow.decode(v02_row())
    end

    test "a null attempts_count reports attempt 1 rather than nil" do
      assert %{attempt: 1} = TaskRow.decode(v02_row() ++ [nil])
    end

    test "the first attempt is 1, matching the documented 1-indexing" do
      assert %{attempt: 1} = TaskRow.decode(v02_row() ++ [1])
    end
  end
end
