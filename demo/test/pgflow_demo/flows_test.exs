defmodule PgflowDemo.FlowsTest do
  use PgflowDemo.DataCase, async: true

  import Ecto.Query

  alias PgFlow.Schema.StepState
  alias PgflowDemo.Flows
  alias PgflowDemo.Repo

  describe "get_step_output/2" do
    test "returns JSON output from the matching persisted step state" do
      {:ok, run_id} =
        PgFlow.Client.start_flow(:article_flow, %{"url" => "https://example.com/article"})

      output = [%{"title" => "First"}, %{"title" => "Second"}]

      StepState
      |> where([state], state.run_id == ^run_id and state.step_slug == "fetch_article")
      |> Repo.update_all(
        set: [
          status: "completed",
          completed_at: DateTime.utc_now(),
          remaining_tasks: 0,
          output: output
        ]
      )

      assert Flows.get_step_output(run_id, "fetch_article") == output
      assert Flows.get_step_output(run_id, "missing") == nil
    end

    test "returns nil for an invalid run ID" do
      assert Flows.get_step_output("not-a-uuid", "fetch_article") == nil
    end
  end
end
