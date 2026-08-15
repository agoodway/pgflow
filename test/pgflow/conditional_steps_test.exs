defmodule PgFlow.ConditionalStepsTest do
  use PgFlow.IntegrationCase

  @moduletag :integration

  describe "schema" do
    test "steps table has 0.14 condition columns" do
      %{rows: rows} =
        TestRepo.query!("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'pgflow' AND table_name = 'steps'
          AND column_name IN (
            'required_input_pattern', 'forbidden_input_pattern',
            'when_unmet', 'when_exhausted'
          )
        ORDER BY column_name
        """)

      assert Enum.map(rows, &hd/1) == [
               "forbidden_input_pattern",
               "required_input_pattern",
               "when_exhausted",
               "when_unmet"
             ]
    end
  end

  describe "if / when_unmet skip" do
    test "root step is skipped when if pattern is unmet" do
      create_flow("cond_root_skip")

      add_conditional_step("cond_root_skip", "premium_only",
        if: %{"plan" => "premium"},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_root_skip", %{"plan" => "free"})
      states = get_step_states(run_id)

      assert [%{step_slug: "premium_only", status: "skipped", skip_reason: "condition_unmet"}] =
               states

      assert get_run_status(run_id) == "completed"
      assert get_step_tasks(run_id) == []
    end
  end
end
