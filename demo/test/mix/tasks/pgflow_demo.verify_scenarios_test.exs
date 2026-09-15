defmodule Mix.Tasks.PgflowDemo.VerifyScenariosTest do
  use PgflowDemo.ScenarioCase, async: false

  alias PgflowDemo.VerifyScenarios

  @moduletag :integration
  @moduletag :integration_sandbox

  setup_all do
    setup_integration_sandbox()
    :ok
  end

  describe "verify_one/3" do
    test "compares persisted step statuses and outputs" do
      result =
        VerifyScenarios.verify_one("map", "three_items",
          expected: %{
            "run_status" => "completed",
            "step_statuses" => %{
              "generate" => "completed",
              "process_items" => "completed",
              "aggregate" => "completed"
            },
            "outputs" => %{"aggregate" => %{"total" => 12}}
          }
        )

      assert result.pass

      refute VerifyScenarios.verify_one("map", "three_items",
               expected: %{"outputs" => %{"aggregate" => %{"total" => 999}}}
             ).pass
    end

    test "timeout preset exercises a failed handler" do
      result = VerifyScenarios.verify_one("timeout", "slow_path")
      assert result.pass
      assert result.actual["step_statuses"]["slow"] == "failed"
    end

    test "passes when persisted outcome matches catalogue expectation" do
      result = VerifyScenarios.verify_one("retry", "succeed_on_third_attempt")

      assert result.pass
      assert result.expected["run_status"] == "completed"
      assert result.actual["run_status"] == "completed"
      assert result.actual["attempts_count"] == 3
    end

    test "fails when expected outcome is intentionally wrong" do
      result =
        VerifyScenarios.verify_one("retry", "succeed_on_third_attempt",
          expected: %{"run_status" => "failed"}
        )

      refute result.pass
      assert result.actual["run_status"] == "completed"
      assert result.message =~ "run_status"
    end
  end

  describe "run/1" do
    test "passes all executable presets and skips walkthrough scenarios" do
      run_count_before = run_count()

      report = VerifyScenarios.run()

      assert report.pass
      assert report.run_history_preserved
      assert run_count() >= run_count_before + report.executed_count
      assert report.executed_count > 0
      assert Enum.all?(report.results, & &1.pass)

      walkthrough_ids = report.walkthrough |> Enum.map(& &1.scenario_id) |> MapSet.new()
      assert "article" in walkthrough_ids
      assert "observability" in walkthrough_ids
    end

    test "reports llm article scenario separately when opted in without network" do
      report = VerifyScenarios.run(include_llm: true, llm_configured?: false)

      assert report.llm.status == :skipped
      assert report.llm.reason =~ "not configured"
    end
  end

  @tag :notify_signal
  test "notify signal strategy requires a separate application boot" do
    if System.get_env("PGFLOW_DEMO_SIGNAL") == "notify" do
      assert Application.get_env(:pgflow_demo, :signal_strategy) == :notify
      assert :ok = PgFlow.FlowStarter.await_ready(:infinity)
    else
      assert Application.get_env(:pgflow_demo, :signal_strategy) == :polling
    end
  end

  defp run_count do
    %{rows: [[count]]} = Repo.query!("SELECT count(*) FROM pgflow.runs")
    count
  end
end
