defmodule Mix.Tasks.PgflowDemo.VerifyScenarios do
  @moduledoc """
  Runs deterministic demo scenarios against the test-bed database.

  Uses the same `PgflowDemo.ScenarioRunner` as the LiveView UI. Exits non-zero
  when any executable preset fails expectation checks.

  ## Usage

      MIX_ENV=test mix pgflow_demo.verify_scenarios
      MIX_ENV=test mix pgflow_demo.verify_scenarios --include-llm

  Article/LLM integration is opt-in and never required for the baseline gate.
  """
  use Mix.Task

  alias Ecto.Adapters.SQL
  alias PgflowDemo.VerifyScenarios

  @shortdoc "Verify executable demo scenarios against persisted outcomes"

  @impl Mix.Task
  def run(args) do
    unless Mix.env() == :test,
      do:
        Mix.raise("Run scenario verification with MIX_ENV=test against a dedicated test database")

    Mix.Task.run("app.config")
    Mix.Task.run("pgflow_demo.test.setup")
    {:ok, _} = Application.ensure_all_started(:pgflow_demo)

    SQL.Sandbox.mode(PgflowDemo.Repo, :auto)

    include_llm? = "--include-llm" in args

    report =
      VerifyScenarios.run(
        include_llm: include_llm?,
        llm_configured?: llm_configured?()
      )

    VerifyScenarios.print_report(report)

    unless report.pass do
      System.halt(1)
    end
  end

  defp llm_configured? do
    case System.get_env("AI_API_KEY") do
      nil -> false
      "" -> false
      _ -> true
    end
  end
end
