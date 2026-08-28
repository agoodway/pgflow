defmodule PgFlowDashboard.Live.CronPresentationTest do
  use ExUnit.Case, async: true

  alias PgFlow.CronSummary
  alias PgFlowDashboard.Live.CronPresentation

  test "adds a human schedule to a typed cron summary" do
    cron = cron_summary("0 8 * * 1-5")

    assert %CronPresentation{cron: ^cron, human_schedule: "Daily at 8:00 Weekdays"} =
             CronPresentation.present(cron)
  end

  test "falls back to nil for a schedule it cannot parse" do
    assert %CronPresentation{human_schedule: nil} =
             CronPresentation.present(cron_summary("not a cron"))
  end

  defp cron_summary(expression) do
    CronSummary.new(%{
      flow_slug: "daily_report",
      flow_type: "job",
      cron_expression: expression,
      is_active: true,
      opt_max_attempts: 3,
      opt_base_delay: 1,
      opt_timeout: 60,
      total_runs_24h: 0,
      completed_runs_24h: 0,
      failed_runs_24h: 0,
      success_rate_24h: Decimal.new(0),
      avg_duration_ms: Decimal.new(0),
      p95_duration_ms: 0.0,
      last_run_at: nil,
      last_run_status: nil,
      next_run_at: nil
    })
  end
end
