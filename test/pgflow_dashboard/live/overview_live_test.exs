defmodule PgFlowDashboard.Live.OverviewLiveTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias PgFlow.{OverviewMetrics, RunSummary, WorkerSummary}
  alias PgFlowDashboard.Live.OverviewLive

  test "labels the healthy worker metric accurately" do
    html = render_overview()

    assert html =~ "Healthy Workers"
    refute html =~ "Active Workers"
  end

  test "renders worker health as text instead of color alone" do
    html = render_overview()

    assert html =~ "Stale"
    assert html =~ "bg-amber-100"
  end

  test "uses readable tabular styling for run percentages" do
    html = render_overview()

    assert html =~ ~s(class="text-xs text-slate-500 dark:text-slate-400 tabular-nums")
    assert html =~ "100.0%"
  end

  defp render_overview do
    render_component(&OverviewLive.render/1,
      base_path: "/pgflow",
      time_zone: "UTC",
      metrics:
        OverviewMetrics.new(%{
          active_workers: 2,
          healthy_workers: 1,
          stale_workers: 1,
          total_runs_24h: 1,
          running_runs: 0,
          completed_runs_24h: 1,
          failed_runs_24h: 0,
          avg_duration_ms: Decimal.new(42),
          queue_depth: 0
        }),
      workers: [
        WorkerSummary.new(%{
          worker_id: "58ee65ee-812a-4ac0-8ea8-6f45031f5065",
          flow_slug: "onboarding_flow",
          flow_type: "flow",
          health_status: "stale",
          last_heartbeat_at: ~U[2026-08-28 12:00:00.000000Z],
          active_tasks: 0,
          completed_tasks_24h: 1
        })
      ],
      recent_runs: [
        RunSummary.new(%{
          run_id: "f53bea39-3df7-4020-9047-80ddd19109d0",
          flow_slug: "onboarding_flow",
          flow_type: "flow",
          status: "completed",
          input: %{},
          output: %{},
          started_at: ~U[2026-08-28 12:00:00.000000Z],
          completed_at: ~U[2026-08-28 12:00:00.042000Z],
          duration_ms: Decimal.new(42),
          total_steps: 1,
          completed_steps: 1,
          failed_steps: 0,
          skipped_steps: 0,
          progress_percent: Decimal.new(100)
        })
      ]
    )
  end
end
