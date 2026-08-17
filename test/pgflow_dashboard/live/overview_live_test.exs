defmodule PgFlowDashboard.Live.OverviewLiveTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

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
      metrics: %{
        healthy_workers: 1,
        stale_workers: 1,
        running_runs: 0,
        completed_runs_24h: 1,
        failed_runs_24h: 0,
        avg_duration_ms: 42
      },
      workers: [
        %{
          worker_id: "58ee65ee-812a-4ac0-8ea8-6f45031f5065",
          flow_slug: "onboarding_flow",
          flow_type: "flow",
          health_status: "stale",
          active_tasks: 0
        }
      ],
      recent_runs: [
        %{
          run_id: "f53bea39-3df7-4020-9047-80ddd19109d0",
          flow_slug: "onboarding_flow",
          flow_type: "flow",
          status: "completed",
          duration_ms: 42,
          progress_percent: 100
        }
      ]
    )
  end
end
