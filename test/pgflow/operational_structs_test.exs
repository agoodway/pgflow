defmodule PgFlow.OperationalStructsTest do
  use ExUnit.Case, async: true

  alias PgFlow.{
    CronSummary,
    DefinitionSummary,
    OverviewMetrics,
    RunHistoryCell,
    RunSummary,
    WorkerSummary
  }

  @run_id "0198f38e-c50b-73ce-91be-734b712ae391"
  @worker_id "0198f38e-c50b-73ce-91be-734b712ae392"
  @started_at ~U[2026-08-28 12:00:00.000000Z]
  @completed_at ~U[2026-08-28 12:00:02.500000Z]

  describe "new/1" do
    test "constructs a run summary from a complete projection" do
      summary =
        RunSummary.new(%{
          run_id: @run_id,
          flow_slug: "welcome_flow",
          flow_type: "flow",
          status: "completed",
          input: %{"email" => "user@example.com"},
          output: ["sent", "tracked"],
          started_at: @started_at,
          completed_at: nil,
          duration_ms: Decimal.new("2500.125"),
          total_steps: 4,
          completed_steps: 3,
          failed_steps: 0,
          skipped_steps: 1,
          progress_percent: Decimal.new("100.00")
        })

      assert %RunSummary{} = summary
      assert summary.run_id == @run_id
      assert summary.output == ["sent", "tracked"]
      assert summary.completed_at == nil
      assert summary.duration_ms == Decimal.new("2500.125")
      assert summary.skipped_steps == 1
    end

    test "constructs a worker summary from a complete projection" do
      summary =
        WorkerSummary.new(%{
          worker_id: @worker_id,
          flow_slug: "welcome_flow",
          flow_type: "flow",
          last_heartbeat_at: @completed_at,
          health_status: "healthy",
          active_tasks: 2,
          completed_tasks_24h: 19
        })

      assert %WorkerSummary{} = summary
      assert summary.worker_id == @worker_id
      assert summary.last_heartbeat_at == @completed_at
      assert summary.active_tasks == 2
    end

    test "constructs a definition summary from a complete projection" do
      summary =
        DefinitionSummary.new(%{
          flow_slug: "welcome_flow",
          flow_type: "flow",
          opt_max_attempts: 5,
          opt_base_delay: 250,
          opt_timeout: 30_000,
          total_runs_24h: 20,
          completed_runs_24h: 17,
          failed_runs_24h: 3,
          success_rate_24h: Decimal.new("85.00"),
          avg_duration_ms: Decimal.new("1250.50"),
          p95_duration_ms: 2400.75,
          step_count: 4
        })

      assert %DefinitionSummary{} = summary
      assert summary.flow_slug == "welcome_flow"
      assert summary.success_rate_24h == Decimal.new("85.00")
      assert summary.p95_duration_ms == 2400.75
      assert summary.step_count == 4
    end

    test "constructs a cron summary from a complete projection" do
      summary =
        CronSummary.new(%{
          flow_slug: "daily_digest",
          flow_type: "job",
          cron_expression: "0 8 * * *",
          is_active: true,
          opt_max_attempts: 3,
          opt_base_delay: 1000,
          opt_timeout: 60_000,
          total_runs_24h: 1,
          completed_runs_24h: 1,
          failed_runs_24h: 0,
          success_rate_24h: Decimal.new("100.00"),
          avg_duration_ms: Decimal.new("825.25"),
          p95_duration_ms: 825.25,
          last_run_at: nil,
          last_run_status: nil,
          next_run_at: nil
        })

      assert %CronSummary{} = summary
      assert summary.cron_expression == "0 8 * * *"
      assert summary.p95_duration_ms == 825.25
      assert summary.last_run_at == nil
      assert summary.last_run_status == nil
      assert summary.next_run_at == nil
    end

    test "constructs a run history cell from a complete projection" do
      cell =
        RunHistoryCell.new(%{
          run_id: @run_id,
          started_at: @started_at,
          step_slug: nil,
          status: nil,
          duration_ms: nil
        })

      assert %RunHistoryCell{} = cell
      assert cell.run_id == @run_id
      assert cell.started_at == @started_at
      assert cell.step_slug == nil
      assert cell.status == nil
      assert cell.duration_ms == nil
    end

    test "constructs overview metrics from a complete projection" do
      metrics =
        OverviewMetrics.new(%{
          active_workers: 4,
          healthy_workers: 3,
          stale_workers: 1,
          total_runs_24h: 120,
          completed_runs_24h: 110,
          failed_runs_24h: 5,
          running_runs: 5,
          avg_duration_ms: Decimal.new("937.50"),
          queue_depth: 8
        })

      assert %OverviewMetrics{} = metrics
      assert metrics.active_workers == 4
      assert metrics.avg_duration_ms == Decimal.new("937.50")
      assert metrics.queue_depth == 8
    end
  end
end
