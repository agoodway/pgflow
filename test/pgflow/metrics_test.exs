defmodule PgFlow.MetricsTest do
  use PgFlow.IntegrationCase

  alias PgFlow.{Metrics, OverviewMetrics}

  describe "overview/2" do
    test "returns every typed overview metric with explicit operational windows" do
      now = ~U[2026-08-28 12:00:00.000000Z]
      TestRepo.query!("DELETE FROM pgflow.workers")
      create_flow("metrics_test_flow")
      add_step("metrics_test_flow", "work")

      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", now, 5)
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2", now, 45)
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3", now, 360)
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa4", now, 30)
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa5", now, 60)
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa6", now, 300)
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa7", now, 5, stopped: true)
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa8", now, 5, deprecated: true)

      insert_run("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1", "completed", now, 1, 120)
      insert_run("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2", "failed", now, 2, 30)
      insert_run("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb3", "started", now, 3, nil)
      insert_run("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb4", "completed", now, 25, 60)
      insert_run("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb5", "completed", now, 24, 60)

      queued_run_id = start_flow_run("metrics_test_flow", %{"queued" => true})

      TestRepo.query!("UPDATE pgflow.runs SET started_at = $2 WHERE run_id = $1", [
        Ecto.UUID.dump!(queued_run_id),
        DateTime.add(now, -4, :hour)
      ])

      assert {:ok,
              %OverviewMetrics{
                active_workers: 4,
                healthy_workers: 1,
                stale_workers: 2,
                total_runs_24h: 4,
                completed_runs_24h: 1,
                failed_runs_24h: 1,
                running_runs: 2,
                avg_duration_ms: avg_duration,
                queue_depth: 1
              }} = Metrics.overview(TestRepo, now: now)

      assert Decimal.equal?(avg_duration, Decimal.new(120_000))
    end
  end

  defp insert_worker(worker_id, now, seconds_ago, opts \\ []) do
    heartbeat_at = DateTime.add(now, -seconds_ago, :second)
    stopped_at = if Keyword.get(opts, :stopped, false), do: heartbeat_at
    deprecated_at = if Keyword.get(opts, :deprecated, false), do: heartbeat_at

    TestRepo.query!(
      """
      INSERT INTO pgflow.workers
        (worker_id, queue_name, function_name, started_at, last_heartbeat_at, stopped_at, deprecated_at)
      VALUES ($1, 'metrics_test_flow', 'Elixir.MetricsTest.perform/2', $2, $2, $3, $4)
      """,
      [Ecto.UUID.dump!(worker_id), heartbeat_at, stopped_at, deprecated_at]
    )
  end

  defp insert_run(run_id, status, now, hours_ago, duration_seconds) do
    started_at = DateTime.add(now, -hours_ago, :hour)

    completed_at =
      if status == "completed", do: DateTime.add(started_at, duration_seconds, :second)

    failed_at = if status == "failed", do: DateTime.add(started_at, duration_seconds, :second)

    TestRepo.query!(
      """
      INSERT INTO pgflow.runs
        (run_id, flow_slug, status, input, remaining_steps, started_at, completed_at, failed_at)
      VALUES ($1, 'metrics_test_flow', $2, '{}'::jsonb, 0, $3, $4, $5)
      """,
      [Ecto.UUID.dump!(run_id), status, started_at, completed_at, failed_at]
    )
  end
end
