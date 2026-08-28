defmodule PgFlow.Metrics do
  @moduledoc """
  Typed operational overview metrics for PgFlow.

  Run totals and duration use an explicit 24-hour window. Running runs and
  queued tasks reflect current all-time state, matching their operational use.
  """

  import Ecto.Query

  alias PgFlow.OverviewMetrics
  alias PgFlow.Schema.{Run, StepTask, Worker}

  @active_worker_seconds 5 * 60
  @healthy_worker_seconds 30
  @stale_worker_seconds 60

  @doc """
  Returns the current overview metrics.

  The optional `:now` value provides a common clock boundary for every metric.
  """
  @spec overview(module(), keyword()) :: {:ok, OverviewMetrics.t()}
  def overview(repo, opts \\ []) do
    now = Keyword.get_lazy(opts, :now, &DateTime.utc_now/0)

    metrics =
      worker_metrics(repo, now)
      |> Map.merge(run_metrics(repo, now))
      |> Map.put(:queue_depth, queue_depth(repo))
      |> OverviewMetrics.new()

    {:ok, metrics}
  end

  defp worker_metrics(repo, now) do
    active_after = DateTime.add(now, -@active_worker_seconds, :second)
    healthy_after = DateTime.add(now, -@healthy_worker_seconds, :second)
    stale_after = DateTime.add(now, -@stale_worker_seconds, :second)

    Worker
    |> where([worker], is_nil(worker.stopped_at) and is_nil(worker.deprecated_at))
    |> select([worker], %{
      active_workers:
        type(
          fragment("COUNT(*) FILTER (WHERE ? > ?)", worker.last_heartbeat_at, ^active_after),
          :integer
        ),
      healthy_workers:
        type(
          fragment("COUNT(*) FILTER (WHERE ? > ?)", worker.last_heartbeat_at, ^healthy_after),
          :integer
        ),
      stale_workers:
        type(
          fragment(
            "COUNT(*) FILTER (WHERE ? > ? AND ? <= ?)",
            worker.last_heartbeat_at,
            ^stale_after,
            worker.last_heartbeat_at,
            ^healthy_after
          ),
          :integer
        )
    })
    |> repo.one!()
  end

  defp run_metrics(repo, now) do
    started_after = DateTime.add(now, -24, :hour)

    Run
    |> select([run], %{
      total_runs_24h:
        type(fragment("COUNT(*) FILTER (WHERE ? > ?)", run.started_at, ^started_after), :integer),
      completed_runs_24h:
        type(
          fragment(
            "COUNT(*) FILTER (WHERE ? = 'completed' AND ? > ?)",
            run.status,
            run.started_at,
            ^started_after
          ),
          :integer
        ),
      failed_runs_24h:
        type(
          fragment(
            "COUNT(*) FILTER (WHERE ? = 'failed' AND ? > ?)",
            run.status,
            run.started_at,
            ^started_after
          ),
          :integer
        ),
      running_runs: type(fragment("COUNT(*) FILTER (WHERE ? = 'started')", run.status), :integer),
      avg_duration_ms:
        type(
          fragment(
            "COALESCE(AVG(EXTRACT(EPOCH FROM (? - ?)) * 1000) FILTER (WHERE ? = 'completed' AND ? > ?), 0)",
            run.completed_at,
            run.started_at,
            run.status,
            run.started_at,
            ^started_after
          ),
          :decimal
        )
    })
    |> repo.one!()
  end

  defp queue_depth(repo) do
    StepTask
    |> where([task], task.status == "queued")
    |> repo.aggregate(:count)
  end
end
