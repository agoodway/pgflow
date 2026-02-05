defmodule PgFlowDashboard.Queries.Metrics do
  @moduledoc """
  Database queries for dashboard metrics with caching.
  """

  import PgFlowDashboard.Queries.Base

  alias PgFlowDashboard.Cache.MetricsCache

  @doc """
  Returns overview dashboard metrics.

  Uses ETS cache with configurable TTL to reduce database load.
  """
  @spec get_overview_metrics(module(), keyword()) :: map()
  def get_overview_metrics(repo, opts \\ []) do
    cache_ttl = Keyword.get(opts, :cache_ttl, 5_000)

    MetricsCache.fetch(
      :overview_metrics,
      fn -> fetch_overview_metrics(repo) end,
      ttl: cache_ttl
    )
  end

  defp fetch_overview_metrics(repo) do
    case execute_rpc(repo, "get_overview_metrics", [], mode: :single) do
      {:ok, metrics} -> metrics
      {:error, _} -> default_metrics()
    end
  end

  defp default_metrics do
    %{
      active_workers: 0,
      healthy_workers: 0,
      stale_workers: 0,
      total_runs_24h: 0,
      completed_runs_24h: 0,
      failed_runs_24h: 0,
      running_runs: 0,
      avg_duration_ms: 0,
      queue_depth: 0
    }
  end
end
