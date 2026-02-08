defmodule PgFlowDashboard.Queries.Jobs do
  @moduledoc """
  Database queries for job-related data.
  """

  import PgFlow.Queries.Helpers

  @doc """
  Counts all jobs.
  """
  @spec count_jobs(module()) :: integer()
  def count_jobs(repo) do
    execute_rpc(repo, "count_jobs", [], schema: "pgflow_dashboard", mode: :count)
  end

  @doc """
  Lists jobs with statistics.

  ## Options
    * `:limit` - Maximum number of jobs to return
    * `:cursor` - Cursor for pagination (flow_slug to start after)
  """
  @spec list_jobs(module(), keyword()) :: list(map())
  def list_jobs(repo, opts \\ []) do
    limit = Keyword.get(opts, :limit)
    cursor = Keyword.get(opts, :cursor)

    execute_rpc(repo, "list_jobs", [limit, cursor], schema: "pgflow_dashboard", mode: :list)
  end

  @doc """
  Gets a job's statistics.
  """
  @spec get_job(module(), String.t()) :: {:ok, map()} | {:error, :not_found | term()}
  def get_job(repo, flow_slug) do
    execute_rpc(repo, "get_job", [flow_slug], schema: "pgflow_dashboard", mode: :single)
  end

  @doc """
  Gets run history data for a job's activity grid.

  Returns a list of run result cells for the single job step.
  """
  @spec get_run_history_grid(module(), String.t(), keyword()) :: list(map())
  def get_run_history_grid(repo, flow_slug, opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)

    execute_rpc(repo, "get_run_history_grid", [flow_slug, limit],
      schema: "pgflow_dashboard",
      mode: :list
    )
  end
end
