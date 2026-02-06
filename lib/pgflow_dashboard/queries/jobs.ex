defmodule PgFlowDashboard.Queries.Jobs do
  @moduledoc """
  Database queries for job-related data.
  """

  import PgFlow.Queries.Base

  @doc """
  Lists jobs with statistics.
  """
  @spec list_jobs(module()) :: list(map())
  def list_jobs(repo) do
    execute_rpc(repo, "list_jobs", [], schema: "pgflow_dashboard", mode: :list)
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
