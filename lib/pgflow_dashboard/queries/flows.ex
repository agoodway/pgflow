defmodule PgFlowDashboard.Queries.Flows do
  @moduledoc """
  Database queries for flow-related data.
  """

  import PgFlow.Queries.Helpers

  @doc """
  Counts all flows.
  """
  @spec count_flows(module()) :: integer()
  def count_flows(repo) do
    execute_rpc(repo, "count_flows", [], schema: "pgflow_dashboard", mode: :count)
  end

  @doc """
  Lists flows with statistics.

  ## Options
    * `:limit` - Maximum number of flows to return
    * `:cursor` - Cursor for pagination (flow_slug to start after)
  """
  @spec list_flows(module(), keyword()) :: list(map())
  def list_flows(repo, opts \\ []) do
    limit = Keyword.get(opts, :limit)
    cursor = Keyword.get(opts, :cursor)

    execute_rpc(repo, "list_flows", [limit, cursor], schema: "pgflow_dashboard", mode: :list)
  end

  @doc """
  Gets a flow with its dependency graph.
  """
  @spec get_flow_with_graph(module(), String.t()) :: {:ok, map()} | {:error, :not_found | term()}
  def get_flow_with_graph(repo, flow_slug) do
    with {:ok, flow} <-
           execute_rpc(repo, "get_flow", [flow_slug], schema: "pgflow_dashboard", mode: :single) do
      steps =
        execute_rpc(repo, "list_flow_steps", [flow_slug], schema: "pgflow_dashboard", mode: :list)

      {:ok, Map.put(flow, :steps, steps)}
    end
  end

  @doc """
  Gets run history data for a GitHub-style activity grid.

  Returns a map with steps as keys and lists of run results as values.
  """
  @spec get_run_history_grid(module(), String.t(), keyword()) :: map()
  def get_run_history_grid(repo, flow_slug, opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)

    execute_rpc(repo, "get_run_history_grid", [flow_slug, limit],
      schema: "pgflow_dashboard",
      mode: :list
    )
    |> Enum.group_by(& &1.step_slug)
  end
end
