defmodule PgFlowDashboard.Queries.Runs do
  @moduledoc """
  Database queries for run-related data.
  """

  import PgFlow.Queries.Base

  @doc """
  Lists runs with progress information.

  ## Options

    * `:flow_slug` - Filter by flow slug
    * `:status` - Filter by status (started, completed, failed)
    * `:time_range` - Filter by time range (:last_hour, :last_24h, :last_7d, :last_30d)
    * `:limit` - Maximum number of runs to return (default: 50)
    * `:cursor` - Cursor for pagination (run_id to start after)

  """
  @spec list_runs(module(), keyword()) :: list(map())
  def list_runs(repo, opts \\ []) do
    params = [
      time_range_start(Keyword.get(opts, :time_range, :last_24h)),
      Keyword.get(opts, :flow_slug),
      status_to_string(Keyword.get(opts, :status)),
      Keyword.get(opts, :limit, 50),
      parse_uuid(Keyword.get(opts, :cursor))
    ]

    execute_rpc(repo, "list_runs", params, schema: "pgflow_dashboard", mode: :list)
  end

  @doc """
  Counts runs matching the given filters.

  ## Options

    * `:flow_slug` - Filter by flow slug
    * `:status` - Filter by status (started, completed, failed)
    * `:time_range` - Filter by time range (:last_hour, :last_24h, :last_7d, :last_30d)

  """
  @spec count_runs(module(), keyword()) :: integer()
  def count_runs(repo, opts \\ []) do
    params = [
      time_range_start(Keyword.get(opts, :time_range, :last_24h)),
      Keyword.get(opts, :flow_slug),
      status_to_string(Keyword.get(opts, :status))
    ]

    execute_rpc(repo, "count_runs", params, schema: "pgflow_dashboard", mode: :count)
  end

  @doc """
  Gets a single run with full details.
  """
  @spec get_run(module(), String.t()) :: {:ok, map()} | {:error, :not_found | term()}
  def get_run(repo, run_id) do
    execute_rpc(repo, "get_run", [parse_uuid(run_id)], schema: "pgflow_dashboard", mode: :single)
  end

  @doc """
  Gets the adjacent run (next or previous by started_at).

  Direction can be :next or :prev.
  Returns {:ok, run_id} or {:error, :not_found}.
  """
  @spec get_adjacent_run(module(), String.t(), :next | :prev) ::
          {:ok, String.t()} | {:error, :not_found}
  def get_adjacent_run(repo, run_id, direction) do
    params = [
      parse_uuid(run_id),
      direction_to_string(direction)
    ]

    case execute_rpc(repo, "get_adjacent_run", params, schema: "pgflow_dashboard", mode: :single) do
      {:ok, %{run_id: adjacent_id}} when not is_nil(adjacent_id) ->
        {:ok, adjacent_id}

      {:ok, %{run_id: nil}} ->
        {:error, :not_found}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc """
  Lists step states for a run.
  """
  @spec list_step_states(module(), String.t()) :: list(map())
  def list_step_states(repo, run_id) do
    execute_rpc(repo, "list_step_states", [parse_uuid(run_id)],
      schema: "pgflow_dashboard",
      mode: :list
    )
  end

  @doc """
  Lists tasks for a specific step in a run.
  """
  @spec list_step_tasks(module(), String.t(), String.t()) :: list(map())
  def list_step_tasks(repo, run_id, step_slug) do
    execute_rpc(repo, "list_step_tasks", [parse_uuid(run_id), step_slug],
      schema: "pgflow_dashboard",
      mode: :list
    )
  end
end
