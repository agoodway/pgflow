defmodule PgFlowDashboard.Queries.Workers do
  @moduledoc """
  Database queries for worker-related data.
  """

  import PgFlow.Queries.Base

  @doc """
  Counts workers matching the given filters.

  ## Options

    * `:flow_slug` - Filter by flow slug
    * `:health_status` - Filter by health (healthy, stale, dead)

  """
  @spec count_workers(module(), keyword()) :: integer()
  def count_workers(repo, opts \\ []) do
    params = [
      Keyword.get(opts, :flow_slug),
      health_status_to_string(Keyword.get(opts, :health_status))
    ]

    execute_rpc(repo, "count_workers", params, schema: "pgflow_dashboard", mode: :count)
  end

  @doc """
  Lists workers with load information.

  ## Options

    * `:flow_slug` - Filter by flow slug
    * `:health_status` - Filter by health (healthy, stale, dead)
    * `:limit` - Maximum number of workers to return
    * `:cursor` - Worker ID to start after for pagination

  """
  @spec list_workers(module(), keyword()) :: list(map())
  def list_workers(repo, opts \\ []) do
    params = [
      Keyword.get(opts, :flow_slug),
      health_status_to_string(Keyword.get(opts, :health_status)),
      Keyword.get(opts, :limit),
      parse_uuid(Keyword.get(opts, :cursor))
    ]

    execute_rpc(repo, "list_workers", params, schema: "pgflow_dashboard", mode: :list)
  end

  @doc """
  Gets a single worker with details.
  """
  @spec get_worker(module(), String.t()) :: {:ok, map()} | {:error, :not_found | term()}
  def get_worker(repo, worker_id) do
    execute_rpc(repo, "get_worker", [parse_uuid(worker_id)],
      schema: "pgflow_dashboard",
      mode: :single
    )
  end

  @doc """
  Lists tasks processed by a specific worker.
  """
  @spec list_worker_tasks(module(), String.t(), keyword()) :: list(map())
  def list_worker_tasks(repo, worker_id, opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)

    execute_rpc(repo, "list_worker_tasks", [parse_uuid(worker_id), limit],
      schema: "pgflow_dashboard",
      mode: :list
    )
  end

  @doc """
  Gets the adjacent worker (next or previous by last_heartbeat_at).

  Direction can be :next or :prev.
  Returns {:ok, worker_id} or {:error, :not_found}.
  """
  @spec get_adjacent_worker(module(), String.t(), :next | :prev) ::
          {:ok, String.t()} | {:error, :not_found}
  def get_adjacent_worker(repo, worker_id, direction) do
    params = [
      parse_uuid(worker_id),
      direction_to_string(direction)
    ]

    case execute_rpc(repo, "get_adjacent_worker", params,
           schema: "pgflow_dashboard",
           mode: :single
         ) do
      {:ok, %{worker_id: adjacent_id}} when not is_nil(adjacent_id) ->
        {:ok, adjacent_id}

      {:ok, %{worker_id: nil}} ->
        {:error, :not_found}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end
end
