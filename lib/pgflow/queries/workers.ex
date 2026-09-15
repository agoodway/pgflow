defmodule PgFlow.Queries.Workers do
  @moduledoc """
  SQL query interface for pgflow worker operations.

  Provides functions for registering workers and managing their lifecycle.
  """

  import PgFlow.Queries.Helpers, only: [execute_rpc: 4, parse_uuid: 1]
  alias Ecto.Adapters.SQL

  @pgflow_schema "pgflow"

  @doc """
  Registers a worker in the database.

  Creates a new worker record or updates the heartbeat if the worker already exists.

  ## Parameters

    * `repo` - The Ecto repository
    * `worker_id` - The worker identifier (UUID string)
    * `queue_name` - The queue name (flow_slug)
    * `function_name` - The function name (e.g., "elixir:MyApp.Flows.MyFlow")

  ## Returns

    * `{:ok, nil}` - Success
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec register_worker(Ecto.Repo.t(), String.t(), String.t(), String.t()) ::
          {:ok, nil} | {:error, term()}
  def register_worker(repo, worker_id, queue_name, function_name) do
    execute_rpc(repo, "register_worker", [parse_uuid(worker_id), queue_name, function_name],
      schema: @pgflow_schema,
      mode: :void
    )
  end

  @doc """
  Marks a worker as stopped.

  Sets the `stopped_at` timestamp for graceful shutdown signaling.

  ## Parameters

    * `repo` - The Ecto repository
    * `worker_id` - The worker identifier (UUID string)

  ## Returns

    * `{:ok, nil}` - Success
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec mark_worker_stopped(Ecto.Repo.t(), String.t()) ::
          {:ok, nil} | {:error, term()}
  def mark_worker_stopped(repo, worker_id) do
    execute_rpc(repo, "mark_worker_stopped", [parse_uuid(worker_id)],
      schema: @pgflow_schema,
      mode: :void
    )
  end

  @doc """
  Refreshes a worker heartbeat and reports whether it has been deprecated.

  Missing registration is treated as deprecated, matching upstream edge-worker
  behavior when a worker row no longer exists.
  """
  @spec heartbeat_worker(Ecto.Repo.t(), String.t()) ::
          {:ok, :alive | :deprecated} | {:error, term()}
  def heartbeat_worker(repo, worker_id) do
    sql = """
    UPDATE pgflow.workers
    SET last_heartbeat_at = NOW()
    WHERE worker_id = $1::uuid
    RETURNING (deprecated_at IS NOT NULL) AS deprecated
    """

    case repo_query(repo, sql, [parse_uuid(worker_id)]) do
      {:ok, %{num_rows: 0}} ->
        {:ok, :deprecated}

      {:ok, %{rows: [[true]]}} ->
        {:ok, :deprecated}

      {:ok, %{rows: [[false]]}} ->
        {:ok, :alive}

      {:error, error} ->
        {:error, error}
    end
  end

  defp repo_query(repo, sql, params) do
    SQL.query(repo, sql, params)
  end

  @doc """
  Registers an edge/worker function for monitoring by `pgflow.ensure_workers()`.

  Elixir workers call this on startup with `"process"` start mode after flow
  compilation succeeds.
  """
  @spec track_worker_function(Ecto.Repo.t(), String.t(), String.t()) ::
          {:ok, nil} | {:error, term()}
  def track_worker_function(repo, function_name, start_mode)
      when is_binary(function_name) and is_binary(start_mode) do
    sql = "SELECT pgflow.track_worker_function($1::text, $2::text)"

    case SQL.query(repo, sql, [function_name, start_mode]) do
      {:ok, _} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end
end
