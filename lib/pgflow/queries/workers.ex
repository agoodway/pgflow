defmodule PgFlow.Queries.Workers do
  @moduledoc """
  SQL query interface for pgflow worker operations.

  Provides functions for registering workers and managing their lifecycle.
  """

  import PgFlow.Queries.Helpers, only: [execute_rpc: 4, parse_uuid: 1]

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
end
