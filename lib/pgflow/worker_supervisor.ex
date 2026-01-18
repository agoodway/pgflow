defmodule PgFlow.WorkerSupervisor do
  @moduledoc """
  Supervisor for PgFlow workers.

  This module manages worker processes that poll for and execute flow tasks.
  Each flow can have one or more workers processing its tasks.

  Workers are GenServer processes that:
  - Poll pgmq for pending messages
  - Execute step handlers concurrently via Task.Supervisor
  - Report task completion/failure back to pgflow
  - Send heartbeats and handle graceful shutdown
  """

  use DynamicSupervisor
  require Logger

  alias PgFlow.Worker.Server, as: WorkerServer

  @doc """
  Starts the WorkerSupervisor.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(config) when is_list(config) do
    DynamicSupervisor.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(config) do
    # Store config in persistent term for workers to access
    repo = Keyword.fetch!(config, :repo)
    :persistent_term.put({PgFlow, :repo}, repo)
    :persistent_term.put({PgFlow, :config}, config)

    Logger.debug("WorkerSupervisor initialized")

    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Starts a worker for the given flow.

  ## Options

    * `:repo` - The Ecto repository (optional, defaults to configured repo)

  Returns `{:ok, pid}` on success or `{:error, reason}` on failure.
  """
  @spec start_worker(module(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_worker(flow_module, opts \\ []) do
    repo = Keyword.get(opts, :repo) || :persistent_term.get({PgFlow, :repo})

    spec = %{
      id: flow_module,
      start: {__MODULE__, :start_worker_process, [flow_module, repo]},
      restart: :transient
    }

    case DynamicSupervisor.start_child(__MODULE__, spec) do
      {:ok, _pid} = result ->
        Logger.info("Started worker for flow #{inspect(flow_module)}")
        result

      {:error, {:already_started, pid}} ->
        Logger.debug("Worker for flow #{inspect(flow_module)} already running")
        {:ok, pid}

      {:error, reason} = error ->
        Logger.error(
          "Failed to start worker for flow #{inspect(flow_module)}: #{inspect(reason)}"
        )

        error
    end
  end

  @doc false
  def start_worker_process(flow_module, repo) do
    # Get global config for default values
    config = :persistent_term.get({PgFlow, :config}, [])

    worker_config = %{
      flow_module: flow_module,
      repo: repo,
      max_concurrency: Keyword.get(config, :max_concurrency, 10),
      batch_size: Keyword.get(config, :batch_size, 10),
      poll_interval: Keyword.get(config, :poll_interval, 100),
      visibility_timeout: Keyword.get(config, :visibility_timeout, 2)
    }

    WorkerServer.start_link(worker_config)
  end

  @doc """
  Stops a worker for the given flow.

  Gracefully stops the worker, waiting for active tasks to complete.
  """
  @spec stop_worker(module()) :: :ok | {:error, :not_found}
  def stop_worker(flow_module) do
    case find_worker(flow_module) do
      nil ->
        {:error, :not_found}

      pid ->
        # Gracefully stop the worker (waits for active tasks)
        try do
          WorkerServer.stop(pid)
        catch
          :exit, _ -> :ok
        end

        :ok
    end
  end

  @doc """
  Lists all running workers.

  Returns a list of maps with worker information.
  """
  @spec list_workers() :: [%{pid: pid(), status: :running}]
  def list_workers do
    __MODULE__
    |> DynamicSupervisor.which_children()
    |> Enum.map(fn
      {_id, pid, _type, _modules} when is_pid(pid) ->
        %{pid: pid, status: :running}

      _other ->
        nil
    end)
    |> Enum.reject(&is_nil/1)
  end

  # Private Functions

  defp find_worker(flow_module) do
    __MODULE__
    |> DynamicSupervisor.which_children()
    |> Enum.find_value(fn
      {^flow_module, pid, _type, _modules} when is_pid(pid) -> pid
      _other -> nil
    end)
  end
end
