defmodule PgFlow.WorkerSupervisor do
  @moduledoc """
  Supervisor for PgFlow workers.

  This module manages worker processes that poll for and execute flow tasks.
  Sequential starts for the same flow reuse the registered worker on this node.

  Workers are GenServer processes that:
  - Compile or verify their flow definition via `PgFlow.Worker.Bootstrap` on startup
  - Poll pgmq for pending messages
  - Execute step handlers concurrently via Task.Supervisor
  - Report task completion/failure back to pgflow
  - Handle graceful shutdown
  """

  use DynamicSupervisor
  require Logger

  alias PgFlow.Signal.Notify
  alias PgFlow.Worker.Server, as: WorkerServer

  @registry_table :pgflow_worker_registry

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

    case :ets.whereis(@registry_table) do
      :undefined ->
        :ets.new(@registry_table, [:named_table, :set, :public, read_concurrency: true])

      table ->
        :ets.delete_all_objects(table)
    end

    Logger.debug("WorkerSupervisor initialized")

    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 10, max_seconds: 60)
  end

  @doc """
  Starts a worker for the given flow.

  ## Options

    * `:repo` - The Ecto repository (optional, defaults to configured repo)

  Returns `{:ok, pid}` on success or `{:error, reason}` on failure.
  """
  @spec start_worker(module(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_worker(flow_module, opts \\ []) do
    case find_worker(flow_module) do
      pid when is_pid(pid) -> {:ok, pid}
      nil -> do_start_worker(flow_module, opts)
    end
  end

  defp do_start_worker(flow_module, opts) do
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
    config = :persistent_term.get({PgFlow, :config})

    worker_config = %{
      flow_module: flow_module,
      repo: repo,
      max_concurrency: Keyword.fetch!(config, :max_concurrency),
      batch_size: Keyword.fetch!(config, :batch_size),
      signal_strategy: Keyword.fetch!(config, :signal_strategy),
      heartbeat_interval: Keyword.fetch!(config, :heartbeat_interval),
      min_poll_interval: Keyword.fetch!(config, :min_poll_interval),
      max_poll_interval: Keyword.fetch!(config, :max_poll_interval),
      notify_fallback_interval: Keyword.fetch!(config, :notify_fallback_interval)
    }

    case WorkerServer.start_link(worker_config) do
      {:ok, pid} = result ->
        replacement? = replacing_worker?(flow_module)
        register_worker(flow_module, pid)
        maybe_register_replacement_notify(worker_config, flow_module, pid, replacement?)
        result

      other ->
        other
    end
  end

  @doc """
  Stops a worker for the given flow.

  Gracefully stops the worker, waiting for active tasks to complete, then
  terminates the supervised child and unregisters it. Normal stops are final
  under the transient restart policy; crashes and deprecation exits restart.
  """
  @spec stop_worker(module()) :: :ok | {:error, :not_found}
  def stop_worker(flow_module) do
    case find_worker(flow_module) do
      nil ->
        {:error, :not_found}

      pid ->
        monitor = Process.monitor(pid)

        try do
          WorkerServer.stop(pid)
        catch
          :exit, _ -> :ok
        end

        receive do
          {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
        end

        remove_stopped_child(flow_module, pid)
    end
  end

  defp remove_stopped_child(flow_module, pid) do
    case DynamicSupervisor.terminate_child(__MODULE__, pid) do
      result when result in [:ok, {:error, :not_found}] ->
        stop_registered_replacement(flow_module, pid)

      {:error, _} = error ->
        error
    end
  end

  defp stop_registered_replacement(flow_module, stopped_pid) do
    case find_worker(flow_module) do
      pid when pid in [nil, stopped_pid] ->
        unregister_worker(flow_module)
        :ok

      _replacement ->
        stop_worker(flow_module)
    end
  end

  @doc """
  Returns the pid registered for a flow module, if any.
  """
  @spec find_worker(module()) :: pid() | nil
  def find_worker(flow_module) do
    if :ets.whereis(@registry_table) == :undefined do
      nil
    else
      lookup_registered_worker(flow_module)
    end
  end

  defp lookup_registered_worker(flow_module) do
    case :ets.lookup(@registry_table, flow_module) do
      [{^flow_module, pid}] when is_pid(pid) ->
        if Process.alive?(pid), do: pid, else: nil

      [] ->
        nil
    end
  end

  @doc """
  Lists all running workers.

  Returns a list of maps with worker information.
  """
  @spec list_workers() :: [%{pid: pid(), status: :running}]
  def list_workers do
    case :ets.whereis(@registry_table) do
      :undefined ->
        []

      _table ->
        @registry_table
        |> :ets.tab2list()
        |> Enum.flat_map(&live_worker_summary/1)
    end
  end

  # Private Functions

  defp maybe_register_replacement_notify(
         %{signal_strategy: :notify},
         flow_module,
         pid,
         true
       ) do
    flow_slug = flow_module.__pgflow_definition__().slug |> Atom.to_string()
    Notify.register_worker_async(flow_slug, pid)
  end

  defp maybe_register_replacement_notify(_config, _flow_module, _pid, _replacement?), do: :ok

  defp replacing_worker?(flow_module) do
    case :ets.whereis(@registry_table) do
      :undefined ->
        false

      _table ->
        stale_worker_registered?(flow_module)
    end
  end

  defp stale_worker_registered?(flow_module) do
    case :ets.lookup(@registry_table, flow_module) do
      [{^flow_module, previous_pid}] when is_pid(previous_pid) ->
        not Process.alive?(previous_pid)

      [] ->
        false
    end
  end

  defp live_worker_summary({_flow_module, pid}) when is_pid(pid) do
    if Process.alive?(pid), do: [%{pid: pid, status: :running}], else: []
  end

  defp live_worker_summary(_), do: []

  defp register_worker(flow_module, pid) do
    ensure_registry_table()
    :ets.insert(@registry_table, {flow_module, pid})
  end

  defp unregister_worker(flow_module) do
    if :ets.whereis(@registry_table) != :undefined do
      :ets.delete(@registry_table, flow_module)
    end
  end

  defp ensure_registry_table do
    case :ets.whereis(@registry_table) do
      :undefined ->
        :ets.new(@registry_table, [:named_table, :set, :public, read_concurrency: true])

      _table ->
        :ok
    end
  end
end
