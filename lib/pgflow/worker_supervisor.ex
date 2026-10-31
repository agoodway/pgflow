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
  alias PgFlow.WorkerOptions

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
    * `:max_concurrency` - Override max concurrent tasks for this worker
    * `:batch_size` - Override batch size for this worker

  Limits resolve from the global config, then the module's `worker_options`
  entry, then the supplied options. When a worker is already running, supplied
  options are compared against its configured (start-time) limits, not limits
  changed later by `scale_worker/2`.

  Returns `{:ok, pid}` on success, `{:error, {:conflicting_worker_options, pid}}`
  when a worker is already running with different supplied options,
  `{:error, :not_started}` when a concurrent supervisor restart started a
  replacement that exited before it could be returned (retrying is safe), or
  `{:error, reason}` on failure. Raises `ArgumentError` for unknown option keys
  or invalid values.
  """
  @spec start_worker(module(), keyword()) ::
          {:ok, pid()}
          | {:error, {:conflicting_worker_options, pid()} | :not_started}
          | {:error, term()}
  def start_worker(flow_module, opts \\ []) do
    {worker_opts, rest} = WorkerOptions.split(opts)
    validate_start_rest!(rest)
    WorkerOptions.validate!(worker_opts)

    :global.trans(
      {{__MODULE__, flow_module}, self()},
      fn -> find_or_start_worker(flow_module, rest, worker_opts) end,
      [node()]
    )
  end

  defp find_or_start_worker(flow_module, rest, worker_opts) do
    case find_worker(flow_module) do
      pid when is_pid(pid) -> reply_for_existing_worker(flow_module, rest, worker_opts, pid)
      nil -> do_start_worker(flow_module, rest, worker_opts)
    end
  end

  # Without supplied options any live worker satisfies the request; with them,
  # the worker's configured limits must match. A worker that exits before it
  # answers the check is treated as absent and a fresh one is started.
  defp reply_for_existing_worker(_flow_module, _rest, [], pid), do: {:ok, pid}

  defp reply_for_existing_worker(flow_module, rest, worker_opts, _pid) do
    case check_worker(flow_module) do
      nil ->
        do_start_worker(flow_module, rest, worker_opts)

      %{pid: pid} = check ->
        if worker_opts_match?(worker_opts, check),
          do: {:ok, pid},
          else: {:error, {:conflicting_worker_options, pid}}
    end
  end

  defp validate_start_rest!(rest) do
    case Enum.reject(rest, fn {key, _} -> key == :repo end) do
      [] ->
        :ok

      invalid ->
        keys = Enum.map(invalid, &elem(&1, 0))
        raise ArgumentError, "unknown start_worker options: #{inspect(keys)}"
    end
  end

  defp worker_opts_match?(worker_opts, %{
         configured_max_concurrency: configured_max_concurrency,
         configured_batch_size: configured_batch_size
       }) do
    Enum.all?(worker_opts, fn
      {:max_concurrency, value} -> configured_max_concurrency == value
      {:batch_size, value} -> configured_batch_size == value
    end)
  end

  defp do_start_worker(flow_module, opts, worker_opts) do
    repo = Keyword.get(opts, :repo) || :persistent_term.get({PgFlow, :repo})

    spec = %{
      id: flow_module,
      start: {__MODULE__, :start_worker_process, [flow_module, repo, worker_opts]},
      restart: :transient
    }

    case DynamicSupervisor.start_child(__MODULE__, spec) do
      {:ok, _pid} = result ->
        Logger.info("Started worker for flow #{inspect(flow_module)}")
        result

      # A supervisor restart registered a replacement while this call waited.
      :ignore ->
        case find_worker(flow_module) do
          nil -> {:error, :not_started}
          pid -> reply_for_existing_worker(flow_module, opts, worker_opts, pid)
        end

      {:error, reason} = error ->
        Logger.error(
          "Failed to start worker for flow #{inspect(flow_module)}: #{inspect(reason)}"
        )

        error
    end
  end

  @doc false
  def start_worker_process(flow_module, repo) do
    start_worker_process(flow_module, repo, [])
  end

  # DynamicSupervisor runs start functions one at a time in its own process,
  # so this check and the registration below cannot interleave with another
  # start for the same module. Returning `:ignore` keeps an explicit start and
  # a crash restart from leaving two workers polling the same flow.
  @doc false
  def start_worker_process(flow_module, repo, overrides) when is_list(overrides) do
    case find_worker(flow_module) do
      nil -> start_unregistered_worker(flow_module, repo, overrides)
      _live_pid -> :ignore
    end
  end

  defp start_unregistered_worker(flow_module, repo, overrides) do
    config = :persistent_term.get({PgFlow, :config})
    resolved = WorkerOptions.resolve(config, flow_module, overrides)

    worker_config = %{
      flow_module: flow_module,
      repo: repo,
      max_concurrency: resolved.max_concurrency,
      batch_size: resolved.batch_size,
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
  Scales a running worker's runtime limits.

  `opts` takes `:max_concurrency` and/or `:batch_size`, plus an optional
  `:timeout` (default `5_000` ms, or `:infinity`) for the call to the worker.
  Raises `ArgumentError` on missing or invalid options.

  Returns `{:error, :not_found}` when no worker is registered for the module on
  this node, or when the worker exits before the scale call completes. When the
  timeout elapses the caller exits with `{:timeout, {GenServer, :call, _}}`;
  the worker may still apply the scale after that.
  """
  @spec scale_worker(module(), keyword()) ::
          {:ok, map()} | {:error, :not_found | :not_running}
  def scale_worker(flow_module, opts) when is_list(opts) do
    {timeout, scale_opts} = WorkerOptions.pop_call_timeout!(opts)
    WorkerOptions.validate_scale!(scale_opts)

    call_local_worker(flow_module, {:error, :not_found}, fn pid ->
      case WorkerServer.scale(pid, scale_opts, timeout) do
        {:ok, check} -> {:ok, Map.merge(check, %{module: flow_module, pid: pid})}
        {:error, :not_running} = error -> error
      end
    end)
  end

  @doc """
  Returns runtime worker state for a flow module on this node, or `nil`.

  ## Options

    * `:timeout` - milliseconds (or `:infinity`) to wait for the worker to
      reply. Default: `5_000`. When it elapses the caller exits with
      `{:timeout, {GenServer, :call, _}}`.

  Raises `ArgumentError` on unknown or invalid options.
  """
  @spec check_worker(module(), keyword()) :: map() | nil
  def check_worker(flow_module, opts \\ []) when is_list(opts) do
    timeout = WorkerOptions.validate_call!(opts)

    call_local_worker(flow_module, nil, fn pid ->
      pid |> WorkerServer.check(timeout) |> Map.merge(%{module: flow_module, pid: pid})
    end)
  end

  # Calls `fun` with the registered worker pid, returning `gone` when no worker
  # is registered or the worker exits before replying. Call timeouts re-raise:
  # the request may still be applied, so reporting the worker as gone would lie.
  defp call_local_worker(flow_module, gone, fun) do
    case find_worker(flow_module) do
      nil ->
        gone

      pid ->
        try do
          fun.(pid)
        catch
          :exit, reason ->
            if worker_gone_exit?(reason),
              do: gone,
              else: :erlang.raise(:exit, reason, __STACKTRACE__)
        end
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

  # `GenServer.call/3` exits with `{reason, {GenServer, :call, args}}`. Any
  # reason other than a timeout means the worker died before replying.
  defp worker_gone_exit?({:timeout, {GenServer, :call, _args}}), do: false
  defp worker_gone_exit?({_reason, {GenServer, :call, _args}}), do: true
  defp worker_gone_exit?(_reason), do: false
end
