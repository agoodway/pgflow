defmodule PgFlow.Supervisor do
  @moduledoc """
  Main supervisor for PgFlow components.

  This supervisor is started by `PgFlow.start_link/1` and manages the core
  PgFlow processes including:

  - TaskSupervisor - Supervises async task execution
  - WorkerSupervisor - Supervises flow workers
  - StalledTaskRecovery - Recovers orphaned tasks

  ## Supervision Tree

      PgFlow.Supervisor
      ├── Task.Supervisor (PgFlow.TaskSupervisor)
      ├── PgFlow.WorkerSupervisor
      └── PgFlow.Worker.StalledTaskRecovery

  """

  use Supervisor
  require Logger

  alias PgFlow.{Config, FlowRegistry, WorkerSupervisor}
  alias PgFlow.Queries.Pgmq, as: PgmqQueries
  alias PgFlow.Signal
  alias PgFlow.Worker.StalledTaskRecovery

  @task_supervisor PgFlow.TaskSupervisor

  @doc """
  Starts the PgFlow supervisor with the given configuration.

  The configuration should be validated using `PgFlow.Config.validate!/1`.

  ## Examples

      config = PgFlow.Config.validate!(repo: MyApp.Repo, flows: [MyFlow])
      {:ok, pid} = PgFlow.Supervisor.start_link(config)

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(config) when is_list(config) do
    Supervisor.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(config) do
    # Ensure config has all defaults applied, even if started directly
    # (e.g. {PgFlow.Supervisor, repo: MyRepo} without going through PgFlow.start_link)
    config = Config.validate!(config)

    repo = Keyword.fetch!(config, :repo)
    flows = Keyword.get(config, :flows, [])
    jobs = Keyword.get(config, :jobs, [])
    attach_logger = Keyword.get(config, :attach_default_logger, false)

    if attach_logger, do: PgFlow.Telemetry.attach_default_logger()

    signal_strategy = Keyword.get(config, :signal_strategy, :polling)
    notify_throttle_ms = Keyword.get(config, :notify_throttle_ms, 250)

    children =
      List.flatten([
        {Task.Supervisor, name: @task_supervisor},
        notify_child(signal_strategy, repo, notify_throttle_ms),
        {WorkerSupervisor, config},
        {StalledTaskRecovery, config},
        starter_child(flows, jobs, repo, signal_strategy, notify_throttle_ms)
      ])

    # Use :rest_for_one so if TaskSupervisor crashes, WorkerSupervisor
    # and StalledTaskRecovery (which depend on it) also restart
    result = Supervisor.init(children, strategy: :rest_for_one)

    Logger.info("PgFlow.Supervisor started with repo: #{inspect(repo)}")

    result
  end

  # Private Functions

  defp notify_child(:notify, repo, throttle_ms),
    do: [{Signal.Notify, [repo: repo, notify_throttle_ms: throttle_ms]}]

  defp notify_child(_strategy, _repo, _throttle_ms), do: []

  defp starter_child(flows, jobs, repo, signal_strategy, notify_throttle_ms) do
    %{
      id: :flow_starter,
      start:
        {Task, :start_link,
         [
           fn ->
             register_modules(flows, "flow", repo, signal_strategy, notify_throttle_ms)
             register_modules(jobs, "job", repo, signal_strategy, notify_throttle_ms)
           end
         ]},
      restart: :temporary
    }
  end

  defp register_modules(modules, module_type, repo, signal_strategy, notify_throttle_ms) do
    Enum.each(
      modules,
      &register_module(&1, module_type, repo, signal_strategy, notify_throttle_ms)
    )
  end

  defp register_module(module, module_type, repo, signal_strategy, notify_throttle_ms) do
    FlowRegistry.register(module)
    Logger.info("Registered #{module_type}: #{inspect(module)}")

    case WorkerSupervisor.start_worker(module, repo: repo) do
      {:ok, pid} ->
        Logger.info("Started worker for #{module_type}: #{inspect(module)}")
        maybe_enable_notify(signal_strategy, module, pid, repo, notify_throttle_ms)

      {:error, reason} ->
        Logger.error(
          "Failed to start worker for #{module_type} #{inspect(module)}: #{inspect(reason)}"
        )
    end
  rescue
    error ->
      Logger.error(
        "Failed to register #{module_type} #{inspect(module)}: #{Exception.message(error)}"
      )
  end

  defp maybe_enable_notify(:notify, module, pid, repo, throttle_ms) do
    flow_slug = module.__pgflow_definition__().slug |> Atom.to_string()

    # Enable pgmq notifications BEFORE registering with Signal.Notify.
    # Must happen outside the SimpleConnection process to avoid blocking
    # the socket read loop and missing notifications.
    case PgmqQueries.enable_notify_insert(repo, flow_slug, throttle_ms) do
      :ok ->
        Logger.debug("Enabled pgmq notifications for #{flow_slug} with #{throttle_ms}ms throttle")

      {:error, reason} ->
        Logger.error("Failed to enable pgmq notify for #{flow_slug}: #{inspect(reason)}")
    end

    Signal.Notify.register_worker(flow_slug, pid)
  end

  defp maybe_enable_notify(_strategy, _module, _pid, _repo, _throttle_ms), do: :ok
end
