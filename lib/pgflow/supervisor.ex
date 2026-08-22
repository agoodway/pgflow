defmodule PgFlow.Supervisor do
  @moduledoc """
  Main supervisor for PgFlow components.

  This supervisor is started by `PgFlow.start_link/1` and manages the core
  PgFlow processes including:

  - TaskSupervisor - Supervises async task execution
  - WorkerSupervisor - Supervises flow workers
  - StalledTaskRecovery - Recovers orphaned tasks
  - WaitingTaskRecovery - Re-queues expired `waiting` tasks

  ## Supervision Tree

      PgFlow.Supervisor
      ├── Task.Supervisor (PgFlow.TaskSupervisor)
      ├── PgFlow.WorkerSupervisor
      ├── PgFlow.Worker.StalledTaskRecovery
      └── PgFlow.Worker.WaitingTaskRecovery

  """

  use Supervisor
  require Logger

  alias PgFlow.{Config, FlowStarter, Telemetry, WorkerSupervisor}
  alias PgFlow.Signal
  alias PgFlow.Worker.{StalledTaskRecovery, WaitingTaskRecovery}

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

    pubsub = Keyword.get(config, :pubsub)

    if attach_logger, do: Telemetry.attach_default_logger()
    if pubsub, do: Telemetry.PubSub.attach(pubsub: pubsub)

    signal_strategy = Keyword.get(config, :signal_strategy, :polling)
    notify_throttle_ms = Keyword.get(config, :notify_throttle_ms, 250)

    children =
      List.flatten([
        {Task.Supervisor, name: @task_supervisor},
        notify_child(signal_strategy, repo, notify_throttle_ms),
        {WorkerSupervisor, config},
        {StalledTaskRecovery, config},
        {WaitingTaskRecovery, config},
        {FlowStarter,
         repo: repo,
         flows: flows,
         jobs: jobs,
         signal_strategy: signal_strategy,
         notify_throttle_ms: notify_throttle_ms}
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
end
