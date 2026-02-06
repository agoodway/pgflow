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

  alias PgFlow.{FlowRegistry, WorkerSupervisor}
  alias PgFlow.Worker.StalledTaskRecovery

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
    config = PgFlow.Config.validate!(config)

    repo = Keyword.fetch!(config, :repo)
    flows = Keyword.get(config, :flows, [])
    jobs = Keyword.get(config, :jobs, [])
    crons = Keyword.get(config, :crons, [])
    attach_logger = Keyword.get(config, :attach_default_logger, false)

    # Attach telemetry logger if configured
    if attach_logger do
      attach_telemetry_logger()
    end

    children = [
      # Start TaskSupervisor first — workers depend on it for async task execution
      {Task.Supervisor, name: PgFlow.TaskSupervisor},

      # Start the WorkerSupervisor to manage flow workers
      {WorkerSupervisor, config},

      # Start stalled task recovery to reclaim orphaned tasks
      {StalledTaskRecovery, config},

      # Register flows/jobs and start workers — runs after WorkerSupervisor is alive
      # Uses :temporary restart since it's a one-shot initialization task
      %{
        id: :flow_starter,
        start:
          {Task, :start_link,
           [
             fn ->
               register_modules(flows, "flow", repo)
               register_modules(jobs, "job", repo)
               register_modules(crons, "cron", repo)
             end
           ]},
        restart: :temporary
      }
    ]

    # Use :rest_for_one so if TaskSupervisor crashes, WorkerSupervisor
    # and StalledTaskRecovery (which depend on it) also restart
    result = Supervisor.init(children, strategy: :rest_for_one)

    Logger.info("PgFlow.Supervisor started with repo: #{inspect(repo)}")

    result
  end

  # Private Functions

  defp register_modules(modules, module_type, repo) do
    Enum.each(modules, fn module ->
      try do
        FlowRegistry.register(module)
        Logger.info("Registered #{module_type}: #{inspect(module)}")

        case WorkerSupervisor.start_worker(module, repo: repo) do
          {:ok, _pid} ->
            Logger.info("Started worker for #{module_type}: #{inspect(module)}")

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
    end)
  end

  defp attach_telemetry_logger do
    events = [
      [:pgflow, :flow, :started],
      [:pgflow, :flow, :completed],
      [:pgflow, :flow, :failed],
      [:pgflow, :step, :started],
      [:pgflow, :step, :completed],
      [:pgflow, :step, :failed],
      [:pgflow, :task, :started],
      [:pgflow, :task, :completed],
      [:pgflow, :task, :failed],
      [:pgflow, :worker, :started],
      [:pgflow, :worker, :stopped],
      [:pgflow, :worker, :poll],
      [:pgflow, :worker, :error]
    ]

    :telemetry.attach_many(
      "pgflow-default-logger",
      events,
      &__MODULE__.handle_telemetry_event/4,
      nil
    )

    Logger.debug("Attached PgFlow telemetry logger")
  end

  @doc false
  def handle_telemetry_event([:pgflow, :flow, :started], _measurements, metadata, _config) do
    Logger.info("Flow started: #{metadata.flow_slug}, run_id: #{metadata.run_id}")
  end

  def handle_telemetry_event([:pgflow, :flow, :completed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.info(
      "Flow completed: #{metadata.flow_slug}, run_id: #{metadata.run_id}, duration: #{duration_ms}ms"
    )
  end

  def handle_telemetry_event([:pgflow, :flow, :failed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.error(
      "Flow failed: #{metadata.flow_slug}, run_id: #{metadata.run_id}, " <>
        "error: #{metadata.error}, duration: #{duration_ms}ms"
    )
  end

  def handle_telemetry_event([:pgflow, :step, :started], _measurements, metadata, _config) do
    Logger.debug("Step started: #{metadata.step_slug}, run_id: #{metadata.run_id}")
  end

  def handle_telemetry_event([:pgflow, :step, :completed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.debug(
      "Step completed: #{metadata.step_slug}, run_id: #{metadata.run_id}, duration: #{duration_ms}ms"
    )
  end

  def handle_telemetry_event([:pgflow, :step, :failed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.warning(
      "Step failed: #{metadata.step_slug}, run_id: #{metadata.run_id}, " <>
        "error: #{metadata.error}, duration: #{duration_ms}ms"
    )
  end

  def handle_telemetry_event([:pgflow, :task, :started], _measurements, metadata, _config) do
    Logger.debug(
      "Task started: #{metadata.step_slug}, run_id: #{metadata.run_id}, task_index: #{metadata.task_index}"
    )
  end

  def handle_telemetry_event([:pgflow, :task, :completed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.debug(
      "Task completed: #{metadata.step_slug}, run_id: #{metadata.run_id}, " <>
        "task_index: #{metadata.task_index}, duration: #{duration_ms}ms"
    )
  end

  def handle_telemetry_event([:pgflow, :task, :failed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.warning(
      "Task failed: #{metadata.step_slug}, run_id: #{metadata.run_id}, " <>
        "task_index: #{metadata.task_index}, error: #{metadata.error}, duration: #{duration_ms}ms"
    )
  end

  def handle_telemetry_event([:pgflow, :worker, :started], _measurements, metadata, _config) do
    Logger.info("Worker started: #{metadata.flow_slug}")
  end

  def handle_telemetry_event([:pgflow, :worker, :stopped], _measurements, metadata, _config) do
    Logger.info("Worker stopped: #{metadata.flow_slug}")
  end

  def handle_telemetry_event([:pgflow, :worker, :poll], measurements, metadata, _config) do
    Logger.debug(
      "Worker poll: #{metadata.flow_slug}, messages: #{measurements.message_count}, " <>
        "duration: #{measurements.duration}ms"
    )
  end

  def handle_telemetry_event([:pgflow, :worker, :error], _measurements, metadata, _config) do
    Logger.error("Worker error: #{metadata.flow_slug}, error: #{metadata.error}")
  end
end
