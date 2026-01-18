defmodule PgFlow.Telemetry do
  @moduledoc """
  Telemetry events emitted by PgFlow.

  PgFlow uses `:telemetry` to emit events at key points in the workflow lifecycle.
  These events can be used for monitoring, logging, and metrics collection.

  ## Event Naming Convention

  All events are prefixed with `[:pgflow, ...]` and follow this pattern:
  - `[:pgflow, :worker, :start | :stop]` - Worker lifecycle
  - `[:pgflow, :poll, :start | :stop]` - Polling cycles
  - `[:pgflow, :task, :start | :stop | :exception]` - Task execution
  - `[:pgflow, :run, :started | :completed | :failed]` - Run lifecycle

  ## Attaching Handlers

      :telemetry.attach_many(
        "my-handler",
        [
          [:pgflow, :task, :stop],
          [:pgflow, :run, :completed],
          [:pgflow, :run, :failed]
        ],
        &MyModule.handle_event/4,
        nil
      )

  ## Default Logger

  PgFlow includes a default logger handler that can be attached by setting
  `attach_default_logger: true` in the configuration.

  Note: The default logger is disabled by default since `PgFlow.Logger` provides
  structured logging directly in the worker. Enable this if you need telemetry-based
  logging for specific use cases like metrics collection or external log aggregation.
  """

  alias PgFlow.Logger, as: PgLogger

  @doc """
  Attaches the default telemetry handlers for logging.

  This is called automatically on application start if `attach_default_logger: true`
  is set in the configuration.
  """
  @spec attach_default_logger() :: :ok | {:error, :already_exists}
  def attach_default_logger do
    events = [
      [:pgflow, :worker, :start],
      [:pgflow, :worker, :stop],
      [:pgflow, :poll, :start],
      [:pgflow, :poll, :stop],
      [:pgflow, :task, :start],
      [:pgflow, :task, :stop],
      [:pgflow, :task, :exception],
      [:pgflow, :run, :started],
      [:pgflow, :run, :completed],
      [:pgflow, :run, :failed]
    ]

    :telemetry.attach_many(
      "pgflow-default-logger",
      events,
      &__MODULE__.handle_event/4,
      %{}
    )
  end

  @doc """
  Detaches the default telemetry handlers.
  """
  @spec detach_default_logger() :: :ok | {:error, :not_found}
  def detach_default_logger do
    :telemetry.detach("pgflow-default-logger")
  end

  @doc false
  # Worker lifecycle events - minimal logging since Worker.Server handles startup banner
  def handle_event([:pgflow, :worker, :start], _measurements, metadata, _config) do
    require Logger
    Logger.debug("[Telemetry] Worker started for flow #{metadata.flow_slug}")
  end

  def handle_event([:pgflow, :worker, :stop], measurements, metadata, _config) do
    require Logger
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    Logger.debug(
      "[Telemetry] Worker stopped for flow #{metadata.flow_slug} after #{duration_ms}ms"
    )
  end

  # Poll events - minimal logging since Worker.Server handles structured polling logs
  def handle_event([:pgflow, :poll, :start], _measurements, _metadata, _config) do
    # No-op: Worker.Server handles polling logs via PgFlow.Logger
    :ok
  end

  def handle_event([:pgflow, :poll, :stop], _measurements, _metadata, _config) do
    # No-op: Worker.Server handles polling logs via PgFlow.Logger
    :ok
  end

  # Task events - no-op since Worker.Server handles structured task logging
  def handle_event([:pgflow, :task, :start], _measurements, _metadata, _config) do
    # No-op: Worker.Server handles task_started via PgFlow.Logger
    :ok
  end

  def handle_event([:pgflow, :task, :stop], _measurements, _metadata, _config) do
    # No-op: Worker.Server handles task_completed via PgFlow.Logger
    :ok
  end

  def handle_event([:pgflow, :task, :exception], _measurements, _metadata, _config) do
    # No-op: Worker.Server handles task_failed via PgFlow.Logger
    :ok
  end

  # Run lifecycle events - use PgFlow.Logger for consistency
  def handle_event([:pgflow, :run, :started], _measurements, metadata, _config) do
    PgLogger.run_started(metadata.flow_slug, metadata.run_id)
  end

  def handle_event([:pgflow, :run, :completed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)
    PgLogger.run_completed(metadata.flow_slug, metadata.run_id, duration_ms)
  end

  def handle_event([:pgflow, :run, :failed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)
    error = metadata[:error] || "Unknown error"
    PgLogger.run_failed(metadata.flow_slug, metadata.run_id, duration_ms, error)
  end

  # Catch-all for any unhandled events
  def handle_event(_event, _measurements, _metadata, _config) do
    :ok
  end

  # Helper functions for emitting events

  @doc """
  Emits a worker start event.
  """
  @spec emit_worker_start(atom(), term()) :: :ok
  def emit_worker_start(flow_slug, worker_id) do
    :telemetry.execute(
      [:pgflow, :worker, :start],
      %{system_time: System.system_time()},
      %{flow_slug: flow_slug, worker_id: worker_id}
    )
  end

  @doc """
  Emits a worker stop event.
  """
  @spec emit_worker_stop(atom(), term(), integer()) :: :ok
  def emit_worker_stop(flow_slug, worker_id, start_time) do
    :telemetry.execute(
      [:pgflow, :worker, :stop],
      %{duration: System.monotonic_time() - start_time},
      %{flow_slug: flow_slug, worker_id: worker_id}
    )
  end

  @doc """
  Emits a poll start event.
  """
  @spec emit_poll_start(atom(), term()) :: :ok
  def emit_poll_start(flow_slug, worker_id) do
    :telemetry.execute(
      [:pgflow, :poll, :start],
      %{system_time: System.system_time()},
      %{flow_slug: flow_slug, worker_id: worker_id}
    )
  end

  @doc """
  Emits a poll stop event.
  """
  @spec emit_poll_stop(atom(), term(), integer(), non_neg_integer()) :: :ok
  def emit_poll_stop(flow_slug, worker_id, start_time, task_count) do
    :telemetry.execute(
      [:pgflow, :poll, :stop],
      %{duration: System.monotonic_time() - start_time, task_count: task_count},
      %{flow_slug: flow_slug, worker_id: worker_id}
    )
  end

  @doc """
  Emits a task start event.
  """
  @spec emit_task_start(atom(), String.t(), atom(), non_neg_integer()) :: :ok
  def emit_task_start(flow_slug, run_id, step_slug, task_index) do
    :telemetry.execute(
      [:pgflow, :task, :start],
      %{system_time: System.system_time()},
      %{flow_slug: flow_slug, run_id: run_id, step_slug: step_slug, task_index: task_index}
    )
  end

  @doc """
  Emits a task stop event.
  """
  @spec emit_task_stop(atom(), String.t(), atom(), non_neg_integer(), integer()) :: :ok
  def emit_task_stop(flow_slug, run_id, step_slug, task_index, start_time) do
    :telemetry.execute(
      [:pgflow, :task, :stop],
      %{duration: System.monotonic_time() - start_time},
      %{flow_slug: flow_slug, run_id: run_id, step_slug: step_slug, task_index: task_index}
    )
  end

  @doc """
  Emits a task exception event.
  """
  @spec emit_task_exception(atom(), String.t(), atom(), non_neg_integer(), integer(), term()) ::
          :ok
  def emit_task_exception(flow_slug, run_id, step_slug, task_index, start_time, error) do
    :telemetry.execute(
      [:pgflow, :task, :exception],
      %{duration: System.monotonic_time() - start_time},
      %{
        flow_slug: flow_slug,
        run_id: run_id,
        step_slug: step_slug,
        task_index: task_index,
        error: error
      }
    )
  end

  @doc """
  Emits a run started event.
  """
  @spec emit_run_started(atom(), String.t()) :: :ok
  def emit_run_started(flow_slug, run_id) do
    :telemetry.execute(
      [:pgflow, :run, :started],
      %{system_time: System.system_time()},
      %{flow_slug: flow_slug, run_id: run_id}
    )
  end

  @doc """
  Emits a run completed event.
  """
  @spec emit_run_completed(atom(), String.t(), integer()) :: :ok
  def emit_run_completed(flow_slug, run_id, start_time) do
    :telemetry.execute(
      [:pgflow, :run, :completed],
      %{duration: System.monotonic_time() - start_time},
      %{flow_slug: flow_slug, run_id: run_id}
    )
  end

  @doc """
  Emits a run failed event.
  """
  @spec emit_run_failed(atom(), String.t(), integer(), term()) :: :ok
  def emit_run_failed(flow_slug, run_id, start_time, error) do
    :telemetry.execute(
      [:pgflow, :run, :failed],
      %{duration: System.monotonic_time() - start_time},
      %{flow_slug: flow_slug, run_id: run_id, error: error}
    )
  end
end
