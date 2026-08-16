defmodule PgFlow.Telemetry do
  @moduledoc """
  Telemetry events emitted by PgFlow.

  PgFlow uses `:telemetry` to emit events at key points in the workflow lifecycle.
  These events can be used for monitoring, logging, and metrics collection.

  ## Events

  ### Worker Lifecycle

  - `[:pgflow, :worker, :start]` — Worker process started
  - `[:pgflow, :worker, :stop]` — Worker process stopped

  ### Poll Cycles

  - `[:pgflow, :worker, :poll, :start]` — Poll cycle started
  - `[:pgflow, :worker, :poll, :stop]` — Poll cycle completed

  ### Task Execution

  - `[:pgflow, :worker, :task, :start]` — Task execution started
  - `[:pgflow, :worker, :task, :stop]` — Task execution completed successfully
  - `[:pgflow, :worker, :task, :exception]` — Task execution failed

  ### Step Lifecycle

  - `[:pgflow, :step, :skipped]` — Step skipped without a worker (e.g. unmet `if`/`if_not`)

  ### Run Lifecycle

  - `[:pgflow, :run, :started]` — Flow run created (emitted by `PgFlow.Client`)
  - `[:pgflow, :run, :completed]` — Flow run completed (emitted by worker after task cascades)
  - `[:pgflow, :run, :failed]` — Flow run failed (emitted by worker after task cascades)

  ## Attaching Handlers

      :telemetry.attach_many(
        "my-handler",
        [
          [:pgflow, :worker, :task, :stop],
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

  require Logger

  alias PgFlow.Logger, as: PgLogger
  alias PgFlow.Queries.Flows

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
      [:pgflow, :worker, :poll, :start],
      [:pgflow, :worker, :poll, :stop],
      [:pgflow, :worker, :task, :start],
      [:pgflow, :worker, :task, :stop],
      [:pgflow, :worker, :task, :exception],
      [:pgflow, :step, :skipped],
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

  @doc """
  Emits `[:pgflow, :step, :skipped]` when a step is skipped without a worker.

  ## Metadata

    * `:flow_slug` - Flow identifier
    * `:run_id` - Run UUID
    * `:step_slug` - Skipped step identifier
    * `:skip_reason` - Why the step was skipped, or `nil`
  """
  @spec emit_step_skipped(%{
          flow_slug: String.t(),
          run_id: String.t(),
          step_slug: String.t(),
          skip_reason: String.t() | nil
        }) :: :ok
  def emit_step_skipped(meta) do
    :telemetry.execute(
      [:pgflow, :step, :skipped],
      %{system_time: System.system_time()},
      meta
    )
  end

  @doc """
  Emits `[:pgflow, :step, :skipped]` for every skipped step on a run.

  One-shot form, for callers that observe a run exactly once (such as
  `PgFlow.Client.start_flow/2`, which sees the skips the run was born with).
  Callers that sweep the same run repeatedly must use
  `emit_skipped_steps/4` instead, or they will re-announce every skip on
  every sweep.
  """
  @spec emit_skipped_steps(Ecto.Repo.t(), String.t(), String.t()) :: :ok
  def emit_skipped_steps(repo, flow_slug, run_id) do
    emit_skipped_steps(repo, flow_slug, run_id, MapSet.new())
    :ok
  end

  @doc """
  Emits `[:pgflow, :step, :skipped]` for skips not already announced.

  Looks up skipped steps via `Flows.list_skipped_steps/2` (dependency
  ordered, so a parent's skip is always emitted before its cascaded
  children). `already_emitted` is a `MapSet` of step slugs this caller has
  already announced for `run_id`; the union of it and the slugs emitted by
  this call is returned, ready to be passed back in on the next sweep.

  Query errors are swallowed so callers can treat this as fire-and-forget;
  the set is returned unchanged in that case and the skips are picked up on
  a later sweep.

  ## Delivery contract

  Skips are decided in PostgreSQL, and PgFlow discovers them by polling
  `step_states` after each `complete_task`/`fail_task`. Passing the returned
  set back in makes a single emitter announce each skip exactly once — the
  guarantee non-idempotent handlers need.

  It is a per-emitter guarantee, not a global one. Two workers processing
  the same run each sweep it independently, so a skip can be announced once
  per worker that touches the run (and once by `Client.start_flow/2` for
  skips decided at run start). Handlers that must be globally exactly-once
  should dedupe on `{run_id, step_slug}`; `PgFlow.LiveClient` does this
  structurally by treating `step:skipped` as an idempotent state transition
  rather than a counter. Closing the gap properly means having the SQL
  return newly transitioned rows, which is a core pgflow change.
  """
  @spec emit_skipped_steps(Ecto.Repo.t(), String.t(), String.t(), MapSet.t(String.t())) ::
          MapSet.t(String.t())
  def emit_skipped_steps(repo, flow_slug, run_id, already_emitted) do
    case Flows.list_skipped_steps(repo, run_id) do
      {:ok, skipped} ->
        Enum.reduce(skipped, already_emitted, &emit_unseen_skip(&1, &2, flow_slug, run_id))

      {:error, _} ->
        already_emitted
    end
  end

  defp emit_unseen_skip(%{step_slug: slug, skip_reason: reason}, seen, flow_slug, run_id) do
    if MapSet.member?(seen, slug) do
      seen
    else
      emit_step_skipped(%{
        flow_slug: flow_slug,
        run_id: run_id,
        step_slug: slug,
        skip_reason: reason
      })

      MapSet.put(seen, slug)
    end
  end

  @doc false
  # Worker lifecycle events - minimal logging since Worker.Server handles startup banner
  def handle_event([:pgflow, :worker, :start], _measurements, metadata, _config) do
    Logger.debug("[Telemetry] Worker started for flow #{metadata.flow_slug}")
  end

  def handle_event([:pgflow, :worker, :stop], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    Logger.debug(
      "[Telemetry] Worker stopped for flow #{metadata.flow_slug} after #{duration_ms}ms"
    )
  end

  # Poll events - no-op since Worker.Server handles structured polling logs
  def handle_event([:pgflow, :worker, :poll, :start], _measurements, _metadata, _config) do
    :ok
  end

  def handle_event([:pgflow, :worker, :poll, :stop], _measurements, _metadata, _config) do
    :ok
  end

  # Task events - no-op since Worker.Server handles structured task logging
  def handle_event([:pgflow, :worker, :task, :start], _measurements, _metadata, _config) do
    :ok
  end

  def handle_event([:pgflow, :worker, :task, :stop], _measurements, _metadata, _config) do
    :ok
  end

  def handle_event([:pgflow, :worker, :task, :exception], _measurements, _metadata, _config) do
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
end
