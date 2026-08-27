defmodule PgFlow.Telemetry.PubSub do
  @moduledoc """
  Bridges PgFlow telemetry events to Phoenix.PubSub.

  Attaches to all PgFlow telemetry events and broadcasts them as
  `{:pgflow, run_id, event_payload}` tuples to per-run and global topics.

  ## Topics

    * `"pgflow:run:<run_id>"` — all events for a specific run
    * `"pgflow:runs"` — all run lifecycle events (started, completed, failed)
    * `"pgflow:tasks"` — all task events (started, completed, failed) and step skips

  ## Usage

  Typically started automatically by `PgFlow.Supervisor` when `:pubsub` is configured:

      children = [
        {PgFlow.Supervisor, pubsub: MyApp.PubSub, ...}
      ]

  Can also be attached manually:

      PgFlow.Telemetry.PubSub.attach(pubsub: MyApp.PubSub)

  """

  alias PgFlow.Queries.Helpers
  require Logger

  @handler_id "pgflow-telemetry-pubsub"

  @task_events [
    [:pgflow, :worker, :task, :start],
    [:pgflow, :worker, :task, :stop],
    [:pgflow, :worker, :task, :exception],
    [:pgflow, :worker, :task, :waiting],
    [:pgflow, :step, :skipped]
  ]

  @run_events [
    [:pgflow, :run, :started],
    [:pgflow, :run, :completed],
    [:pgflow, :run, :failed]
  ]

  @doc """
  Attaches telemetry handlers that broadcast events to PubSub.

  ## Options

    * `:pubsub` — (required) the Phoenix.PubSub module to broadcast on
  """
  @spec attach(keyword()) :: :ok | {:error, :already_exists}
  def attach(opts) do
    pubsub = Keyword.fetch!(opts, :pubsub)

    :telemetry.attach_many(
      @handler_id,
      @task_events ++ @run_events,
      &__MODULE__.handle_event/4,
      %{pubsub: pubsub}
    )
  end

  @doc """
  Detaches the telemetry handlers.
  """
  @spec detach() :: :ok | {:error, :not_found}
  def detach do
    :telemetry.detach(@handler_id)
  end

  @doc false
  def handle_event([:pgflow, :worker, :task, :start], _measurements, metadata, config) do
    run_id = normalize_uuid(metadata.run_id)

    payload =
      {:task_started,
       %{
         step_slug: metadata.step_slug,
         task_index: metadata.task_index,
         timestamp: DateTime.utc_now()
       }}

    broadcast(config.pubsub, run_id, payload, :task)
  end

  def handle_event([:pgflow, :worker, :task, :waiting], _measurements, metadata, config) do
    run_id = normalize_uuid(metadata.run_id)

    payload =
      {:task_waiting,
       %{
         step_slug: metadata.step_slug,
         task_index: metadata.task_index,
         timestamp: DateTime.utc_now()
       }}

    broadcast(config.pubsub, run_id, payload, :task)
  end

  def handle_event([:pgflow, :worker, :task, :stop], measurements, metadata, config) do
    run_id = normalize_uuid(metadata.run_id)
    duration_ms = duration_to_ms(measurements[:duration])

    payload =
      {:task_completed,
       %{
         step_slug: metadata.step_slug,
         task_index: metadata.task_index,
         output: metadata[:output],
         duration_ms: duration_ms,
         timestamp: DateTime.utc_now()
       }}

    broadcast(config.pubsub, run_id, payload, :task)
  end

  def handle_event([:pgflow, :worker, :task, :exception], measurements, metadata, config) do
    run_id = normalize_uuid(metadata.run_id)
    duration_ms = duration_to_ms(measurements[:duration])

    error =
      case metadata[:reason] do
        msg when is_binary(msg) -> msg
        other -> inspect(other)
      end

    payload =
      {:task_failed,
       %{
         step_slug: metadata.step_slug,
         task_index: metadata.task_index,
         error: error,
         duration_ms: duration_ms,
         timestamp: DateTime.utc_now()
       }}

    broadcast(config.pubsub, run_id, payload, :task)
  end

  def handle_event([:pgflow, :step, :skipped], _measurements, metadata, config) do
    run_id = normalize_uuid(metadata.run_id)

    payload =
      {:step_skipped,
       %{
         step_slug: metadata.step_slug,
         skip_reason: metadata[:skip_reason],
         timestamp: DateTime.utc_now()
       }}

    broadcast(config.pubsub, run_id, payload, :task)
  end

  def handle_event([:pgflow, :run, :started], _measurements, metadata, config) do
    run_id = normalize_uuid(metadata.run_id)

    payload =
      {:run_started,
       %{
         flow_slug: metadata.flow_slug,
         timestamp: DateTime.utc_now()
       }}

    broadcast(config.pubsub, run_id, payload, :run)
  end

  def handle_event([:pgflow, :run, :completed], _measurements, metadata, config) do
    run_id = normalize_uuid(metadata.run_id)

    payload =
      {:run_completed,
       %{
         output: metadata[:output],
         timestamp: DateTime.utc_now()
       }}

    broadcast(config.pubsub, run_id, payload, :run)
  end

  def handle_event([:pgflow, :run, :failed], _measurements, metadata, config) do
    run_id = normalize_uuid(metadata.run_id)

    error =
      case metadata[:error] do
        msg when is_binary(msg) -> msg
        other -> inspect(other)
      end

    payload =
      {:run_failed,
       %{
         error: error,
         timestamp: DateTime.utc_now()
       }}

    broadcast(config.pubsub, run_id, payload, :run)
  end

  # Broadcasts to per-run topic and the appropriate global topic
  defp broadcast(pubsub, run_id, payload, event_type) do
    message = {:pgflow, run_id, payload}
    global_topic = global_topic(event_type)

    broadcast_safe(pubsub, "pgflow:run:#{run_id}", message)
    broadcast_safe(pubsub, global_topic, message)
  end

  defp broadcast_safe(pubsub, topic, message) do
    case Phoenix.PubSub.broadcast(pubsub, topic, message) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("PgFlow PubSub broadcast failed on #{topic}: #{inspect(reason)}")
    end
  end

  defp global_topic(:run), do: "pgflow:runs"
  defp global_topic(:task), do: "pgflow:tasks"

  # Normalizes binary UUIDs (16-byte binaries) to hyphenated string format.
  # Passes through strings unchanged. Delegates to Helpers.format_uuid/1
  # which uses Ecto.UUID.load/1 (not load!) to avoid crashing telemetry handlers.
  defp normalize_uuid(uuid), do: Helpers.format_uuid(uuid)

  defp duration_to_ms(nil), do: nil

  defp duration_to_ms(duration) do
    System.convert_time_unit(duration, :native, :millisecond)
  end
end
