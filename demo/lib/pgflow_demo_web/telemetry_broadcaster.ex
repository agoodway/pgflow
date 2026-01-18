defmodule PgflowDemoWeb.TelemetryBroadcaster do
  @moduledoc """
  Bridges PgFlow telemetry events to Phoenix.PubSub for real-time LiveView updates.

  Attaches to PgFlow telemetry events and broadcasts them to topics like:
  - `"pgflow:run:<run_id>"` - All events for a specific run

  ## Usage

  Call `attach/0` in your application's start function to begin broadcasting.
  """

  require Logger

  @pubsub PgflowDemo.PubSub

  @doc """
  Attaches telemetry handlers to broadcast PgFlow events via PubSub.
  """
  def attach do
    events = [
      [:pgflow, :worker, :task, :start],
      [:pgflow, :worker, :task, :stop],
      [:pgflow, :worker, :task, :exception],
      [:pgflow, :run, :started],
      [:pgflow, :run, :completed],
      [:pgflow, :run, :failed]
    ]

    :telemetry.attach_many(
      "pgflow-demo-broadcaster",
      events,
      &__MODULE__.handle_event/4,
      %{}
    )
  end

  @doc """
  Detaches the telemetry handlers.
  """
  def detach do
    :telemetry.detach("pgflow-demo-broadcaster")
  end

  @doc """
  Returns the PubSub topic for a given run_id.
  """
  def topic(run_id), do: "pgflow:run:#{run_id}"

  # Telemetry event handlers

  def handle_event([:pgflow, :worker, :task, :start], _measurements, metadata, _config) do
    broadcast(metadata.run_id, {:task_started, metadata.step_slug, metadata.task_index})
  end

  def handle_event([:pgflow, :worker, :task, :stop], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    broadcast(metadata.run_id, {
      :task_completed,
      metadata.step_slug,
      metadata.task_index,
      duration_ms,
      metadata[:output]
    })
  end

  def handle_event([:pgflow, :worker, :task, :exception], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    broadcast(metadata.run_id, {
      :task_failed,
      metadata.step_slug,
      metadata.task_index,
      metadata[:error],
      duration_ms
    })
  end

  def handle_event([:pgflow, :run, :started], _measurements, metadata, _config) do
    broadcast(metadata.run_id, {:run_started, metadata.flow_slug})
  end

  def handle_event([:pgflow, :run, :completed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)
    broadcast(metadata.run_id, {:run_completed, duration_ms})
  end

  def handle_event([:pgflow, :run, :failed], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)
    broadcast(metadata.run_id, {:run_failed, metadata[:error], duration_ms})
  end

  # Catch-all for any unhandled events
  def handle_event(_event, _measurements, _metadata, _config) do
    :ok
  end

  # Private helpers

  defp broadcast(run_id, message) do
    # Normalize run_id to string (might come as binary UUID from telemetry)
    run_id_str = normalize_run_id(run_id)
    Logger.debug("Broadcasting #{inspect(message)} to topic #{topic(run_id_str)}")
    Phoenix.PubSub.broadcast(@pubsub, topic(run_id_str), message)
  end

  defp normalize_run_id(run_id) when is_binary(run_id) and byte_size(run_id) == 16 do
    # Binary UUID - convert to string
    {:ok, str} = Ecto.UUID.load(run_id)
    str
  end

  defp normalize_run_id(run_id) when is_binary(run_id), do: run_id
  defp normalize_run_id(run_id), do: to_string(run_id)
end
