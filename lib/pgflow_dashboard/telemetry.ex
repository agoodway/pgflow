defmodule PgFlowDashboard.Telemetry do
  @moduledoc """
  Bridges PgFlow telemetry events to Phoenix.PubSub for real-time dashboard updates.

  Attaches to PgFlow telemetry events and broadcasts them to topics:
  - `"pgflow:runs"` - Run start/complete/fail events
  - `"pgflow:workers"` - Worker heartbeat events
  - `"pgflow:tasks"` - Task start/complete/fail events
  - `"pgflow:run:<run_id>"` - All events for a specific run

  Also invalidates cache entries when relevant events occur.
  """

  require Logger

  alias PgFlowDashboard.Cache.MetricsCache

  @doc """
  Attaches telemetry handlers for the dashboard.

  Should be called during application startup.

  ## Options

    * `:pubsub` - Required. The Phoenix.PubSub module to broadcast to.
    * `:handler_id` - Optional. Telemetry handler ID. Default: "pgflow-dashboard".

  """
  @spec attach(keyword()) :: :ok
  def attach(opts) do
    pubsub = Keyword.fetch!(opts, :pubsub)
    handler_id = Keyword.get(opts, :handler_id, "pgflow-dashboard")

    events = [
      [:pgflow, :worker, :task, :start],
      [:pgflow, :worker, :task, :stop],
      [:pgflow, :worker, :task, :exception],
      [:pgflow, :run, :started],
      [:pgflow, :run, :completed],
      [:pgflow, :run, :failed],
      [:pgflow, :worker, :heartbeat]
    ]

    :telemetry.attach_many(
      handler_id,
      events,
      &__MODULE__.handle_event/4,
      %{pubsub: pubsub}
    )

    :ok
  end

  @doc """
  Detaches the telemetry handlers.
  """
  @spec detach(String.t()) :: :ok | {:error, :not_found}
  def detach(handler_id \\ "pgflow-dashboard") do
    :telemetry.detach(handler_id)
  end

  @doc """
  Returns the PubSub topic for a specific run.
  """
  @spec run_topic(String.t()) :: String.t()
  def run_topic(run_id), do: "pgflow:run:#{run_id}"

  # Telemetry event handlers

  def handle_event([:pgflow, :worker, :task, :start], _measurements, metadata, config) do
    run_id = normalize_run_id(metadata.run_id)

    message = {
      :task_started,
      %{
        run_id: run_id,
        step_slug: metadata.step_slug,
        task_index: metadata.task_index,
        timestamp: DateTime.utc_now()
      }
    }

    broadcast(config.pubsub, "pgflow:tasks", message)
    broadcast(config.pubsub, run_topic(run_id), message)
  end

  def handle_event([:pgflow, :worker, :task, :stop], measurements, metadata, config) do
    run_id = normalize_run_id(metadata.run_id)
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    message = {
      :task_completed,
      %{
        run_id: run_id,
        step_slug: metadata.step_slug,
        task_index: metadata.task_index,
        duration_ms: duration_ms,
        timestamp: DateTime.utc_now()
      }
    }

    broadcast(config.pubsub, "pgflow:tasks", message)
    broadcast(config.pubsub, run_topic(run_id), message)
  end

  def handle_event([:pgflow, :worker, :task, :exception], measurements, metadata, config) do
    run_id = normalize_run_id(metadata.run_id)
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    message = {
      :task_failed,
      %{
        run_id: run_id,
        step_slug: metadata.step_slug,
        task_index: metadata.task_index,
        error: format_error(metadata[:error]),
        duration_ms: duration_ms,
        timestamp: DateTime.utc_now()
      }
    }

    broadcast(config.pubsub, "pgflow:tasks", message)
    broadcast(config.pubsub, run_topic(run_id), message)
  end

  def handle_event([:pgflow, :run, :started], _measurements, metadata, config) do
    run_id = normalize_run_id(metadata.run_id)

    message = {
      :run_started,
      %{
        run_id: run_id,
        flow_slug: metadata.flow_slug,
        timestamp: DateTime.utc_now()
      }
    }

    # Invalidate overview cache
    MetricsCache.invalidate(:overview_metrics)

    broadcast(config.pubsub, "pgflow:runs", message)
    broadcast(config.pubsub, run_topic(run_id), message)
  end

  def handle_event([:pgflow, :run, :completed], measurements, metadata, config) do
    run_id = normalize_run_id(metadata.run_id)
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    message = {
      :run_completed,
      %{
        run_id: run_id,
        duration_ms: duration_ms,
        timestamp: DateTime.utc_now()
      }
    }

    # Invalidate caches
    MetricsCache.invalidate(:overview_metrics)
    MetricsCache.invalidate({:flow_stats, metadata[:flow_slug]})

    broadcast(config.pubsub, "pgflow:runs", message)
    broadcast(config.pubsub, run_topic(run_id), message)
  end

  def handle_event([:pgflow, :run, :failed], measurements, metadata, config) do
    run_id = normalize_run_id(metadata.run_id)
    duration_ms = System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    message = {
      :run_failed,
      %{
        run_id: run_id,
        error: format_error(metadata[:error]),
        duration_ms: duration_ms,
        timestamp: DateTime.utc_now()
      }
    }

    # Invalidate caches
    MetricsCache.invalidate(:overview_metrics)
    MetricsCache.invalidate({:flow_stats, metadata[:flow_slug]})

    broadcast(config.pubsub, "pgflow:runs", message)
    broadcast(config.pubsub, run_topic(run_id), message)
  end

  def handle_event([:pgflow, :worker, :heartbeat], _measurements, metadata, config) do
    message = {
      :worker_heartbeat,
      %{
        worker_id: metadata[:worker_id],
        flow_slug: metadata[:flow_slug],
        timestamp: DateTime.utc_now()
      }
    }

    broadcast(config.pubsub, "pgflow:workers", message)
  end

  def handle_event(_event, _measurements, _metadata, _config) do
    :ok
  end

  # Private helpers

  defp broadcast(pubsub, topic, message) do
    Phoenix.PubSub.broadcast(pubsub, topic, message)
  rescue
    e ->
      Logger.warning("Failed to broadcast to #{topic}: #{inspect(e)}")
      :ok
  end

  defp normalize_run_id(run_id) when is_binary(run_id) and byte_size(run_id) == 16 do
    # Binary UUID - convert to string
    case Ecto.UUID.load(run_id) do
      {:ok, str} -> str
      :error -> Base.encode16(run_id, case: :lower)
    end
  end

  defp normalize_run_id(run_id) when is_binary(run_id), do: run_id
  defp normalize_run_id(run_id), do: to_string(run_id)

  defp format_error(nil), do: nil
  defp format_error(%{message: msg}), do: msg
  defp format_error(error) when is_binary(error), do: error
  defp format_error(error), do: inspect(error)
end
