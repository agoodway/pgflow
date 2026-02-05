defmodule PgFlowDashboard.Live.LiveHelpers do
  @moduledoc """
  Shared hooks and utilities for PgFlowDashboard LiveViews.

  Provides common functionality for configuration access and real-time subscriptions.
  """

  import Phoenix.Component
  import Phoenix.LiveView

  @doc """
  Assigns dashboard configuration for LiveViews.

  This function should be called in the `on_mount` callback:

      def on_mount(:default, _params, session, socket) do
        PgFlowDashboard.Live.LiveHelpers.on_mount(session, socket)
      end

  """
  def on_mount(session, socket) do
    config = session["pgflow_dashboard_config"]

    socket =
      socket
      |> assign(:config, config)
      |> assign(:repo, config[:repo])
      |> assign(:pubsub, config[:pubsub])
      |> assign(:time_zone, config[:time_zone])
      |> assign(:refresh_interval, config[:refresh_interval])
      |> assign(:enable_pubsub, config[:enable_pubsub])

    {:cont, socket}
  end

  @doc """
  Subscribes to PgFlow PubSub topics for real-time updates.
  """
  def subscribe_to_updates(socket) do
    if socket.assigns.enable_pubsub && connected?(socket) do
      pubsub = socket.assigns.pubsub
      Phoenix.PubSub.subscribe(pubsub, "pgflow:runs")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:workers")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:tasks")
    end

    socket
  end

  @doc """
  Subscribes to updates for a specific run.
  """
  def subscribe_to_run(socket, run_id) do
    if socket.assigns.enable_pubsub && connected?(socket) do
      pubsub = socket.assigns.pubsub
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")
    end

    socket
  end

  @doc """
  Unsubscribes from a specific run's updates.
  """
  def unsubscribe_from_run(socket, run_id) do
    if socket.assigns.enable_pubsub do
      pubsub = socket.assigns.pubsub
      Phoenix.PubSub.unsubscribe(pubsub, "pgflow:run:#{run_id}")
    end

    socket
  end

  @doc """
  Schedules a refresh timer for polling updates.
  """
  def schedule_refresh(socket) do
    if connected?(socket) do
      Process.send_after(self(), :refresh, socket.assigns.refresh_interval)
    end

    socket
  end

  @doc """
  Formats a timestamp for display in the configured time zone.
  """
  def format_timestamp(nil, _time_zone), do: "-"

  def format_timestamp(%DateTime{} = dt, "UTC") do
    Calendar.strftime(dt, "%Y-%m-%d %H:%M:%S")
  end

  def format_timestamp(%DateTime{} = dt, "Etc/UTC") do
    Calendar.strftime(dt, "%Y-%m-%d %H:%M:%S")
  end

  def format_timestamp(%DateTime{} = dt, time_zone) do
    case DateTime.shift_zone(dt, time_zone) do
      {:ok, shifted} -> Calendar.strftime(shifted, "%Y-%m-%d %H:%M:%S")
      {:error, _} -> Calendar.strftime(dt, "%Y-%m-%d %H:%M:%S")
    end
  end

  def format_timestamp(%NaiveDateTime{} = ndt, time_zone) do
    ndt
    |> DateTime.from_naive!("Etc/UTC")
    |> format_timestamp(time_zone)
  end

  @doc """
  Formats a duration in milliseconds for display.
  """
  def format_duration(nil), do: "-"
  def format_duration(ms) when is_struct(ms, Decimal), do: ms |> Decimal.to_float() |> format_duration()
  def format_duration(ms) when is_float(ms), do: format_duration(round(ms))
  def format_duration(ms) when is_integer(ms) and ms < 1000, do: "#{ms}ms"
  def format_duration(ms) when is_integer(ms) and ms < 60_000, do: "#{Float.round(ms / 1000, 1)}s"
  def format_duration(ms) when is_integer(ms) and ms < 3_600_000, do: "#{Float.round(ms / 60_000, 1)}m"
  def format_duration(ms) when is_integer(ms), do: "#{Float.round(ms / 3_600_000, 1)}h"

  @doc """
  Returns a short form of a UUID for display.
  """
  def short_id(nil), do: "-"
  def short_id(id) when is_binary(id), do: String.slice(id, 0..7)

  @doc """
  Returns CSS classes for a status badge.
  """
  def status_classes(status), do: status |> normalize_status() |> do_status_classes()

  defp do_status_classes(:completed) do
    "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-400"
  end

  defp do_status_classes(:failed) do
    "bg-rose-100 text-rose-800 dark:bg-rose-900/30 dark:text-rose-400"
  end

  defp do_status_classes(:started) do
    "bg-sky-100 text-sky-800 dark:bg-sky-900/30 dark:text-sky-400"
  end

  defp do_status_classes(_) do
    "bg-slate-100 text-slate-800 dark:bg-slate-900/30 dark:text-slate-400"
  end

  @doc """
  Returns the color for a status in hex format (for SVG).
  """
  def status_color(status), do: status |> normalize_status() |> do_status_color()

  defp do_status_color(:completed), do: "#059669"
  defp do_status_color(:failed), do: "#e11d48"
  defp do_status_color(:started), do: "#0284c7"
  defp do_status_color(_), do: "#64748b"

  defp normalize_status(status) when is_atom(status), do: status
  defp normalize_status("completed"), do: :completed
  defp normalize_status("failed"), do: :failed
  defp normalize_status("started"), do: :started
  defp normalize_status("created"), do: :created
  defp normalize_status(_), do: :unknown
end
