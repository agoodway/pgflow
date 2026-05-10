defmodule PgFlowDashboard.Live.LiveHelpers do
  @moduledoc """
  Shared hooks and utilities for PgFlowDashboard LiveViews.

  Provides common functionality for configuration access and real-time subscriptions.
  """

  require Logger

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

  def format_timestamp(%DateTime{} = dt, time_zone) when time_zone in ["UTC", "Etc/UTC"] do
    Calendar.strftime(dt, "%Y-%m-%d %H:%M:%S")
  end

  def format_timestamp(%DateTime{} = dt, time_zone) do
    case DateTime.shift_zone(dt, time_zone, Tz.TimeZoneDatabase) do
      {:ok, shifted} ->
        Calendar.strftime(shifted, "%Y-%m-%d %H:%M:%S")

      {:error, reason} ->
        Logger.warning(
          "format_timestamp: invalid time_zone #{inspect(time_zone)}: #{inspect(reason)}"
        )

        Calendar.strftime(dt, "%Y-%m-%d %H:%M:%S")
    end
  end

  def format_timestamp(%NaiveDateTime{} = ndt, time_zone) do
    ndt
    |> DateTime.from_naive!("Etc/UTC")
    |> format_timestamp(time_zone)
  end

  @doc """
  Formats a datetime relative to now.

  Returns strings like "in 5 minutes", "in 2 hours", "3 minutes ago".
  """
  def format_relative_time(nil), do: "-"

  def format_relative_time(%DateTime{} = dt), do: relative_from_now(dt)

  def format_relative_time(%NaiveDateTime{} = ndt) do
    ndt
    |> DateTime.from_naive!("Etc/UTC")
    |> relative_from_now()
  end

  @doc """
  Formats a duration in milliseconds for display.

  Formats in a compact style suitable for dashboards: "50ms", "1.5s", "2.3m", "1.2h".
  """
  def format_duration(nil), do: "-"

  def format_duration(ms) when is_struct(ms, Decimal),
    do: ms |> Decimal.to_float() |> format_duration()

  def format_duration(ms) when is_float(ms), do: format_duration(round(ms))

  def format_duration(ms) when is_integer(ms) do
    total_seconds = ms / 1000

    cond do
      total_seconds < 1 -> "#{ms}ms"
      total_seconds < 60 -> "#{Float.round(total_seconds, 1)}s"
      total_seconds < 3600 -> "#{Float.round(total_seconds / 60, 1)}m"
      true -> "#{Float.round(total_seconds / 3600, 1)}h"
    end
  end

  defp relative_from_now(dt) do
    diff = DateTime.diff(dt, DateTime.utc_now(), :second)

    {abs_diff, suffix, prefix} =
      if diff >= 0, do: {diff, "", "in "}, else: {abs(diff), " ago", ""}

    relative_time(abs_diff, prefix, suffix)
  end

  defp relative_time(abs_diff, prefix, suffix) when abs_diff < 3600 do
    cond do
      abs_diff < 5 -> "just now"
      abs_diff < 60 -> "#{prefix}#{abs_diff} seconds#{suffix}"
      abs_diff < 120 -> "#{prefix}1 minute#{suffix}"
      true -> "#{prefix}#{div(abs_diff, 60)} minutes#{suffix}"
    end
  end

  defp relative_time(abs_diff, prefix, suffix) when abs_diff < 172_800 do
    cond do
      abs_diff < 7200 -> "#{prefix}1 hour#{suffix}"
      abs_diff < 86_400 -> "#{prefix}#{div(abs_diff, 3600)} hours#{suffix}"
      true -> "#{prefix}1 day#{suffix}"
    end
  end

  defp relative_time(abs_diff, prefix, suffix) when abs_diff < 63_072_000 do
    cond do
      abs_diff < 2_592_000 -> "#{prefix}#{div(abs_diff, 86_400)} days#{suffix}"
      abs_diff < 5_184_000 -> "#{prefix}1 month#{suffix}"
      abs_diff < 31_536_000 -> "#{prefix}#{div(abs_diff, 2_592_000)} months#{suffix}"
      true -> "#{prefix}1 year#{suffix}"
    end
  end

  defp relative_time(abs_diff, prefix, suffix),
    do: "#{prefix}#{div(abs_diff, 31_536_000)} years#{suffix}"

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

  @doc """
  Splits a list fetched with limit+1 into results and has_more flag.

  When fetching paginated data, request `page_size + 1` items. This function
  splits the result to determine if there are more items available.

  ## Examples

      iex> paginate_results([1, 2, 3], 2)
      {[1, 2], true}

      iex> paginate_results([1, 2], 2)
      {[1, 2], false}

  """
  def paginate_results(items, page_size) do
    if length(items) > page_size do
      {Enum.take(items, page_size), true}
    else
      {items, false}
    end
  end

  @doc """
  Determines view mode based on count and threshold.

  Returns `:card` for small datasets (≤ threshold) and `:list` for larger ones.
  Default threshold is 12 (fits nicely in a 3-column grid).

  ## Examples

      iex> determine_view_mode(5)
      :card

      iex> determine_view_mode(15)
      :list

      iex> determine_view_mode(15, 20)
      :card

  """
  def determine_view_mode(count, threshold \\ 12) do
    if count <= threshold, do: :card, else: :list
  end
end
