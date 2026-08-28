defmodule PgFlowDashboard.Live.WorkersLive do
  @moduledoc """
  Workers monitoring page.

  Uses LiveView streams for efficient rendering of large lists.
  """

  use Phoenix.LiveView

  alias PgFlow.Workers
  alias PgFlowDashboard.Components.{HealthBadge, Layouts, TypeBadge}
  alias PgFlowDashboard.Live.LiveHelpers

  @page_size 50

  @impl true
  def mount(_params, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Workers")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:health_filter, nil)
      |> assign(:cursor, nil)
      |> assign(:has_more, false)
      |> assign(:total_count, 0)
      |> assign(:workers_count, 0)
      |> stream_configure(:workers, dom_id: &"worker-#{&1.worker_id}")
      |> stream(:workers, [])
      |> load_all_workers_summary()
      |> load_workers()
      |> LiveHelpers.subscribe_to_updates()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    socket =
      socket
      |> assign(:health_filter, params["health"])
      |> assign(:cursor, nil)
      |> load_workers(reset: true)

    {:noreply, socket}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_all_workers_summary()
      |> refresh_workers()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  def handle_info(_, socket), do: {:noreply, socket}

  @impl true
  def handle_event("load_more", _, socket) do
    {:ok, workers} =
      Workers.list(socket.assigns.repo,
        health_status: socket.assigns.health_filter,
        cursor: socket.assigns.cursor,
        limit: @page_size + 1
      )

    {workers, has_more} =
      if length(workers) > @page_size do
        {Enum.take(workers, @page_size), true}
      else
        {workers, false}
      end

    new_cursor = if workers != [], do: List.last(workers).worker_id, else: nil
    new_count = socket.assigns.workers_count + length(workers)

    socket =
      socket
      |> stream(:workers, workers)
      |> assign(:cursor, new_cursor)
      |> assign(:has_more, has_more)
      |> assign(:workers_count, new_count)

    {:noreply, socket}
  end

  defp load_workers(socket, opts \\ []) do
    reset = Keyword.get(opts, :reset, false)
    health_filter = socket.assigns.health_filter

    {:ok, workers} =
      Workers.list(socket.assigns.repo,
        health_status: health_filter,
        limit: @page_size + 1
      )

    {workers, has_more} =
      if length(workers) > @page_size do
        {Enum.take(workers, @page_size), true}
      else
        {workers, false}
      end

    cursor = if workers != [], do: List.last(workers).worker_id, else: nil
    {:ok, total_count} = Workers.count(socket.assigns.repo, health_status: health_filter)

    socket
    |> stream(:workers, workers, reset: reset)
    |> assign(:cursor, cursor)
    |> assign(:has_more, has_more)
    |> assign(:total_count, total_count)
    |> assign(:workers_count, length(workers))
  end

  defp refresh_workers(socket) do
    current_count = max(socket.assigns.workers_count, @page_size)
    health_filter = socket.assigns.health_filter

    {:ok, workers} =
      Workers.list(socket.assigns.repo,
        health_status: health_filter,
        limit: current_count + 1
      )

    {workers, has_more} =
      if length(workers) > current_count do
        {Enum.take(workers, current_count), true}
      else
        {workers, false}
      end

    cursor = if workers != [], do: List.last(workers).worker_id, else: nil

    socket
    |> stream(:workers, workers, reset: true)
    |> assign(:cursor, cursor)
    |> assign(:has_more, has_more)
    |> assign(:workers_count, length(workers))
  end

  defp load_all_workers_summary(socket) do
    summary = %{
      healthy: worker_count(socket.assigns.repo, "healthy"),
      stale: worker_count(socket.assigns.repo, "stale"),
      dead: worker_count(socket.assigns.repo, "dead")
    }

    assign(socket, :summary, summary)
  end

  defp worker_count(repo, health_status) do
    {:ok, count} = Workers.count(repo, health_status: health_status)
    count
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:workers} base_path={@base_path}>
      <Layouts.page_header title="Workers" subtitle="Active worker processes" />

      <!-- Summary Filter Cards -->
      <div class="grid grid-cols-3 gap-4 mb-6">
        <.link
          patch={health_filter_url(@base_path, @health_filter, "healthy")}
          class={[
            "block bg-white dark:bg-slate-800 rounded-lg border p-4 transition-colors cursor-pointer",
            health_card_border_class(@health_filter, "healthy")
          ]}
        >
          <div class="flex items-center gap-2">
            <span class="w-3 h-3 rounded-full bg-emerald-500"></span>
            <span class="text-sm text-slate-500 dark:text-slate-400">Healthy</span>
          </div>
          <p class="text-2xl font-semibold text-slate-900 dark:text-white mt-1">{@summary.healthy}</p>
        </.link>
        <.link
          patch={health_filter_url(@base_path, @health_filter, "stale")}
          class={[
            "block bg-white dark:bg-slate-800 rounded-lg border p-4 transition-colors cursor-pointer",
            health_card_border_class(@health_filter, "stale")
          ]}
        >
          <div class="flex items-center gap-2">
            <span class="w-3 h-3 rounded-full bg-amber-500"></span>
            <span class="text-sm text-slate-500 dark:text-slate-400">Stale</span>
          </div>
          <p class="text-2xl font-semibold text-slate-900 dark:text-white mt-1">{@summary.stale}</p>
        </.link>
        <.link
          patch={health_filter_url(@base_path, @health_filter, "dead")}
          class={[
            "block bg-white dark:bg-slate-800 rounded-lg border p-4 transition-colors cursor-pointer",
            health_card_border_class(@health_filter, "dead")
          ]}
        >
          <div class="flex items-center gap-2">
            <span class="w-3 h-3 rounded-full bg-slate-400"></span>
            <span class="text-sm text-slate-500 dark:text-slate-400">Dead</span>
          </div>
          <p class="text-2xl font-semibold text-slate-900 dark:text-white mt-1">{@summary.dead}</p>
        </.link>
      </div>

      <!-- Workers List -->
      <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
        <div class="px-4 py-2 border-b border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 flex items-center justify-between">
          <span class="text-sm text-slate-500 dark:text-slate-400">
            Showing {@workers_count} of {@total_count} workers
          </span>
        </div>
        <table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
          <thead class="bg-slate-50 dark:bg-slate-800/50">
            <tr>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Worker ID</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Flow / Job</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Status</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Active Tasks</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Completed (24h)</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Last Heartbeat</th>
            </tr>
          </thead>
          <tbody id="workers-list" phx-update="stream" class="divide-y divide-slate-200 dark:divide-slate-700">
            <tr :if={@workers_count == 0} id="workers-empty">
              <td colspan="6" class="px-4 py-8 text-center text-slate-500 dark:text-slate-400">
                No workers registered
              </td>
            </tr>
            <tr :for={{dom_id, worker} <- @streams.workers} id={dom_id} class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
              <td class="px-4 py-3">
                <.link
                  navigate={"#{@base_path}/workers/#{worker.worker_id}"}
                  class="text-sm font-mono text-purple-600 hover:text-purple-700 dark:text-purple-400"
                >
                  {LiveHelpers.short_id(worker.worker_id)}
                </.link>
              </td>
              <td class="px-4 py-3">
                <div class="flex items-center gap-2">
                  <span class="text-sm text-slate-700 dark:text-slate-300">{worker.flow_slug}</span>
                  <TypeBadge.type_badge type={Map.get(worker, :flow_type, "flow")} />
                </div>
              </td>
              <td class="px-4 py-3">
                <HealthBadge.health_badge status={worker.health_status} size={:sm} />
              </td>
              <td class="px-4 py-3 text-sm text-slate-700 dark:text-slate-300">{worker.active_tasks}</td>
              <td class="px-4 py-3 text-sm text-slate-700 dark:text-slate-300">{worker.completed_tasks_24h}</td>
              <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
                {LiveHelpers.format_timestamp(worker.last_heartbeat_at, @time_zone)}
              </td>
            </tr>
          </tbody>
        </table>

        <!-- Load More Button -->
        <div :if={@has_more} class="px-4 py-3 border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50">
          <button
            phx-click="load_more"
            class="w-full py-2 px-4 text-sm font-medium text-purple-600 hover:text-purple-700 dark:text-purple-400 dark:hover:text-purple-300 hover:bg-purple-50 dark:hover:bg-purple-900/20 rounded-md transition-colors"
          >
            Load more workers
          </button>
        </div>
      </div>
    </Layouts.dashboard_layout>
    """
  end

  defp health_filter_url(base_path, current_filter, status) do
    if current_filter == status do
      "#{base_path}/workers"
    else
      "#{base_path}/workers?health=#{status}"
    end
  end

  defp health_card_border_class(current_filter, status) do
    if current_filter == status do
      "border-purple-500 dark:border-purple-400"
    else
      "border-slate-200 dark:border-slate-700 hover:border-purple-300 dark:hover:border-purple-600"
    end
  end
end
