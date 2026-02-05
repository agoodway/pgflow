defmodule PgFlowDashboard.Live.OverviewLive do
  @moduledoc """
  Overview dashboard page showing key metrics and recent activity.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{Layouts, MetricCard, StatusBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries

  @impl true
  def mount(_params, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Overview")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> load_metrics()
      |> load_recent_runs()
      |> load_workers()
      |> LiveHelpers.subscribe_to_updates()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_metrics()
      |> load_recent_runs()
      |> load_workers()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  def handle_info({:run_started, _}, socket) do
    {:noreply, load_recent_runs(socket)}
  end

  def handle_info({:run_completed, _}, socket) do
    socket =
      socket
      |> load_metrics()
      |> load_recent_runs()

    {:noreply, socket}
  end

  def handle_info({:run_failed, _}, socket) do
    socket =
      socket
      |> load_metrics()
      |> load_recent_runs()

    {:noreply, socket}
  end

  def handle_info({:worker_heartbeat, _}, socket) do
    {:noreply, load_workers(socket)}
  end

  def handle_info(_, socket), do: {:noreply, socket}

  defp load_metrics(socket) do
    metrics = Queries.get_overview_metrics(socket.assigns.repo)
    assign(socket, :metrics, metrics)
  end

  defp load_recent_runs(socket) do
    runs = Queries.list_runs(socket.assigns.repo, limit: 15)
    assign(socket, :recent_runs, runs)
  end

  defp load_workers(socket) do
    workers = Queries.list_workers(socket.assigns.repo, limit: 15)
    assign(socket, :workers, workers)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:overview} base_path={@base_path}>
      <Layouts.page_header title="Overview" subtitle="Real-time workflow monitoring" />

      <!-- Metrics Grid -->
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <MetricCard.metric_card
          title="Active Workers"
          value={@metrics.healthy_workers}
          subtitle={"#{@metrics.stale_workers} stale"}
          href={"#{@base_path}/workers?health=healthy"}
        />
        <MetricCard.metric_card
          title="Running Flows"
          value={@metrics.running_runs}
          subtitle="currently executing"
          href={"#{@base_path}/runs?status=started"}
        />
        <MetricCard.metric_card
          title="Completed (24h)"
          value={@metrics.completed_runs_24h}
          subtitle={"#{@metrics.failed_runs_24h} failed"}
        />
        <MetricCard.metric_card
          title="Avg Duration"
          value={LiveHelpers.format_duration(@metrics.avg_duration_ms)}
          subtitle="last 24 hours"
        />
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Workers Status -->
        <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
          <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between">
            <h2 class="text-sm font-semibold text-slate-900 dark:text-white">Workers</h2>
            <.link navigate={"#{@base_path}/workers"} class="text-xs text-purple-600 hover:text-purple-700 dark:text-purple-400">
              View all →
            </.link>
          </div>
          <div class="divide-y divide-slate-200 dark:divide-slate-700">
            <%= if @workers == [] do %>
              <div class="px-4 py-8 text-center text-slate-500 dark:text-slate-400 text-sm">
                No workers registered
              </div>
            <% else %>
              <%= for worker <- @workers do %>
                <div class="px-4 py-3">
                  <div class="flex items-center justify-between">
                    <div class="flex items-center gap-3">
                      <.health_indicator status={worker.health_status} />
                      <div>
                        <p class="text-sm font-medium text-slate-900 dark:text-white">{worker.flow_slug}</p>
                        <p class="text-xs text-slate-500 dark:text-slate-400">{LiveHelpers.short_id(worker.worker_id)}</p>
                      </div>
                    </div>
                    <div class="text-right">
                      <p class="text-xs text-slate-500 dark:text-slate-400">{worker.active_tasks} active</p>
                    </div>
                  </div>
                </div>
              <% end %>
            <% end %>
          </div>
        </div>

        <!-- Recent Runs -->
        <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
          <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between">
            <h2 class="text-sm font-semibold text-slate-900 dark:text-white">Recent Runs</h2>
            <.link navigate={"#{@base_path}/runs"} class="text-xs text-purple-600 hover:text-purple-700 dark:text-purple-400">
              View all →
            </.link>
          </div>
          <div class="divide-y divide-slate-200 dark:divide-slate-700">
            <%= if @recent_runs == [] do %>
              <div class="px-4 py-8 text-center text-slate-500 dark:text-slate-400 text-sm">
                No runs yet
              </div>
            <% else %>
              <%= for run <- @recent_runs do %>
                <.link
                  navigate={"#{@base_path}/runs/#{run.run_id}"}
                  class="block px-4 py-3 hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors"
                >
                  <div class="flex items-center justify-between">
                    <div class="flex items-center gap-3">
                      <StatusBadge.status_badge status={run.status} size={:sm} pulse={run.status == "started"} />
                      <div>
                        <p class="text-sm font-medium text-slate-900 dark:text-white">{run.flow_slug}</p>
                        <p class="text-xs text-slate-500 dark:text-slate-400">{LiveHelpers.short_id(run.run_id)}</p>
                      </div>
                    </div>
                    <div class="text-right">
                      <p class="text-xs text-slate-500 dark:text-slate-400">{LiveHelpers.format_duration(run.duration_ms)}</p>
                      <p class="text-xs text-slate-400">{run.progress_percent}%</p>
                    </div>
                  </div>
                </.link>
              <% end %>
            <% end %>
          </div>
        </div>
      </div>
    </Layouts.dashboard_layout>
    """
  end

  defp health_indicator(assigns) do
    color =
      case assigns.status do
        "healthy" -> "bg-emerald-500"
        "stale" -> "bg-amber-500"
        _ -> "bg-slate-400"
      end

    assigns = assign(assigns, :color, color)

    ~H"""
    <span class={["w-2 h-2 rounded-full", @color]} />
    """
  end
end
