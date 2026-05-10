defmodule PgFlowDashboard.Live.JobsLive.Show do
  @moduledoc """
  Job detail page with run history grid and recent runs table.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{Layouts, RunHistoryHelpers, StatusBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries.{Jobs, Runs}

  @page_size 20

  @impl true
  def mount(%{"id" => job_slug}, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, job_slug)
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:job_slug, job_slug)
      |> load_job()

    if socket.assigns.job do
      socket =
        socket
        |> load_run_history()
        |> load_recent_runs()
        |> LiveHelpers.subscribe_to_updates()
        |> LiveHelpers.schedule_refresh()

      {:ok, socket}
    else
      {:ok, push_navigate(socket, to: "#{socket.assigns.base_path}/jobs")}
    end
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_job()
      |> load_run_history()
      |> load_recent_runs()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  def handle_info({:pgflow, _run_id, {:run_completed, _}}, socket) do
    {:noreply, socket |> load_run_history() |> load_recent_runs()}
  end

  def handle_info({:pgflow, _run_id, {:run_failed, _}}, socket) do
    {:noreply, socket |> load_run_history() |> load_recent_runs()}
  end

  def handle_info(_, socket), do: {:noreply, socket}

  defp load_job(socket) do
    case Jobs.get_job(socket.assigns.repo, socket.assigns.job_slug) do
      {:ok, job} -> assign(socket, :job, job)
      {:error, _} -> assign(socket, :job, nil)
    end
  end

  defp load_run_history(socket) do
    grid_cells =
      Jobs.get_run_history_grid(
        socket.assigns.repo,
        socket.assigns.job_slug,
        limit: socket.assigns.config[:max_grid_runs] || 50
      )

    assign(socket, :grid_cells, grid_cells)
  end

  defp load_recent_runs(socket) do
    runs =
      Runs.list_runs(socket.assigns.repo,
        flow_slug: socket.assigns.job_slug,
        limit: @page_size + 1
      )

    {runs, has_more} =
      if length(runs) > @page_size do
        {Enum.take(runs, @page_size), true}
      else
        {runs, false}
      end

    socket
    |> assign(:recent_runs, runs)
    |> assign(:has_more_runs, has_more)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:jobs} base_path={@base_path}>
      <div :if={@job}>
        <!-- Header -->
        <div class="mb-6">
          <.link
            navigate={"#{@base_path}/jobs"}
            class="text-sm text-slate-500 hover:text-slate-700 dark:text-slate-400 mb-2 inline-block"
          >
            ← Back to jobs
          </.link>
          <div class="flex items-center gap-3">
            <h1 class="text-2xl font-bold text-slate-900 dark:text-white">{@job.flow_slug}</h1>
            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400">
              job
            </span>
          </div>
          <p class="mt-1 text-sm text-slate-500 dark:text-slate-400">
            Max {@job.opt_max_attempts} attempts · {@job.opt_timeout}s timeout
          </p>
        </div>

        <!-- Stats -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Total Runs (24h)</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">{@job.total_runs_24h}</p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Success Rate</p>
            <p class="text-2xl font-semibold text-emerald-600 dark:text-emerald-400">
              {@job.success_rate_24h}%
            </p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Avg Duration</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">
              {LiveHelpers.format_duration(@job.avg_duration_ms)}
            </p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">P95 Duration</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">
              {LiveHelpers.format_duration(@job.p95_duration_ms)}
            </p>
          </div>
        </div>

        <!-- Run History Grid -->
        <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 mb-6">
          <h2 class="text-sm font-semibold text-slate-900 dark:text-white mb-4">Run History</h2>
          <p class="text-xs text-slate-500 dark:text-slate-400 mb-3">
            Recent job executions (oldest → newest)
          </p>
          <RunHistoryHelpers.run_history_grid cells={@grid_cells} base_path={@base_path} />
        </div>

        <!-- Recent Runs Table -->
        <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
          <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700">
            <h2 class="text-sm font-semibold text-slate-900 dark:text-white">Recent Runs</h2>
          </div>
          <table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
            <thead class="bg-slate-50 dark:bg-slate-800/50">
              <tr>
                <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
                  Run ID
                </th>
                <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
                  Status
                </th>
                <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
                  Duration
                </th>
                <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
                  Started
                </th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-200 dark:divide-slate-700">
              <%= if @recent_runs == [] do %>
                <tr>
                  <td colspan="4" class="px-4 py-8 text-center text-slate-500 dark:text-slate-400">
                    No runs yet
                  </td>
                </tr>
              <% else %>
                <%= for run <- @recent_runs do %>
                  <tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
                    <td class="px-4 py-3">
                      <.link
                        navigate={"#{@base_path}/runs/#{run.run_id}"}
                        class="text-sm font-mono text-purple-600 hover:text-purple-700 dark:text-purple-400"
                      >
                        {LiveHelpers.short_id(run.run_id)}
                      </.link>
                    </td>
                    <td class="px-4 py-3">
                      <StatusBadge.status_badge
                        status={run.status}
                        size={:sm}
                        pulse={run.status == "started"}
                      />
                    </td>
                    <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
                      {LiveHelpers.format_duration(run.duration_ms)}
                    </td>
                    <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
                      {LiveHelpers.format_timestamp(run.started_at, @time_zone)}
                    </td>
                  </tr>
                <% end %>
              <% end %>
            </tbody>
          </table>

          <div
            :if={@has_more_runs}
            class="px-4 py-3 border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50"
          >
            <.link
              navigate={"#{@base_path}/runs?flow=#{@job_slug}"}
              class="text-sm text-purple-600 hover:text-purple-700 dark:text-purple-400"
            >
              View all runs →
            </.link>
          </div>
        </div>
      </div>
    </Layouts.dashboard_layout>
    """
  end
end
