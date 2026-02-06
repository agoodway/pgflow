defmodule PgFlowDashboard.Live.JobsLive.Index do
  @moduledoc """
  Jobs list page with statistics.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.Layouts
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries

  @impl true
  def mount(_params, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Jobs")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> load_jobs()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_jobs()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  def handle_info(_, socket), do: {:noreply, socket}

  defp load_jobs(socket) do
    jobs = Queries.list_jobs(socket.assigns.repo)
    assign(socket, :jobs, jobs)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:jobs} base_path={@base_path}>
      <Layouts.page_header title="Jobs" subtitle="Background job definitions" />

      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        <%= if @jobs == [] do %>
          <div class="col-span-full bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
            <p class="text-slate-500 dark:text-slate-400">No jobs registered</p>
          </div>
        <% else %>
          <%= for job <- @jobs do %>
            <.link
              navigate={"#{@base_path}/jobs/#{job.flow_slug}"}
              class="block bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 hover:border-blue-300 dark:hover:border-blue-600 transition-colors"
            >
              <div class="flex items-start justify-between mb-3">
                <h3 class="text-lg font-semibold text-slate-900 dark:text-white">{job.flow_slug}</h3>
                <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400">
                  job
                </span>
              </div>

              <div class="grid grid-cols-3 gap-2 text-center mb-3">
                <div class="bg-slate-50 dark:bg-slate-900 rounded p-2">
                  <p class="text-lg font-semibold text-slate-900 dark:text-white">{job.total_runs_24h}</p>
                  <p class="text-xs text-slate-500 dark:text-slate-400">runs</p>
                </div>
                <div class="bg-emerald-50 dark:bg-emerald-900/20 rounded p-2">
                  <p class="text-lg font-semibold text-emerald-600 dark:text-emerald-400">{job.success_rate_24h}%</p>
                  <p class="text-xs text-slate-500 dark:text-slate-400">success</p>
                </div>
                <div class="bg-slate-50 dark:bg-slate-900 rounded p-2">
                  <p class="text-lg font-semibold text-slate-900 dark:text-white">{LiveHelpers.format_duration(job.avg_duration_ms)}</p>
                  <p class="text-xs text-slate-500 dark:text-slate-400">avg</p>
                </div>
              </div>

              <div class="flex items-center justify-between text-xs text-slate-500 dark:text-slate-400">
                <span>Max attempts: {job.opt_max_attempts}</span>
                <span>Timeout: {job.opt_timeout}s</span>
              </div>
            </.link>
          <% end %>
        <% end %>
      </div>
    </Layouts.dashboard_layout>
    """
  end
end
