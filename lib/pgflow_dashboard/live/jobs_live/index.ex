defmodule PgFlowDashboard.Live.JobsLive.Index do
  @moduledoc """
  Jobs list page with statistics.

  Shows card view for ≤12 jobs, switches to paginated list view for more.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{Layouts, TypeBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries.Jobs

  @card_threshold 12
  @page_size 50

  @impl true
  def mount(_params, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Jobs")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:cursor, nil)
      |> assign(:has_more, false)
      |> assign(:total_count, 0)
      |> assign(:jobs_count, 0)
      |> stream_configure(:jobs, dom_id: &"job-#{&1.flow_slug}")
      |> stream(:jobs, [])
      |> load_jobs()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_event("load_more", _, socket) do
    jobs =
      Jobs.list_jobs(socket.assigns.repo,
        cursor: socket.assigns.cursor,
        limit: @page_size + 1
      )

    {jobs, has_more} = LiveHelpers.paginate_results(jobs, @page_size)
    new_cursor = if jobs != [], do: List.last(jobs).flow_slug, else: nil
    new_count = socket.assigns.jobs_count + length(jobs)

    socket =
      socket
      |> stream(:jobs, jobs)
      |> assign(:cursor, new_cursor)
      |> assign(:has_more, has_more)
      |> assign(:jobs_count, new_count)

    {:noreply, socket}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> refresh_jobs()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  def handle_info(_, socket), do: {:noreply, socket}

  defp load_jobs(socket) do
    total_count = Jobs.count_jobs(socket.assigns.repo)
    view_mode = LiveHelpers.determine_view_mode(total_count, @card_threshold)

    case view_mode do
      :card ->
        jobs = Jobs.list_jobs(socket.assigns.repo)

        socket
        |> assign(:view_mode, :card)
        |> assign(:total_count, total_count)
        |> assign(:jobs, jobs)

      :list ->
        jobs = Jobs.list_jobs(socket.assigns.repo, limit: @page_size + 1)
        {jobs, has_more} = LiveHelpers.paginate_results(jobs, @page_size)
        cursor = if jobs != [], do: List.last(jobs).flow_slug, else: nil

        socket
        |> assign(:view_mode, :list)
        |> assign(:total_count, total_count)
        |> assign(:jobs_count, length(jobs))
        |> assign(:cursor, cursor)
        |> assign(:has_more, has_more)
        |> stream(:jobs, jobs, reset: true)
    end
  end

  defp refresh_jobs(socket) do
    total_count = Jobs.count_jobs(socket.assigns.repo)
    new_view_mode = LiveHelpers.determine_view_mode(total_count, @card_threshold)

    if new_view_mode != socket.assigns.view_mode do
      load_jobs(socket)
    else
      case socket.assigns.view_mode do
        :card ->
          jobs = Jobs.list_jobs(socket.assigns.repo)

          socket
          |> assign(:total_count, total_count)
          |> assign(:jobs, jobs)

        :list ->
          current_count = max(socket.assigns.jobs_count, @page_size)
          jobs = Jobs.list_jobs(socket.assigns.repo, limit: current_count + 1)
          {jobs, has_more} = LiveHelpers.paginate_results(jobs, current_count)
          cursor = if jobs != [], do: List.last(jobs).flow_slug, else: nil

          socket
          |> assign(:total_count, total_count)
          |> assign(:jobs_count, length(jobs))
          |> assign(:cursor, cursor)
          |> assign(:has_more, has_more)
          |> stream(:jobs, jobs, reset: true)
      end
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:jobs} base_path={@base_path}>
      <Layouts.page_header title="Jobs" subtitle="Background job definitions" />

      <%= if @view_mode == :card do %>
        <.card_view jobs={@jobs} base_path={@base_path} />
      <% else %>
        <.list_view
          streams={@streams}
          base_path={@base_path}
          jobs_count={@jobs_count}
          total_count={@total_count}
          has_more={@has_more}
        />
      <% end %>
    </Layouts.dashboard_layout>
    """
  end

  defp card_view(assigns) do
    ~H"""
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
              <TypeBadge.type_badge type="job" />
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

            <div class="flex items-center justify-end text-xs text-slate-500 dark:text-slate-400">
              <span>{job.opt_max_attempts} attempts · {job.opt_timeout}s timeout</span>
            </div>
          </.link>
        <% end %>
      <% end %>
    </div>
    """
  end

  defp list_view(assigns) do
    ~H"""
    <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
      <div class="px-4 py-2 border-b border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 flex items-center justify-between">
        <span class="text-sm text-slate-500 dark:text-slate-400">
          Showing {@jobs_count} of {@total_count} jobs
        </span>
      </div>
      <table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
        <thead class="bg-slate-50 dark:bg-slate-800/50">
          <tr>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Name
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Runs (24h)
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Success
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Avg Duration
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Config
            </th>
          </tr>
        </thead>
        <tbody id="jobs-list" phx-update="stream" class="divide-y divide-slate-200 dark:divide-slate-700">
          <tr :if={@jobs_count == 0} id="jobs-empty">
            <td colspan="5" class="px-4 py-8 text-center text-slate-500 dark:text-slate-400">
              No jobs registered
            </td>
          </tr>
          <tr
            :for={{dom_id, job} <- @streams.jobs}
            id={dom_id}
            class="hover:bg-slate-50 dark:hover:bg-slate-700/50"
          >
            <td class="px-4 py-3">
              <.link
                navigate={"#{@base_path}/jobs/#{job.flow_slug}"}
                class="flex items-center gap-2"
              >
                <span class="text-sm font-medium text-blue-600 hover:text-blue-700 dark:text-blue-400">
                  {job.flow_slug}
                </span>
                <TypeBadge.type_badge type="job" />
              </.link>
            </td>
            <td class="px-4 py-3 text-sm text-slate-700 dark:text-slate-300">
              {job.total_runs_24h}
            </td>
            <td class="px-4 py-3 text-sm text-emerald-600 dark:text-emerald-400">
              {job.success_rate_24h}%
            </td>
            <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
              {LiveHelpers.format_duration(job.avg_duration_ms)}
            </td>
            <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
              {job.opt_max_attempts} attempts · {job.opt_timeout}s
            </td>
          </tr>
        </tbody>
      </table>

      <div
        :if={@has_more}
        class="px-4 py-3 border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50"
      >
        <button
          phx-click="load_more"
          class="w-full py-2 px-4 text-sm font-medium text-blue-600 hover:text-blue-700 dark:text-blue-400 dark:hover:text-blue-300 hover:bg-blue-50 dark:hover:bg-blue-900/20 rounded-md transition-colors"
        >
          Load more jobs
        </button>
      </div>
    </div>
    """
  end
end
