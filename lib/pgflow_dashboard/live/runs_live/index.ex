defmodule PgFlowDashboard.Live.RunsLive.Index do
  @moduledoc """
  Runs list page with filtering.

  Uses LiveView streams for efficient rendering of large lists.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{Layouts, ProgressBar, StatusBadge, TypeBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries

  @page_size 50

  @impl true
  def mount(_params, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Runs")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:flow_filter, nil)
      |> assign(:status_filter, nil)
      |> assign(:type_filter, nil)
      |> assign(:time_range, :last_24h)
      |> assign(:cursor, nil)
      |> assign(:has_more, false)
      |> assign(:total_count, 0)
      |> assign(:runs_count, 0)
      |> stream_configure(:runs, dom_id: &"run-#{&1.run_id}")
      |> stream(:runs, [])
      |> load_flows_and_jobs()
      |> load_runs()
      |> LiveHelpers.subscribe_to_updates()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    socket =
      socket
      |> assign(:flow_filter, params["flow"])
      |> assign(:status_filter, params["status"])
      |> assign(:type_filter, params["type"])
      |> assign(:time_range, parse_time_range(params["time_range"]))
      |> assign(:cursor, nil)
      |> load_runs(reset: true)

    {:noreply, socket}
  end

  @impl true
  def handle_event(
        "filter",
        %{"flow" => flow, "status" => status, "type" => type, "time_range" => time_range},
        socket
      ) do
    params = %{}
    params = if flow != "", do: Map.put(params, "flow", flow), else: params
    params = if status != "", do: Map.put(params, "status", status), else: params
    params = if type != "", do: Map.put(params, "type", type), else: params

    params =
      if time_range != "last_24h", do: Map.put(params, "time_range", time_range), else: params

    {:noreply,
     push_patch(socket, to: "#{socket.assigns.base_path}/runs?#{URI.encode_query(params)}")}
  end

  def handle_event("load_more", _, socket) do
    runs =
      Queries.list_runs(socket.assigns.repo,
        flow_slug: socket.assigns.flow_filter,
        status: socket.assigns.status_filter,
        flow_type: socket.assigns.type_filter,
        time_range: socket.assigns.time_range,
        cursor: socket.assigns.cursor,
        limit: @page_size + 1
      )

    {runs, has_more} =
      if length(runs) > @page_size do
        {Enum.take(runs, @page_size), true}
      else
        {runs, false}
      end

    new_cursor = if runs != [], do: List.last(runs).run_id, else: nil
    new_count = socket.assigns.runs_count + length(runs)

    socket =
      socket
      |> stream(:runs, runs)
      |> assign(:cursor, new_cursor)
      |> assign(:has_more, has_more)
      |> assign(:runs_count, new_count)

    {:noreply, socket}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> refresh_runs()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  def handle_info({:run_started, _}, socket), do: {:noreply, load_runs(socket, reset: true)}
  def handle_info({:run_completed, _}, socket), do: {:noreply, refresh_runs(socket)}
  def handle_info({:run_failed, _}, socket), do: {:noreply, refresh_runs(socket)}
  def handle_info(_, socket), do: {:noreply, socket}

  defp load_flows_and_jobs(socket) do
    flows = Queries.list_flows(socket.assigns.repo)
    jobs = Queries.list_jobs(socket.assigns.repo)
    crons = Queries.list_crons(socket.assigns.repo)

    socket
    |> assign(:flows, flows)
    |> assign(:jobs, jobs)
    |> assign(:crons, crons)
  end

  defp load_runs(socket, opts \\ []) do
    reset = Keyword.get(opts, :reset, false)

    runs =
      Queries.list_runs(socket.assigns.repo,
        flow_slug: socket.assigns.flow_filter,
        status: socket.assigns.status_filter,
        flow_type: socket.assigns.type_filter,
        time_range: socket.assigns.time_range,
        limit: @page_size + 1
      )

    {runs, has_more} =
      if length(runs) > @page_size do
        {Enum.take(runs, @page_size), true}
      else
        {runs, false}
      end

    cursor = if runs != [], do: List.last(runs).run_id, else: nil

    total_count =
      Queries.count_runs(socket.assigns.repo,
        flow_slug: socket.assigns.flow_filter,
        status: socket.assigns.status_filter,
        flow_type: socket.assigns.type_filter,
        time_range: socket.assigns.time_range
      )

    socket
    |> stream(:runs, runs, reset: reset)
    |> assign(:cursor, cursor)
    |> assign(:has_more, has_more)
    |> assign(:total_count, total_count)
    |> assign(:runs_count, length(runs))
  end

  defp refresh_runs(socket) do
    current_count = max(socket.assigns.runs_count, @page_size)

    runs =
      Queries.list_runs(socket.assigns.repo,
        flow_slug: socket.assigns.flow_filter,
        status: socket.assigns.status_filter,
        flow_type: socket.assigns.type_filter,
        time_range: socket.assigns.time_range,
        limit: current_count + 1
      )

    {runs, has_more} =
      if length(runs) > current_count do
        {Enum.take(runs, current_count), true}
      else
        {runs, false}
      end

    cursor = if runs != [], do: List.last(runs).run_id, else: nil

    socket
    |> stream(:runs, runs, reset: true)
    |> assign(:cursor, cursor)
    |> assign(:has_more, has_more)
    |> assign(:runs_count, length(runs))
  end

  defp parse_time_range("last_hour"), do: :last_hour
  defp parse_time_range("last_7d"), do: :last_7d
  defp parse_time_range("last_30d"), do: :last_30d
  defp parse_time_range(_), do: :last_24h

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:runs} base_path={@base_path}>
      <Layouts.page_header title="Runs" subtitle="All workflow executions" />

      <!-- Filters -->
      <div class="mb-6 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
        <form phx-change="filter" class="flex flex-wrap gap-4">
          <div>
            <label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">
              Type
            </label>
            <select
              name="type"
              class="block w-28 rounded-md border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-700 text-slate-900 dark:text-slate-100 text-sm"
            >
              <option value="">All</option>
              <option value="flow" selected={@type_filter == "flow"}>Flows</option>
              <option value="job" selected={@type_filter == "job"}>Jobs</option>
              <option value="cron" selected={@type_filter == "cron"}>Crons</option>
            </select>
          </div>

          <div>
            <label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">
              Queue
            </label>
            <select
              name="flow"
              class="block w-40 rounded-md border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-700 text-slate-900 dark:text-slate-100 text-sm"
            >
              <option value="">All</option>
              <%= if @flows != [] do %>
                <optgroup label="Flows">
                  <%= for flow <- @flows do %>
                    <option value={flow.flow_slug} selected={@flow_filter == flow.flow_slug}>
                      {flow.flow_slug}
                    </option>
                  <% end %>
                </optgroup>
              <% end %>
              <%= if @jobs != [] do %>
                <optgroup label="Jobs">
                  <%= for job <- @jobs do %>
                    <option value={job.flow_slug} selected={@flow_filter == job.flow_slug}>
                      {job.flow_slug}
                    </option>
                  <% end %>
                </optgroup>
              <% end %>
              <%= if @crons != [] do %>
                <optgroup label="Crons">
                  <%= for cron <- @crons do %>
                    <option value={cron.flow_slug} selected={@flow_filter == cron.flow_slug}>
                      {cron.flow_slug}
                    </option>
                  <% end %>
                </optgroup>
              <% end %>
            </select>
          </div>

          <div>
            <label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">
              Status
            </label>
            <select
              name="status"
              class="block w-32 rounded-md border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-700 text-slate-900 dark:text-slate-100 text-sm"
            >
              <option value="">All statuses</option>
              <option value="started" selected={@status_filter == "started"}>Running</option>
              <option value="completed" selected={@status_filter == "completed"}>Completed</option>
              <option value="failed" selected={@status_filter == "failed"}>Failed</option>
            </select>
          </div>

          <div>
            <label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">
              Time Range
            </label>
            <select
              name="time_range"
              class="block w-32 rounded-md border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-700 text-slate-900 dark:text-slate-100 text-sm"
            >
              <option value="last_hour" selected={@time_range == :last_hour}>Last hour</option>
              <option value="last_24h" selected={@time_range == :last_24h}>Last 24h</option>
              <option value="last_7d" selected={@time_range == :last_7d}>Last 7 days</option>
              <option value="last_30d" selected={@time_range == :last_30d}>Last 30 days</option>
            </select>
          </div>
        </form>
      </div>

      <!-- Runs Table -->
      <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
        <div class="px-4 py-2 border-b border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 flex items-center justify-between">
          <span class="text-sm text-slate-500 dark:text-slate-400">
            Showing {@runs_count} of {@total_count} runs
          </span>
        </div>
        <table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
          <thead class="bg-slate-50 dark:bg-slate-800/50">
            <tr>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Run ID</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Queue</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Status</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Progress</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Duration</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Started</th>
            </tr>
          </thead>
          <tbody id="runs-list" phx-update="stream" class="divide-y divide-slate-200 dark:divide-slate-700">
            <tr :if={@runs_count == 0} id="runs-empty">
              <td colspan="6" class="px-4 py-8 text-center text-slate-500 dark:text-slate-400">
                No runs found
              </td>
            </tr>
            <tr
              :for={{dom_id, run} <- @streams.runs}
              id={dom_id}
              class="hover:bg-slate-50 dark:hover:bg-slate-700/50"
            >
              <td class="px-4 py-3">
                <.link
                  navigate={"#{@base_path}/runs/#{run.run_id}"}
                  class="text-sm font-mono text-purple-600 hover:text-purple-700 dark:text-purple-400"
                >
                  {LiveHelpers.short_id(run.run_id)}
                </.link>
              </td>
              <td class="px-4 py-3">
                <div class="flex items-center gap-2">
                  <span class="text-sm text-slate-700 dark:text-slate-300">{run.flow_slug}</span>
                  <TypeBadge.type_badge type={Map.get(run, :flow_type, "flow")} />
                </div>
              </td>
              <td class="px-4 py-3">
                <StatusBadge.status_badge
                  status={run.status}
                  size={:sm}
                  pulse={run.status == "started"}
                />
              </td>
              <td class="px-4 py-3 w-32">
                <%= if Map.get(run, :flow_type) in ["job", "cron"] do %>
                  <span class="text-sm text-slate-400 dark:text-slate-500">—</span>
                <% else %>
                  <ProgressBar.progress_bar
                    progress={run.progress_percent}
                    completed={run.completed_steps}
                    total={run.total_steps}
                    failed={run.failed_steps}
                    size={:sm}
                  />
                <% end %>
              </td>
              <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
                {LiveHelpers.format_duration(run.duration_ms)}
              </td>
              <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
                {LiveHelpers.format_timestamp(run.started_at, @time_zone)}
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
            Load more runs
          </button>
        </div>
      </div>
    </Layouts.dashboard_layout>
    """
  end
end
