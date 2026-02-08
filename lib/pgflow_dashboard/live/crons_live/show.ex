defmodule PgFlowDashboard.Live.CronsLive.Show do
  @moduledoc """
  Cron detail page with schedule info, run history grid, and recent runs table.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{Layouts, StatusBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries.{Crons, Runs}

  @page_size 20

  @impl true
  def mount(%{"id" => cron_slug}, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, cron_slug)
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:cron_slug, cron_slug)
      |> load_cron()

    if socket.assigns.cron do
      socket =
        socket
        |> load_run_history()
        |> load_recent_runs()
        |> LiveHelpers.subscribe_to_updates()
        |> LiveHelpers.schedule_refresh()

      {:ok, socket}
    else
      {:ok, push_navigate(socket, to: "#{socket.assigns.base_path}/crons")}
    end
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_cron()
      |> load_run_history()
      |> load_recent_runs()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_completed, _}}, socket) do
    {:noreply, socket |> load_run_history() |> load_recent_runs()}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_failed, _}}, socket) do
    {:noreply, socket |> load_run_history() |> load_recent_runs()}
  end

  @impl true
  def handle_info(_, socket), do: {:noreply, socket}

  defp load_cron(socket) do
    case Crons.get_cron(socket.assigns.repo, socket.assigns.cron_slug) do
      {:ok, cron} -> assign(socket, :cron, cron)
      {:error, _} -> assign(socket, :cron, nil)
    end
  end

  defp load_run_history(socket) do
    grid_cells =
      Crons.get_run_history_grid(
        socket.assigns.repo,
        socket.assigns.cron_slug,
        limit: socket.assigns.config[:max_grid_runs] || 50
      )

    assign(socket, :grid_cells, grid_cells)
  end

  defp load_recent_runs(socket) do
    runs =
      Runs.list_runs(socket.assigns.repo,
        flow_slug: socket.assigns.cron_slug,
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
    <Layouts.dashboard_layout current_page={:crons} base_path={@base_path}>
      <div :if={@cron}>
        <!-- Header -->
        <div class="mb-6">
          <.link
            navigate={"#{@base_path}/crons"}
            class="text-sm text-slate-500 hover:text-slate-700 dark:text-slate-400 mb-2 inline-block"
          >
            ← Back to crons
          </.link>
          <div class="flex items-center gap-3">
            <h1 class="text-2xl font-bold text-slate-900 dark:text-white">{@cron.flow_slug}</h1>
            <span class={[
              "inline-flex items-center px-2 py-0.5 rounded text-xs font-medium",
              @cron.flow_type == "job" && "bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-400",
              @cron.flow_type == "flow" && "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400"
            ]}>
              {@cron.flow_type}
            </span>
            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-400">
              cron
            </span>
            <span
              :if={@cron.is_active}
              class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-400"
            >
              Active
            </span>
            <span
              :if={!@cron.is_active}
              class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-400"
            >
              Inactive
            </span>
          </div>
          <div class="mt-2">
            <p class="text-sm text-slate-700 dark:text-slate-300">
              {@cron.human_schedule || "Custom schedule"}
              <span class="text-slate-400 dark:text-slate-500 font-mono text-xs ml-2">
                ({@cron.cron_expression || "—"})
              </span>
            </p>
          </div>
          <p class="mt-1 text-sm text-slate-500 dark:text-slate-400">
            Max {@cron.opt_max_attempts} attempts · {@cron.opt_timeout}s timeout
          </p>
        </div>

        <!-- Stats -->
        <div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Next Run</p>
            <p class="text-lg font-semibold text-amber-600 dark:text-amber-400">
              {LiveHelpers.format_relative_time(@cron.next_run_at)}
            </p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Runs (24h)</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">{@cron.total_runs_24h}</p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Success Rate</p>
            <p class="text-2xl font-semibold text-emerald-600 dark:text-emerald-400">
              {@cron.success_rate_24h}%
            </p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Avg Duration</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">
              {LiveHelpers.format_duration(@cron.avg_duration_ms)}
            </p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">P95 Duration</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">
              {LiveHelpers.format_duration(@cron.p95_duration_ms)}
            </p>
          </div>
        </div>

        <!-- Run History Grid -->
        <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 mb-6">
          <h2 class="text-sm font-semibold text-slate-900 dark:text-white mb-4">Run History</h2>
          <p class="text-xs text-slate-500 dark:text-slate-400 mb-3">
            Recent cron executions (oldest → newest)
          </p>
          <.cron_history_grid cells={@grid_cells} base_path={@base_path} />
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
              navigate={"#{@base_path}/runs?flow=#{@cron_slug}"}
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

  attr(:cells, :list, required: true)
  attr(:base_path, :string, required: true)

  defp cron_history_grid(assigns) do
    cells = Enum.reverse(assigns.cells)
    assigns = assign(assigns, :cells, cells)

    ~H"""
    <div class="overflow-x-auto">
      <%= if @cells == [] do %>
        <p class="text-sm text-slate-500 dark:text-slate-400">No run history yet</p>
      <% else %>
        <div class="flex flex-wrap gap-1">
          <%= for cell <- @cells do %>
            <.link
              navigate={"#{@base_path}/runs/#{cell.run_id}"}
              class="w-4 h-4 rounded-sm transition-all hover:scale-125 hover:z-10 block focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-1"
              style={"background-color: #{cell_color(cell.status)}"}
              title={cell_tooltip(cell)}
            ></.link>
          <% end %>
        </div>

        <!-- Legend -->
        <div class="mt-4 flex items-center gap-4 text-sm text-slate-500 dark:text-slate-400">
          <div class="flex items-center gap-1.5">
            <div class="w-4 h-4 rounded-sm bg-emerald-500"></div>
            <span>Completed</span>
          </div>
          <div class="flex items-center gap-1.5">
            <div class="w-4 h-4 rounded-sm bg-rose-500"></div>
            <span>Failed</span>
          </div>
          <div class="flex items-center gap-1.5">
            <div class="w-4 h-4 rounded-sm bg-sky-500"></div>
            <span>Running</span>
          </div>
        </div>
      <% end %>
    </div>
    """
  end

  defp cell_color(status) do
    case status do
      s when s in [:completed, "completed"] -> "#10b981"
      s when s in [:failed, "failed"] -> "#ef4444"
      s when s in [:started, "started"] -> "#0ea5e9"
      _ -> "#cbd5e1"
    end
  end

  defp cell_tooltip(cell) do
    duration = format_duration_ms(cell.duration_ms)
    "Run #{String.slice(cell.run_id, 0..7)}\nStatus: #{cell.status}\nDuration: #{duration}"
  end

  defp format_duration_ms(nil), do: "-"
  defp format_duration_ms(%Decimal{} = d), do: "#{round(Decimal.to_float(d))}ms"
  defp format_duration_ms(ms) when is_float(ms), do: "#{round(ms)}ms"
  defp format_duration_ms(ms) when is_integer(ms), do: "#{ms}ms"
end
