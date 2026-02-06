defmodule PgFlowDashboard.Live.CronsLive.Index do
  @moduledoc """
  Crons list page with statistics and schedule info.
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
      |> assign(:page_title, "Crons")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> load_crons()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_crons()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  @impl true
  def handle_info(_, socket), do: {:noreply, socket}

  defp load_crons(socket) do
    crons = Queries.list_crons(socket.assigns.repo)
    assign(socket, :crons, crons)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:crons} base_path={@base_path}>
      <Layouts.page_header title="Crons" subtitle="Scheduled recurring jobs" />

      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        <%= if @crons == [] do %>
          <div class="col-span-full bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
            <p class="text-slate-500 dark:text-slate-400">No crons registered</p>
          </div>
        <% else %>
          <%= for cron <- @crons do %>
            <.link
              navigate={"#{@base_path}/crons/#{cron.flow_slug}"}
              class="block bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 hover:border-amber-300 dark:hover:border-amber-600 transition-colors"
            >
              <div class="flex items-start justify-between mb-2">
                <h3 class="text-lg font-semibold text-slate-900 dark:text-white">{cron.flow_slug}</h3>
                <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-400">
                  cron
                </span>
              </div>

              <div class="mb-3">
                <p class="text-sm text-slate-700 dark:text-slate-300">
                  {cron.human_schedule || "Custom schedule"}
                </p>
                <p class="text-xs text-slate-500 dark:text-slate-400 font-mono">
                  ({cron.cron_expression || "—"})
                </p>
              </div>

              <div class="grid grid-cols-3 gap-2 text-center mb-3">
                <div class="bg-slate-50 dark:bg-slate-900 rounded p-2">
                  <p class="text-lg font-semibold text-slate-900 dark:text-white">{cron.total_runs_24h}</p>
                  <p class="text-xs text-slate-500 dark:text-slate-400">runs</p>
                </div>
                <div class="bg-emerald-50 dark:bg-emerald-900/20 rounded p-2">
                  <p class="text-lg font-semibold text-emerald-600 dark:text-emerald-400">{cron.success_rate_24h}%</p>
                  <p class="text-xs text-slate-500 dark:text-slate-400">success</p>
                </div>
                <div class="bg-slate-50 dark:bg-slate-900 rounded p-2">
                  <p class="text-lg font-semibold text-slate-900 dark:text-white">{LiveHelpers.format_duration(cron.avg_duration_ms)}</p>
                  <p class="text-xs text-slate-500 dark:text-slate-400">avg</p>
                </div>
              </div>

              <div class="flex items-center justify-between text-xs text-slate-500 dark:text-slate-400">
                <span :if={cron.next_run_at}>
                  Next: {LiveHelpers.format_relative_time(cron.next_run_at)}
                </span>
                <span :if={!cron.next_run_at}>
                  Next: —
                </span>
                <span :if={cron.is_active} class="text-emerald-600 dark:text-emerald-400">Active</span>
                <span :if={!cron.is_active} class="text-slate-400 dark:text-slate-500">Inactive</span>
              </div>
            </.link>
          <% end %>
        <% end %>
      </div>
    </Layouts.dashboard_layout>
    """
  end
end
