defmodule PgFlowDashboard.Live.FlowsLive.Index do
  @moduledoc """
  Flows list page with statistics.
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
      |> assign(:page_title, "Flows")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> load_flows()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_flows()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  def handle_info(_, socket), do: {:noreply, socket}

  defp load_flows(socket) do
    flows = Queries.list_flows(socket.assigns.repo)
    assign(socket, :flows, flows)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:flows} base_path={@base_path}>
      <Layouts.page_header title="Flows" subtitle="Registered workflow definitions" />

      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        <%= if @flows == [] do %>
          <div class="col-span-full bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
            <p class="text-slate-500 dark:text-slate-400">No flows registered</p>
          </div>
        <% else %>
          <%= for flow <- @flows do %>
            <.link
              navigate={"#{@base_path}/flows/#{flow.flow_slug}"}
              class="block bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 hover:border-purple-300 dark:hover:border-purple-600 transition-colors"
            >
              <div class="flex items-start justify-between mb-3">
                <h3 class="text-lg font-semibold text-slate-900 dark:text-white">{flow.flow_slug}</h3>
                <span class="text-xs text-slate-500 dark:text-slate-400">{flow.step_count} steps</span>
              </div>

              <div class="grid grid-cols-3 gap-2 text-center mb-3">
                <div class="bg-slate-50 dark:bg-slate-900 rounded p-2">
                  <p class="text-lg font-semibold text-slate-900 dark:text-white">{flow.total_runs_24h}</p>
                  <p class="text-xs text-slate-500 dark:text-slate-400">runs</p>
                </div>
                <div class="bg-emerald-50 dark:bg-emerald-900/20 rounded p-2">
                  <p class="text-lg font-semibold text-emerald-600 dark:text-emerald-400">{flow.success_rate_24h}%</p>
                  <p class="text-xs text-slate-500 dark:text-slate-400">success</p>
                </div>
                <div class="bg-slate-50 dark:bg-slate-900 rounded p-2">
                  <p class="text-lg font-semibold text-slate-900 dark:text-white">{LiveHelpers.format_duration(flow.avg_duration_ms)}</p>
                  <p class="text-xs text-slate-500 dark:text-slate-400">avg</p>
                </div>
              </div>

              <div class="flex items-center justify-between text-xs text-slate-500 dark:text-slate-400">
                <span>Max attempts: {flow.opt_max_attempts}</span>
                <span>Timeout: {flow.opt_timeout}s</span>
              </div>
            </.link>
          <% end %>
        <% end %>
      </div>
    </Layouts.dashboard_layout>
    """
  end
end
