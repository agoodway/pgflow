defmodule PgFlowDashboard.Live.FlowsLive.Index do
  @moduledoc """
  Flows list page with statistics.

  Shows card view for ≤12 flows, switches to paginated list view for more.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{Layouts, TypeBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries.Flows

  @card_threshold 12
  @page_size 50

  @impl true
  def mount(_params, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Flows")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:cursor, nil)
      |> assign(:has_more, false)
      |> assign(:total_count, 0)
      |> assign(:flows_count, 0)
      |> stream_configure(:flows, dom_id: &"flow-#{&1.flow_slug}")
      |> stream(:flows, [])
      |> load_flows()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_event("load_more", _, socket) do
    flows =
      Flows.list_flows(socket.assigns.repo,
        cursor: socket.assigns.cursor,
        limit: @page_size + 1
      )

    {flows, has_more} = LiveHelpers.paginate_results(flows, @page_size)
    new_cursor = if flows != [], do: List.last(flows).flow_slug, else: nil
    new_count = socket.assigns.flows_count + length(flows)

    socket =
      socket
      |> stream(:flows, flows)
      |> assign(:cursor, new_cursor)
      |> assign(:has_more, has_more)
      |> assign(:flows_count, new_count)

    {:noreply, socket}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> refresh_flows()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  def handle_info(_, socket), do: {:noreply, socket}

  defp load_flows(socket) do
    total_count = Flows.count_flows(socket.assigns.repo)
    view_mode = LiveHelpers.determine_view_mode(total_count, @card_threshold)

    case view_mode do
      :card ->
        flows = Flows.list_flows(socket.assigns.repo)

        socket
        |> assign(:view_mode, :card)
        |> assign(:total_count, total_count)
        |> assign(:flows, flows)

      :list ->
        flows = Flows.list_flows(socket.assigns.repo, limit: @page_size + 1)
        {flows, has_more} = LiveHelpers.paginate_results(flows, @page_size)
        cursor = if flows != [], do: List.last(flows).flow_slug, else: nil

        socket
        |> assign(:view_mode, :list)
        |> assign(:total_count, total_count)
        |> assign(:flows_count, length(flows))
        |> assign(:cursor, cursor)
        |> assign(:has_more, has_more)
        |> stream(:flows, flows, reset: true)
    end
  end

  defp refresh_flows(socket) do
    total_count = Flows.count_flows(socket.assigns.repo)
    new_view_mode = LiveHelpers.determine_view_mode(total_count, @card_threshold)

    if new_view_mode != socket.assigns.view_mode do
      load_flows(socket)
    else
      update_flows(socket, total_count)
    end
  end

  defp update_flows(socket, total_count) do
    case socket.assigns.view_mode do
      :card ->
        flows = Flows.list_flows(socket.assigns.repo)

        socket
        |> assign(:total_count, total_count)
        |> assign(:flows, flows)

      :list ->
        current_count = max(socket.assigns.flows_count, @page_size)
        flows = Flows.list_flows(socket.assigns.repo, limit: current_count + 1)
        {flows, has_more} = LiveHelpers.paginate_results(flows, current_count)
        cursor = if flows != [], do: List.last(flows).flow_slug, else: nil

        socket
        |> assign(:total_count, total_count)
        |> assign(:flows_count, length(flows))
        |> assign(:cursor, cursor)
        |> assign(:has_more, has_more)
        |> stream(:flows, flows, reset: true)
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:flows} base_path={@base_path}>
      <Layouts.page_header title="Flows" subtitle="Registered workflow definitions" />

      <%= if @view_mode == :card do %>
        <.card_view flows={@flows} base_path={@base_path} />
      <% else %>
        <.list_view
          streams={@streams}
          base_path={@base_path}
          flows_count={@flows_count}
          total_count={@total_count}
          has_more={@has_more}
          time_zone={@time_zone}
        />
      <% end %>
    </Layouts.dashboard_layout>
    """
  end

  defp card_view(assigns) do
    ~H"""
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
              <TypeBadge.type_badge type="flow" />
            </div>

            <div class="grid grid-cols-3 gap-2 text-center mb-3">
              <div class="bg-slate-50 dark:bg-slate-900 rounded p-2">
                <p class="text-lg font-semibold text-slate-900 dark:text-white">{flow.total_runs_24h}</p>
                <p class="text-xs text-slate-500 dark:text-slate-400">runs</p>
              </div>
              <div class="bg-emerald-50 dark:bg-emerald-900/20 rounded p-2">
                <p class="text-lg font-semibold text-emerald-600 dark:text-emerald-400">{LiveHelpers.format_percent(flow.success_rate_24h)}%</p>
                <p class="text-xs text-slate-500 dark:text-slate-400">success</p>
              </div>
              <div class="bg-slate-50 dark:bg-slate-900 rounded p-2">
                <p class="text-lg font-semibold text-slate-900 dark:text-white">{LiveHelpers.format_duration(flow.avg_duration_ms)}</p>
                <p class="text-xs text-slate-500 dark:text-slate-400">avg</p>
              </div>
            </div>

            <div class="flex items-center justify-between text-xs text-slate-500 dark:text-slate-400">
              <span>{flow.step_count} steps</span>
              <span>{flow.opt_max_attempts} attempts · {flow.opt_timeout}s timeout</span>
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
          Showing {@flows_count} of {@total_count} flows
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
              Steps
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Config
            </th>
          </tr>
        </thead>
        <tbody id="flows-list" phx-update="stream" class="divide-y divide-slate-200 dark:divide-slate-700">
          <tr :if={@flows_count == 0} id="flows-empty">
            <td colspan="6" class="px-4 py-8 text-center text-slate-500 dark:text-slate-400">
              No flows registered
            </td>
          </tr>
          <tr
            :for={{dom_id, flow} <- @streams.flows}
            id={dom_id}
            class="hover:bg-slate-50 dark:hover:bg-slate-700/50"
          >
            <td class="px-4 py-3">
              <.link
                navigate={"#{@base_path}/flows/#{flow.flow_slug}"}
                class="flex items-center gap-2"
              >
                <span class="text-sm font-medium text-purple-600 hover:text-purple-700 dark:text-purple-400">
                  {flow.flow_slug}
                </span>
                <TypeBadge.type_badge type="flow" />
              </.link>
            </td>
            <td class="px-4 py-3 text-sm text-slate-700 dark:text-slate-300">
              {flow.total_runs_24h}
            </td>
            <td class="px-4 py-3 text-sm text-emerald-600 dark:text-emerald-400">
              {LiveHelpers.format_percent(flow.success_rate_24h)}%
            </td>
            <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
              {LiveHelpers.format_duration(flow.avg_duration_ms)}
            </td>
            <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
              {flow.step_count}
            </td>
            <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
              {flow.opt_max_attempts} attempts · {flow.opt_timeout}s
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
          class="w-full py-2 px-4 text-sm font-medium text-purple-600 hover:text-purple-700 dark:text-purple-400 dark:hover:text-purple-300 hover:bg-purple-50 dark:hover:bg-purple-900/20 rounded-md transition-colors"
        >
          Load more flows
        </button>
      </div>
    </div>
    """
  end
end
