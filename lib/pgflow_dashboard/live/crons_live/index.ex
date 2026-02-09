defmodule PgFlowDashboard.Live.CronsLive.Index do
  @moduledoc """
  Crons list page with statistics and schedule info.

  Shows card view for ≤12 crons, switches to paginated list view for more.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{Layouts, TypeBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries.Crons

  @card_threshold 12
  @page_size 50

  @impl true
  def mount(_params, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Crons")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:cursor, nil)
      |> assign(:has_more, false)
      |> assign(:total_count, 0)
      |> assign(:crons_count, 0)
      |> stream_configure(:crons, dom_id: &"cron-#{&1.flow_slug}")
      |> stream(:crons, [])
      |> load_crons()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_event("load_more", _, socket) do
    crons =
      Crons.list_crons(socket.assigns.repo,
        cursor: socket.assigns.cursor,
        limit: @page_size + 1
      )

    {crons, has_more} = LiveHelpers.paginate_results(crons, @page_size)
    new_cursor = if crons != [], do: List.last(crons).flow_slug, else: nil
    new_count = socket.assigns.crons_count + length(crons)

    socket =
      socket
      |> stream(:crons, crons)
      |> assign(:cursor, new_cursor)
      |> assign(:has_more, has_more)
      |> assign(:crons_count, new_count)

    {:noreply, socket}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> refresh_crons()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  @impl true
  def handle_info(_, socket), do: {:noreply, socket}

  defp load_crons(socket) do
    total_count = Crons.count_crons(socket.assigns.repo)
    view_mode = LiveHelpers.determine_view_mode(total_count, @card_threshold)

    case view_mode do
      :card ->
        crons = Crons.list_crons(socket.assigns.repo)

        socket
        |> assign(:view_mode, :card)
        |> assign(:total_count, total_count)
        |> assign(:crons, crons)

      :list ->
        crons = Crons.list_crons(socket.assigns.repo, limit: @page_size + 1)
        {crons, has_more} = LiveHelpers.paginate_results(crons, @page_size)
        cursor = if crons != [], do: List.last(crons).flow_slug, else: nil

        socket
        |> assign(:view_mode, :list)
        |> assign(:total_count, total_count)
        |> assign(:crons_count, length(crons))
        |> assign(:cursor, cursor)
        |> assign(:has_more, has_more)
        |> stream(:crons, crons, reset: true)
    end
  end

  defp refresh_crons(socket) do
    total_count = Crons.count_crons(socket.assigns.repo)
    new_view_mode = LiveHelpers.determine_view_mode(total_count, @card_threshold)

    if new_view_mode != socket.assigns.view_mode do
      load_crons(socket)
    else
      update_crons(socket, total_count)
    end
  end

  defp update_crons(socket, total_count) do
    case socket.assigns.view_mode do
      :card ->
        crons = Crons.list_crons(socket.assigns.repo)

        socket
        |> assign(:total_count, total_count)
        |> assign(:crons, crons)

      :list ->
        current_count = max(socket.assigns.crons_count, @page_size)
        crons = Crons.list_crons(socket.assigns.repo, limit: current_count + 1)
        {crons, has_more} = LiveHelpers.paginate_results(crons, current_count)
        cursor = if crons != [], do: List.last(crons).flow_slug, else: nil

        socket
        |> assign(:total_count, total_count)
        |> assign(:crons_count, length(crons))
        |> assign(:cursor, cursor)
        |> assign(:has_more, has_more)
        |> stream(:crons, crons, reset: true)
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:crons} base_path={@base_path}>
      <Layouts.page_header title="Crons" subtitle="Scheduled recurring jobs" />

      <%= if @view_mode == :card do %>
        <.card_view crons={@crons} base_path={@base_path} />
      <% else %>
        <.list_view
          streams={@streams}
          base_path={@base_path}
          crons_count={@crons_count}
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
              <div class="flex gap-1">
                <TypeBadge.type_badge type={cron.flow_type} />
                <TypeBadge.type_badge type="cron" />
              </div>
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
    """
  end

  defp list_view(assigns) do
    ~H"""
    <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
      <div class="px-4 py-2 border-b border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 flex items-center justify-between">
        <span class="text-sm text-slate-500 dark:text-slate-400">
          Showing {@crons_count} of {@total_count} crons
        </span>
      </div>
      <table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
        <thead class="bg-slate-50 dark:bg-slate-800/50">
          <tr>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Name
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Schedule
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Runs (24h)
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Success
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Next Run
            </th>
            <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">
              Status
            </th>
          </tr>
        </thead>
        <tbody id="crons-list" phx-update="stream" class="divide-y divide-slate-200 dark:divide-slate-700">
          <tr :if={@crons_count == 0} id="crons-empty">
            <td colspan="6" class="px-4 py-8 text-center text-slate-500 dark:text-slate-400">
              No crons registered
            </td>
          </tr>
          <tr
            :for={{dom_id, cron} <- @streams.crons}
            id={dom_id}
            class="hover:bg-slate-50 dark:hover:bg-slate-700/50"
          >
            <td class="px-4 py-3">
              <.link
                navigate={"#{@base_path}/crons/#{cron.flow_slug}"}
                class="flex items-center gap-2"
              >
                <span class="text-sm font-medium text-amber-600 hover:text-amber-700 dark:text-amber-400">
                  {cron.flow_slug}
                </span>
                <TypeBadge.type_badge type={cron.flow_type} />
                <TypeBadge.type_badge type="cron" />
              </.link>
            </td>
            <td class="px-4 py-3">
              <div class="text-sm text-slate-700 dark:text-slate-300">
                {cron.human_schedule || "Custom"}
              </div>
              <div class="text-xs text-slate-400 dark:text-slate-500 font-mono">
                {cron.cron_expression}
              </div>
            </td>
            <td class="px-4 py-3 text-sm text-slate-700 dark:text-slate-300">
              {cron.total_runs_24h}
            </td>
            <td class="px-4 py-3 text-sm text-emerald-600 dark:text-emerald-400">
              {cron.success_rate_24h}%
            </td>
            <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
              <%= if cron.next_run_at do %>
                {LiveHelpers.format_relative_time(cron.next_run_at)}
              <% else %>
                —
              <% end %>
            </td>
            <td class="px-4 py-3">
              <span
                :if={cron.is_active}
                class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-400"
              >
                Active
              </span>
              <span
                :if={!cron.is_active}
                class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-400"
              >
                Inactive
              </span>
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
          class="w-full py-2 px-4 text-sm font-medium text-amber-600 hover:text-amber-700 dark:text-amber-400 dark:hover:text-amber-300 hover:bg-amber-50 dark:hover:bg-amber-900/20 rounded-md transition-colors"
        >
          Load more crons
        </button>
      </div>
    </div>
    """
  end
end
