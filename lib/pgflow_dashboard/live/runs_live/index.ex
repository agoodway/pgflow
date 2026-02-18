defmodule PgFlowDashboard.Live.RunsLive.Index do
  @moduledoc """
  Runs list page with LiveFilter-based filtering.

  Uses URL-driven filters for shareable links and Ecto queries via QueryBuilder.
  """

  use Phoenix.LiveView

  alias LiveFilter.{Pagination, Params.Serializer, QueryBuilder}
  alias PgFlowDashboard.Components.{Layouts, ProgressBar, StatusBadge, TypeBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries.{Crons, Flows, Jobs}
  alias PgFlowDashboard.Schemas.Run

  defp filter_config(socket) do
    [
      LiveFilter.select(:flow_type,
        label: "Type",
        options: [
          {"Flow", "flow"},
          {"Job", "job"}
        ],
        icon: "hero-squares-2x2",
        default_visible: true
      ),
      LiveFilter.select(:flow_slug,
        label: "Queue",
        options_fn: fn -> queue_options(socket.assigns) end,
        icon: "hero-queue-list",
        default_visible: true
      ),
      LiveFilter.select(:status,
        label: "Status",
        options: [
          {"Running", "started"},
          {"Completed", "completed"},
          {"Failed", "failed"}
        ],
        icon: "hero-signal",
        default_visible: true
      ),
      LiveFilter.datetime_range(:started_at,
        label: "Started",
        icon: "hero-calendar-days",
        default_visible: true
      )
    ]
  end

  defp queue_options(assigns) do
    flows = Map.get(assigns, :flows, [])
    jobs = Map.get(assigns, :jobs, [])
    crons = Map.get(assigns, :crons, [])

    flow_opts = Enum.map(flows, fn f -> {f.flow_slug, f.flow_slug} end)
    job_opts = Enum.map(jobs, fn j -> {j.flow_slug, j.flow_slug} end)
    cron_opts = Enum.map(crons, fn c -> {c.flow_slug, c.flow_slug} end)

    (flow_opts ++ job_opts ++ cron_opts)
    |> Enum.uniq_by(fn {_, slug} -> slug end)
    |> Enum.sort_by(fn {label, _} -> label end)
  end

  @impl true
  def mount(_params, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Runs")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> load_flows_and_jobs()
      |> LiveHelpers.subscribe_to_updates()
      |> LiveHelpers.schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    config = filter_config(socket)

    # Redirect to default date range if no started_at filter
    if missing_date_filter?(params) do
      default_params = default_date_params()

      {:noreply,
       push_patch(socket,
         to: LiveFilter.to_path("#{socket.assigns.base_path}/runs", default_params)
       )}
    else
      {filters, remaining} = LiveFilter.from_params(params, config)
      {pagination, remaining} = LiveFilter.pagination_from_params(remaining, default_limit: 50)

      socket =
        socket
        |> LiveFilter.init(config, filters)
        |> assign(:pagination, pagination)
        |> assign(:remaining_params, remaining)
        |> load_runs()

      {:noreply, socket}
    end
  end

  @impl true
  def handle_info(
        {:live_filter, :updated, params},
        %{assigns: %{remaining_params: remaining_params, pagination: %{limit: limit}}} = socket
      ) do
    pagination_params = %{"limit" => to_string(limit), "offset" => "0"}
    all_params = Map.merge(remaining_params, params) |> Map.merge(pagination_params)

    {:noreply,
     push_patch(socket, to: LiveFilter.to_path("#{socket.assigns.base_path}/runs", all_params))}
  end

  @impl true
  def handle_info(
        {:live_filter, :page_changed, pagination_params},
        %{assigns: %{remaining_params: remaining_params, live_filter: %{filters: filters}}} =
          socket
      ) do
    filter_params = Serializer.to_params(filters)
    all_params = Map.merge(remaining_params, filter_params) |> Map.merge(pagination_params)

    {:noreply,
     push_patch(socket, to: LiveFilter.to_path("#{socket.assigns.base_path}/runs", all_params))}
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_runs()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_started, _}}, socket),
    do: {:noreply, load_runs(socket)}

  @impl true
  def handle_info({:pgflow, _run_id, {:run_completed, _}}, socket),
    do: {:noreply, load_runs(socket)}

  @impl true
  def handle_info({:pgflow, _run_id, {:run_failed, _}}, socket),
    do: {:noreply, load_runs(socket)}

  @impl true
  def handle_info(_, socket), do: {:noreply, socket}

  defp load_flows_and_jobs(socket) do
    flows = Flows.list_flows(socket.assigns.repo)
    jobs = Jobs.list_jobs(socket.assigns.repo)
    crons = Crons.list_crons(socket.assigns.repo)

    socket
    |> assign(:flows, flows)
    |> assign(:jobs, jobs)
    |> assign(:crons, crons)
  end

  defp load_runs(%{assigns: %{pagination: pagination, live_filter: %{filters: filters}}} = socket) do
    import Ecto.Query

    base_query =
      Run
      |> QueryBuilder.apply(filters,
        schema: Run,
        allowed_fields: [:flow_type, :flow_slug, :status, :started_at]
      )
      |> order_by([r], desc: r.started_at)

    total_count = QueryBuilder.count(base_query, socket.assigns.repo)

    runs =
      base_query
      |> QueryBuilder.apply_pagination(pagination)
      |> socket.assigns.repo.all()

    pagination = Pagination.with_total(pagination, total_count)

    socket
    |> assign(:runs, runs)
    |> assign(:pagination, pagination)
  end

  # Fallback when socket not yet initialized (PubSub messages before handle_params)
  defp load_runs(socket), do: socket

  defp missing_date_filter?(params) do
    has_legacy_range =
      Map.has_key?(params, "started_at.gte") || Map.has_key?(params, "started_at.lte")

    has_and_range =
      case Map.get(params, "and") do
        and_param when is_binary(and_param) ->
          String.contains?(and_param, "started_at.gte.") ||
            String.contains?(and_param, "started_at.lte.")

        _ ->
          false
      end

    !(has_legacy_range || has_and_range)
  end

  defp default_date_params do
    today = Date.utc_today()
    start_of_day = DateTime.new!(today, ~T[00:00:00], "Etc/UTC")
    end_of_day = DateTime.new!(today, ~T[23:59:59], "Etc/UTC")

    %{
      "and" =>
        "(started_at.gte.#{DateTime.to_iso8601(start_of_day)},started_at.lte.#{DateTime.to_iso8601(end_of_day)})",
      "limit" => "50",
      "offset" => "0"
    }
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:runs} base_path={@base_path}>
      <Layouts.page_header title="Runs" subtitle="All workflow executions" />

      <div class="mb-6 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
        <LiveFilter.bar filter={@live_filter} />
      </div>

      <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
        <div class="px-4 py-2 border-b border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 flex items-center justify-between">
          <span class="text-sm text-slate-500 dark:text-slate-400">
            Showing {length(@runs)} of {@pagination.total_count} runs
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
          <tbody class="divide-y divide-slate-200 dark:divide-slate-700">
            <tr :if={@runs == []} id="runs-empty">
              <td colspan="6" class="px-4 py-8 text-center text-slate-500 dark:text-slate-400">
                No runs found
              </td>
            </tr>
            <tr
              :for={run <- @runs}
              id={"run-#{run.run_id}"}
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
                  <TypeBadge.type_badge type={run.flow_type} />
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
                <%= if run.flow_type in ["job", "cron"] do %>
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

        <div class="px-4 py-3 border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50">
          <LiveFilter.paginator pagination={@pagination} />
        </div>
      </div>
    </Layouts.dashboard_layout>
    """
  end
end
