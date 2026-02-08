defmodule PgFlowDashboard.Live.FlowsLive.Show do
  @moduledoc """
  Flow detail page with dependency graph and run history grid.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{DependencyGraph, Layouts, RunHistoryGrid}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries.Flows

  @impl true
  def mount(%{"slug" => flow_slug}, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, flow_slug)
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:flow_slug, flow_slug)
      |> load_flow()
      |> load_run_history()

    if socket.assigns.flow do
      socket =
        socket
        |> LiveHelpers.subscribe_to_updates()
        |> LiveHelpers.schedule_refresh()

      {:ok, socket}
    else
      {:ok, push_navigate(socket, to: "#{socket.assigns.base_path}/flows")}
    end
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_flow()
      |> load_run_history()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_completed, _}}, socket) do
    {:noreply, load_run_history(socket)}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_failed, _}}, socket) do
    {:noreply, load_run_history(socket)}
  end

  @impl true
  def handle_info(_, socket), do: {:noreply, socket}

  defp load_flow(socket) do
    case Flows.get_flow_with_graph(socket.assigns.repo, socket.assigns.flow_slug) do
      {:ok, flow} -> assign(socket, :flow, flow)
      {:error, _} -> assign(socket, :flow, nil)
    end
  end

  defp load_run_history(socket) do
    grid_data =
      Flows.get_run_history_grid(
        socket.assigns.repo,
        socket.assigns.flow_slug,
        limit: socket.assigns.config[:max_grid_runs] || 50
      )

    step_slugs =
      if socket.assigns.flow do
        Enum.map(socket.assigns.flow.steps, & &1.step_slug)
      else
        Map.keys(grid_data)
      end

    socket
    |> assign(:grid_data, grid_data)
    |> assign(:step_slugs, step_slugs)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:flows} base_path={@base_path}>
      <div :if={@flow}>
        <!-- Header -->
        <div class="mb-6">
          <.link navigate={"#{@base_path}/flows"} class="text-sm text-slate-500 hover:text-slate-700 dark:text-slate-400 mb-2 inline-block">
            ← Back to flows
          </.link>
          <h1 class="text-2xl font-bold text-slate-900 dark:text-white">{@flow.flow_slug}</h1>
          <p class="mt-1 text-sm text-slate-500 dark:text-slate-400">
            {@flow.step_count} steps · Max {@ flow.opt_max_attempts} attempts · {@ flow.opt_timeout}s timeout
          </p>
        </div>

        <!-- Stats -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Total Runs (24h)</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">{@flow.total_runs_24h}</p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Success Rate</p>
            <p class="text-2xl font-semibold text-emerald-600 dark:text-emerald-400">{@flow.success_rate_24h}%</p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Avg Duration</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">{LiveHelpers.format_duration(@flow.avg_duration_ms)}</p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">P95 Duration</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">{LiveHelpers.format_duration(@flow.p95_duration_ms)}</p>
          </div>
        </div>

        <!-- Workflow -->
        <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 mb-6">
          <h2 class="text-sm font-semibold text-slate-900 dark:text-white mb-4">Workflow</h2>
          <DependencyGraph.dependency_graph steps={@flow.steps} />
        </div>

        <!-- Run History Grid -->
        <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
          <h2 class="text-sm font-semibold text-slate-900 dark:text-white mb-4">Run History</h2>
          <p class="text-xs text-slate-500 dark:text-slate-400 mb-3">Click a cell to view run details with step selected</p>
          <RunHistoryGrid.run_history_grid
            grid_data={@grid_data}
            steps={@step_slugs}
            max_runs={@config[:max_grid_runs] || 50}
            base_path={@base_path}
          />
        </div>
      </div>
    </Layouts.dashboard_layout>
    """
  end
end
