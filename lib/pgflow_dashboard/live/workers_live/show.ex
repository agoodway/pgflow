defmodule PgFlowDashboard.Live.WorkersLive.Show do
  @moduledoc """
  Worker detail page showing worker info and task history.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{HealthBadge, Layouts, StatusBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries

  @impl true
  def mount(%{"id" => worker_id}, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Worker Details")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:worker_id, worker_id)
      |> load_worker()
      |> load_worker_tasks()

    if socket.assigns.worker do
      socket =
        socket
        |> LiveHelpers.subscribe_to_updates()
        |> LiveHelpers.schedule_refresh()

      {:ok, socket}
    else
      {:ok, push_navigate(socket, to: "#{socket.assigns.base_path}/workers")}
    end
  end

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_worker()
      |> load_worker_tasks()
      |> LiveHelpers.schedule_refresh()

    {:noreply, socket}
  end

  def handle_info({:worker_heartbeat, _}, socket) do
    {:noreply, load_worker(socket)}
  end

  def handle_info(_, socket), do: {:noreply, socket}

  @impl true
  def handle_event("handle_keydown", %{"key" => "]"}, socket) do
    {:noreply, navigate_to_adjacent_worker(socket, :next)}
  end

  def handle_event("handle_keydown", %{"key" => "["}, socket) do
    {:noreply, navigate_to_adjacent_worker(socket, :prev)}
  end

  def handle_event("handle_keydown", _, socket), do: {:noreply, socket}

  defp navigate_to_adjacent_worker(socket, direction) do
    case Queries.get_adjacent_worker(socket.assigns.repo, socket.assigns.worker_id, direction) do
      {:ok, adjacent_worker_id} ->
        push_navigate(socket, to: "#{socket.assigns.base_path}/workers/#{adjacent_worker_id}")

      {:error, :not_found} ->
        socket
    end
  end

  defp load_worker(socket) do
    case Queries.get_worker(socket.assigns.repo, socket.assigns.worker_id) do
      {:ok, worker} -> assign(socket, :worker, worker)
      {:error, _} -> assign(socket, :worker, nil)
    end
  end

  defp load_worker_tasks(socket) do
    if socket.assigns.worker do
      tasks = Queries.list_worker_tasks(socket.assigns.repo, socket.assigns.worker_id, limit: 50)
      assign(socket, :tasks, tasks)
    else
      assign(socket, :tasks, [])
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:workers} base_path={@base_path}>
      <div :if={@worker} phx-window-keydown="handle_keydown">
        <!-- Header -->
        <div class="mb-6">
          <.link navigate={"#{@base_path}/workers"} class="text-sm text-slate-500 hover:text-slate-700 dark:text-slate-400 mb-2 inline-block">
            ← Back to workers
          </.link>
          <div class="flex items-center justify-between">
            <div>
              <h1 class="text-2xl font-bold text-slate-900 dark:text-white flex items-center gap-3">
                {@worker.flow_slug}
                <HealthBadge.health_badge status={@worker.health_status} />
              </h1>
              <p class="mt-1 text-sm text-slate-500 dark:text-slate-400 font-mono">{@worker.worker_id}</p>
            </div>
          </div>
        </div>

        <!-- Worker Info Cards -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Health Status</p>
            <p class="text-lg font-semibold text-slate-900 dark:text-white capitalize">{@worker.health_status}</p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Active Tasks</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">{@worker.active_tasks}</p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Completed (24h)</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">{@worker.completed_tasks_24h}</p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Last Heartbeat</p>
            <p class="text-sm font-medium text-slate-900 dark:text-white">{LiveHelpers.format_timestamp(@worker.last_heartbeat_at, @time_zone)}</p>
          </div>
        </div>

        <!-- Recent Tasks -->
        <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
          <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700">
            <h2 class="text-sm font-semibold text-slate-900 dark:text-white">Recent Tasks</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400 mt-1">Tasks processed by this worker</p>
          </div>
          <table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
            <thead class="bg-slate-50 dark:bg-slate-800/50">
              <tr>
                <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Status</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Run</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Step</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Attempts</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Completed</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-200 dark:divide-slate-700">
              <%= if @tasks == [] do %>
                <tr>
                  <td colspan="5" class="px-4 py-8 text-center text-slate-500 dark:text-slate-400">
                    No tasks found for this worker
                  </td>
                </tr>
              <% else %>
                <%= for task <- @tasks do %>
                  <tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
                    <td class="px-4 py-3">
                      <StatusBadge.status_badge status={task.status} size={:sm} />
                    </td>
                    <td class="px-4 py-3">
                      <.link
                        navigate={"#{@base_path}/runs/#{task.run_id}"}
                        class="text-sm font-mono text-purple-600 hover:text-purple-700 dark:text-purple-400"
                      >
                        {LiveHelpers.short_id(task.run_id)}
                      </.link>
                    </td>
                    <td class="px-4 py-3 text-sm text-slate-700 dark:text-slate-300">{task.step_slug}</td>
                    <td class="px-4 py-3 text-sm text-slate-700 dark:text-slate-300">{task.attempts_count}</td>
                    <td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">
                      {LiveHelpers.format_timestamp(task.completed_at, @time_zone)}
                    </td>
                  </tr>
                <% end %>
              <% end %>
            </tbody>
          </table>
        </div>
      </div>
    </Layouts.dashboard_layout>
    """
  end
end
