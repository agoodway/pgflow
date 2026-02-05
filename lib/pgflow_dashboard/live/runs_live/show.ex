defmodule PgFlowDashboard.Live.RunsLive.Show do
  @moduledoc """
  Run detail page with step states and dependency graph.
  """

  use Phoenix.LiveView

  alias PgFlowDashboard.Components.{DependencyGraph, GanttTimeline, Layouts, ProgressBar, StatusBadge}
  alias PgFlowDashboard.Live.LiveHelpers
  alias PgFlowDashboard.Queries

  @impl true
  def mount(%{"id" => run_id}, session, socket) do
    {:cont, socket} = LiveHelpers.on_mount(session, socket)

    socket =
      socket
      |> assign(:page_title, "Run Details")
      |> assign(:base_path, session["base_path"] || "/pgflow")
      |> assign(:run_id, run_id)
      |> assign(:selected_step, nil)
      |> assign(:step_tasks, [])
      |> load_run()
      |> load_step_states()
      |> load_flow_steps()

    if socket.assigns.run do
      socket =
        socket
        |> LiveHelpers.subscribe_to_run(run_id)
        |> LiveHelpers.schedule_refresh()

      {:ok, socket}
    else
      {:ok, push_navigate(socket, to: "#{socket.assigns.base_path}/runs")}
    end
  end

  @impl true
  def handle_params(%{"step" => step_slug}, _uri, socket) do
    # Pre-select step from query parameter (e.g., from run history grid click)
    tasks = Queries.list_step_tasks(socket.assigns.repo, socket.assigns.run_id, step_slug)

    socket =
      socket
      |> assign(:selected_step, step_slug)
      |> assign(:step_tasks, tasks)

    {:noreply, socket}
  end

  def handle_params(_params, _uri, socket), do: {:noreply, socket}

  @impl true
  def handle_info(:refresh, socket) do
    socket =
      socket
      |> load_run()
      |> load_step_states()

    if socket.assigns.run && socket.assigns.run.status == "started" do
      {:noreply, LiveHelpers.schedule_refresh(socket)}
    else
      {:noreply, socket}
    end
  end

  def handle_info({:task_started, %{step_slug: _}}, socket) do
    {:noreply, load_step_states(socket)}
  end

  def handle_info({:task_completed, _}, socket) do
    socket =
      socket
      |> load_run()
      |> load_step_states()

    {:noreply, socket}
  end

  def handle_info({:task_failed, _}, socket) do
    socket =
      socket
      |> load_run()
      |> load_step_states()

    {:noreply, socket}
  end

  def handle_info({:run_completed, _}, socket) do
    socket =
      socket
      |> load_run()
      |> load_step_states()

    {:noreply, socket}
  end

  def handle_info({:run_failed, _}, socket) do
    socket =
      socket
      |> load_run()
      |> load_step_states()

    {:noreply, socket}
  end

  def handle_info(_, socket), do: {:noreply, socket}

  @impl true
  def handle_event("select_step", %{"step" => step_slug}, socket) do
    socket =
      if socket.assigns.selected_step == step_slug do
        # Deselect if clicking the same step
        socket
        |> assign(:selected_step, nil)
        |> assign(:step_tasks, [])
      else
        # Select the step and load its tasks
        tasks = Queries.list_step_tasks(socket.assigns.repo, socket.assigns.run_id, step_slug)

        socket
        |> assign(:selected_step, step_slug)
        |> assign(:step_tasks, tasks)
      end

    {:noreply, socket}
  end

  def handle_event("clear_selection", _, socket) do
    socket =
      socket
      |> assign(:selected_step, nil)
      |> assign(:step_tasks, [])

    {:noreply, socket}
  end

  def handle_event("next_step", _, socket) do
    {:noreply, navigate_step(socket, :next)}
  end

  def handle_event("prev_step", _, socket) do
    {:noreply, navigate_step(socket, :prev)}
  end

  def handle_event("handle_keydown", %{"key" => "j"}, socket) do
    {:noreply, navigate_step(socket, :next)}
  end

  def handle_event("handle_keydown", %{"key" => "k"}, socket) do
    {:noreply, navigate_step(socket, :prev)}
  end

  def handle_event("handle_keydown", %{"key" => "Escape"}, socket) do
    socket =
      socket
      |> assign(:selected_step, nil)
      |> assign(:step_tasks, [])

    {:noreply, socket}
  end

  def handle_event("handle_keydown", %{"key" => "]"}, socket) do
    {:noreply, navigate_to_adjacent_run(socket, :next)}
  end

  def handle_event("handle_keydown", %{"key" => "["}, socket) do
    {:noreply, navigate_to_adjacent_run(socket, :prev)}
  end

  def handle_event("handle_keydown", _, socket), do: {:noreply, socket}

  defp navigate_to_adjacent_run(socket, direction) do
    case Queries.get_adjacent_run(socket.assigns.repo, socket.assigns.run_id, direction) do
      {:ok, adjacent_run_id} ->
        push_navigate(socket, to: "#{socket.assigns.base_path}/runs/#{adjacent_run_id}")

      {:error, :not_found} ->
        socket
    end
  end

  defp navigate_step(socket, direction) do
    step_slugs = Enum.map(socket.assigns.step_states, & &1.step_slug)

    case {socket.assigns.selected_step, step_slugs} do
      {nil, [first | _]} when direction == :next ->
        select_step(socket, first)

      {nil, slugs} when direction == :prev ->
        select_step(socket, List.last(slugs))

      {current, slugs} ->
        current_idx = Enum.find_index(slugs, &(&1 == current))

        new_idx =
          case direction do
            :next -> min(current_idx + 1, length(slugs) - 1)
            :prev -> max(current_idx - 1, 0)
          end

        new_step = Enum.at(slugs, new_idx)
        select_step(socket, new_step)
    end
  end

  defp select_step(socket, step_slug) do
    tasks = Queries.list_step_tasks(socket.assigns.repo, socket.assigns.run_id, step_slug)

    socket
    |> assign(:selected_step, step_slug)
    |> assign(:step_tasks, tasks)
  end

  defp load_run(socket) do
    case Queries.get_run(socket.assigns.repo, socket.assigns.run_id) do
      {:ok, run} -> assign(socket, :run, run)
      {:error, _} -> assign(socket, :run, nil)
    end
  end

  defp load_step_states(socket) do
    states = Queries.list_step_states(socket.assigns.repo, socket.assigns.run_id)
    state_map = Map.new(states, fn s -> {s.step_slug, s.status} end)

    socket
    |> assign(:step_states, states)
    |> assign(:step_state_map, state_map)
  end

  defp load_flow_steps(socket) do
    if socket.assigns.run do
      case Queries.get_flow_with_graph(socket.assigns.repo, socket.assigns.run.flow_slug) do
        {:ok, flow} -> assign(socket, :flow_steps, flow.steps)
        {:error, _} -> assign(socket, :flow_steps, [])
      end
    else
      assign(socket, :flow_steps, [])
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.dashboard_layout current_page={:runs} base_path={@base_path}>
      <div :if={@run} phx-window-keydown="handle_keydown">
        <!-- Header -->
        <div class="mb-6">
          <.link navigate={"#{@base_path}/runs"} class="text-sm text-slate-500 hover:text-slate-700 dark:text-slate-400 mb-2 inline-block">
            ← Back to runs
          </.link>
          <div class="flex items-center justify-between">
            <div>
              <h1 class="text-2xl font-bold text-slate-900 dark:text-white flex items-center gap-3">
                {@run.flow_slug}
                <StatusBadge.status_badge status={@run.status} pulse={@run.status == "started"} />
              </h1>
              <p class="mt-1 text-sm text-slate-500 dark:text-slate-400 font-mono">{@run.run_id}</p>
            </div>
          </div>
        </div>

        <!-- Progress and Timing -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400 mb-2">Progress</p>
            <ProgressBar.progress_bar
              progress={@run.progress_percent}
              completed={@run.completed_steps}
              total={@run.total_steps}
              failed={@run.failed_steps}
            />
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Duration</p>
            <p class="text-2xl font-semibold text-slate-900 dark:text-white">{LiveHelpers.format_duration(@run.duration_ms)}</p>
          </div>
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <p class="text-sm text-slate-500 dark:text-slate-400">Started</p>
            <p class="text-sm font-medium text-slate-900 dark:text-white">{LiveHelpers.format_timestamp(@run.started_at, @time_zone)}</p>
          </div>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <!-- Dependency Graph -->
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
            <h2 class="text-sm font-semibold text-slate-900 dark:text-white mb-4">Dependency Graph</h2>
            <p class="text-xs text-slate-500 dark:text-slate-400 mb-3">Click a node to view its output</p>
            <DependencyGraph.dependency_graph
              steps={@flow_steps}
              step_states={@step_state_map}
              highlighted_step={@selected_step}
              on_click="select_step"
            />
          </div>

          <!-- Right column: Step States + Gantt Timeline -->
          <div class="flex flex-col gap-6">
            <!-- Step States -->
            <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
              <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700">
                <h2 class="text-sm font-semibold text-slate-900 dark:text-white">Step States</h2>
                <p class="text-xs text-slate-500 dark:text-slate-400 mt-1">Click a step to view its output</p>
              </div>
              <div class="divide-y divide-slate-200 dark:divide-slate-700 max-h-64 overflow-y-auto">
                <%= if @step_states == [] do %>
                  <div class="px-4 py-8 text-center text-slate-500 dark:text-slate-400 text-sm">
                    No step states yet
                  </div>
                <% else %>
                  <%= for state <- @step_states do %>
                    <div
                      phx-click="select_step"
                      phx-value-step={state.step_slug}
                      class={[
                        "px-4 py-3 cursor-pointer transition-colors",
                        @selected_step == state.step_slug && "bg-purple-50 dark:bg-purple-900/20 border-l-2 border-l-purple-500 !border-b-transparent",
                        @selected_step != state.step_slug && "hover:bg-slate-50 dark:hover:bg-slate-700/50"
                      ]}
                    >
                      <div class="flex items-center justify-between">
                        <div class="flex items-center gap-3">
                          <StatusBadge.status_badge status={state.status} size={:sm} pulse={state.status == "started"} />
                          <span class="text-sm font-medium text-slate-900 dark:text-white">{state.step_slug}</span>
                        </div>
                        <span class="text-xs text-slate-500 dark:text-slate-400">
                          {LiveHelpers.format_duration(state.duration_ms)}
                        </span>
                      </div>
                      <div :if={state.total_tasks > 0} class="mt-2 text-xs text-slate-500 dark:text-slate-400">
                        Tasks: {state.completed_tasks}/{state.total_tasks}
                        <span :if={state.failed_tasks > 0} class="text-rose-500">({state.failed_tasks} failed)</span>
                      </div>
                    </div>
                  <% end %>
                <% end %>
              </div>
            </div>

            <!-- Gantt Timeline -->
            <GanttTimeline.gantt_timeline run={@run} step_states={@step_states} />
          </div>
        </div>

        <!-- Input/Output -->
        <div class="mt-6">
          <!-- Header with context indicator -->
          <div class="flex items-center justify-between mb-3">
            <div class="flex items-center gap-2">
              <h2 class="text-sm font-semibold text-slate-900 dark:text-white">
                <%= if @selected_step do %>
                  Step: {@selected_step}
                <% else %>
                  Run Data
                <% end %>
              </h2>
              <span :if={@selected_step} class="text-xs bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 px-2 py-0.5 rounded">
                Step selected
              </span>
            </div>
            <button
              :if={@selected_step}
              phx-click="clear_selection"
              class="text-xs text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
            >
              Show run data
            </button>
          </div>

          <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
              <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700">
                <h2 class="text-sm font-semibold text-slate-900 dark:text-white">Input</h2>
              </div>
              <div class="p-4">
                <%= if @selected_step do %>
                  <.step_input_display step_slug={@selected_step} step_states={@step_states} run={@run} />
                <% else %>
                  <pre class="text-xs text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-900 rounded p-3 overflow-x-auto max-h-96"><%= format_json(@run.input) %></pre>
                <% end %>
              </div>
            </div>

            <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
              <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700">
                <h2 class="text-sm font-semibold text-slate-900 dark:text-white">Output</h2>
              </div>
              <div class="p-4">
                <%= if @selected_step && @step_tasks != [] do %>
                  <div class="space-y-3">
                    <%= for task <- @step_tasks do %>
                      <div class="border-l-2 border-slate-300 dark:border-slate-600 pl-3">
                        <p class="text-xs text-slate-500 dark:text-slate-400 mb-1">Task {task.task_index}</p>
                        <pre class="text-xs text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-900 rounded p-3 overflow-x-auto max-h-64"><%= format_json(task.output) %></pre>
                        <p :if={task.error_message} class="text-xs text-rose-500 mt-2">Error: {task.error_message}</p>
                      </div>
                    <% end %>
                  </div>
                <% else %>
                  <pre class="text-xs text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-900 rounded p-3 overflow-x-auto max-h-96"><%= format_json(@run.output) %></pre>
                <% end %>
              </div>
            </div>
          </div>
        </div>
      </div>
    </Layouts.dashboard_layout>
    """
  end

  # Component to show step input (based on dependencies)
  defp step_input_display(assigns) do
    # Find the step's dependencies
    step_state = Enum.find(assigns.step_states, fn s -> s.step_slug == assigns.step_slug end)
    deps = if step_state, do: step_state[:deps] || [], else: []

    assigns = assign(assigns, :deps, deps)

    ~H"""
    <div class="space-y-3">
      <%= if @deps == [] do %>
        <p class="text-xs text-slate-500 dark:text-slate-400 italic">
          This step has no dependencies - it receives the run input directly.
        </p>
        <pre class="text-xs text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-900 rounded p-3 overflow-x-auto max-h-64"><%= format_json(@run.input) %></pre>
      <% else %>
        <p class="text-xs text-slate-500 dark:text-slate-400 italic mb-2">
          Input comes from dependencies: {Enum.join(@deps, ", ")}
        </p>
        <p class="text-xs text-slate-400 dark:text-slate-500">
          (Click on a dependency step to see its output)
        </p>
      <% end %>
    </div>
    """
  end

  defp format_json(nil), do: "-"

  defp format_json(data) when is_map(data) or is_list(data) do
    Jason.encode!(data, pretty: true)
  rescue
    _ -> inspect(data)
  end

  defp format_json(data), do: inspect(data)
end
