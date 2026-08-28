defmodule PgFlowDashboard.Live.RunsLive.Show do
  @moduledoc """
  Run detail page with step states and dependency graph.
  """

  use Phoenix.LiveView

  alias PgFlow.{Definitions, Runs, RunSummary}
  alias PgFlow.Schema.StepTask

  alias PgFlowDashboard.Components.{
    DependencyGraph,
    GanttTimeline,
    JsonViewer,
    Layouts,
    ProgressBar,
    StatusBadge
  }

  alias PgFlowDashboard.Live.LiveHelpers

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
    {:ok, tasks} = Runs.list_step_tasks(socket.assigns.repo, socket.assigns.run_id, step_slug)

    socket =
      socket
      |> assign(:selected_step, step_slug)
      |> assign(:step_tasks, tasks)

    {:noreply, socket}
  end

  @impl true
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

  @impl true
  def handle_info({:pgflow, _run_id, {:task_started, _}}, socket) do
    {:noreply, load_step_states(socket)}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:task_completed, _}}, socket) do
    socket =
      socket
      |> load_run()
      |> load_step_states()

    {:noreply, socket}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:task_failed, _}}, socket) do
    socket =
      socket
      |> load_run()
      |> load_step_states()

    {:noreply, socket}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_completed, _}}, socket) do
    socket =
      socket
      |> load_run()
      |> load_step_states()

    {:noreply, socket}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_failed, _}}, socket) do
    socket =
      socket
      |> load_run()
      |> load_step_states()

    {:noreply, socket}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:step_skipped, _}}, socket) do
    {:noreply, load_step_states(socket)}
  end

  @impl true
  def handle_info(_, socket), do: {:noreply, socket}

  @impl true
  def handle_event("select_step", %{"step" => step_slug}, socket) do
    {:noreply, toggle_step(socket, step_slug)}
  end

  @impl true
  def handle_event("select_step_keydown", %{"key" => key, "step" => step_slug}, socket)
      when key in ["Enter", " "] do
    {:noreply, toggle_step(socket, step_slug)}
  end

  @impl true
  def handle_event("select_step_keydown", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("clear_selection", _, socket) do
    socket =
      socket
      |> assign(:selected_step, nil)
      |> assign(:step_tasks, [])

    {:noreply, socket}
  end

  @impl true
  def handle_event("next_step", _, socket) do
    {:noreply, navigate_step(socket, :next)}
  end

  @impl true
  def handle_event("prev_step", _, socket) do
    {:noreply, navigate_step(socket, :prev)}
  end

  @impl true
  def handle_event("handle_keydown", %{"key" => "j"}, socket) do
    {:noreply, navigate_step(socket, :next)}
  end

  @impl true
  def handle_event("handle_keydown", %{"key" => "k"}, socket) do
    {:noreply, navigate_step(socket, :prev)}
  end

  @impl true
  def handle_event("handle_keydown", %{"key" => "Escape"}, socket) do
    socket =
      socket
      |> assign(:selected_step, nil)
      |> assign(:step_tasks, [])

    {:noreply, socket}
  end

  @impl true
  def handle_event("handle_keydown", %{"key" => "]"}, socket) do
    {:noreply, navigate_to_adjacent_run(socket, :next)}
  end

  @impl true
  def handle_event("handle_keydown", %{"key" => "["}, socket) do
    {:noreply, navigate_to_adjacent_run(socket, :prev)}
  end

  @impl true
  def handle_event("handle_keydown", _, socket), do: {:noreply, socket}

  defp navigate_to_adjacent_run(socket, direction) do
    case Runs.adjacent(socket.assigns.repo, socket.assigns.run_id, direction) do
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
    {:ok, tasks} = Runs.list_step_tasks(socket.assigns.repo, socket.assigns.run_id, step_slug)

    socket
    |> assign(:selected_step, step_slug)
    |> assign(:step_tasks, tasks)
  end

  defp toggle_step(socket, step_slug) do
    if socket.assigns.selected_step == step_slug do
      socket
      |> assign(:selected_step, nil)
      |> assign(:step_tasks, [])
    else
      select_step(socket, step_slug)
    end
  end

  defp load_run(socket) do
    case Runs.get(socket.assigns.repo, socket.assigns.run_id) do
      {:ok, run} ->
        {:ok, states} = Runs.list_step_states(socket.assigns.repo, socket.assigns.run_id)
        assign(socket, :run, summarize_run(socket.assigns.repo, run, states))

      {:error, _} ->
        assign(socket, :run, nil)
    end
  end

  defp load_step_states(socket) do
    {:ok, states} = Runs.list_step_states(socket.assigns.repo, socket.assigns.run_id)
    {:ok, tasks} = Runs.list_run_tasks(socket.assigns.repo, socket.assigns.run_id)
    state_map = Map.new(states, fn s -> {s.step_slug, s.status} end)

    socket
    |> assign(:step_states, states)
    |> assign(:step_state_map, state_map)
    |> assign(:step_task_counts, step_task_counts_by_step(tasks))
  end

  defp step_task_counts_by_step(tasks) do
    tasks
    |> Enum.group_by(fn %StepTask{step_slug: step_slug} -> step_slug end)
    |> Map.new(fn {step_slug, step_tasks} ->
      {step_slug,
       %{
         total: length(step_tasks),
         completed: Enum.count(step_tasks, &match?(%StepTask{status: "completed"}, &1)),
         failed: Enum.count(step_tasks, &match?(%StepTask{status: "failed"}, &1))
       }}
    end)
  end

  defp load_flow_steps(socket) do
    if socket.assigns.run do
      {:ok, steps} = Definitions.list_steps(socket.assigns.repo, socket.assigns.run.flow_slug)
      {:ok, deps} = Definitions.list_deps(socket.assigns.repo, socket.assigns.run.flow_slug)

      assign(socket, :flow_steps, DependencyGraph.with_dependencies(steps, deps))
    else
      assign(socket, :flow_steps, [])
    end
  end

  defp summarize_run(repo, run, states) do
    total_steps = length(states)
    completed_steps = Enum.count(states, &(&1.status == "completed"))
    failed_steps = Enum.count(states, &(&1.status == "failed"))
    skipped_steps = Enum.count(states, &(&1.status == "skipped"))
    progress_steps = completed_steps + skipped_steps

    RunSummary.new(%{
      run_id: run.run_id,
      flow_slug: run.flow_slug,
      flow_type: definition_type(repo, run.flow_slug),
      status: run.status,
      input: run.input,
      output: run.output,
      started_at: run.started_at,
      completed_at: run.completed_at,
      duration_ms: duration_ms(run.started_at, run.completed_at || run.failed_at),
      total_steps: total_steps,
      completed_steps: completed_steps,
      failed_steps: failed_steps,
      skipped_steps: skipped_steps,
      progress_percent: progress_percent(progress_steps, total_steps)
    })
  end

  defp definition_type(repo, flow_slug) do
    case Definitions.get_job(repo, flow_slug) do
      {:ok, job} -> job.flow_type
      {:error, :not_found} -> "flow"
    end
  end

  defp duration_ms(nil, _finished_at), do: Decimal.new(0)

  defp duration_ms(started_at, nil),
    do: started_at |> DateTime.diff(DateTime.utc_now(), :millisecond) |> abs() |> Decimal.new()

  defp duration_ms(started_at, finished_at),
    do: finished_at |> DateTime.diff(started_at, :millisecond) |> max(0) |> Decimal.new()

  defp progress_percent(_finished_steps, 0), do: Decimal.new(0)

  defp progress_percent(finished_steps, total_steps) do
    finished_steps
    |> Decimal.new()
    |> Decimal.div(Decimal.new(total_steps))
    |> Decimal.mult(Decimal.new(100))
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
              skipped={@run.skipped_steps}
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

        <!-- Workflow (full width) -->
        <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 mb-6">
          <h2 class="text-sm font-semibold text-slate-900 dark:text-white mb-4">Workflow</h2>
          <p class="text-xs text-slate-500 dark:text-slate-400 mb-3">Select a node to view its output</p>
          <DependencyGraph.dependency_graph
            steps={@flow_steps}
            step_states={@step_state_map}
            highlighted_step={@selected_step}
            on_click="select_step"
            on_keydown="select_step_keydown"
          />
        </div>

        <!-- Step States + Gantt Timeline (side by side) -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <!-- Step States -->
          <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
            <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700">
              <h2 class="text-sm font-semibold text-slate-900 dark:text-white">Step States</h2>
              <p class="text-xs text-slate-500 dark:text-slate-400 mt-1">Select a step to view its output</p>
            </div>
            <div
              class="divide-y divide-slate-200 dark:divide-slate-700 max-h-64 overflow-y-auto"
              tabindex="0"
              aria-label="Step states"
            >
              <%= if @step_states == [] do %>
                <div class="px-4 py-8 text-center text-slate-500 dark:text-slate-400 text-sm">
                  No step states yet
                </div>
              <% else %>
                <%= for state <- @step_states do %>
                  <% task_counts = step_task_counts(@step_task_counts, state.step_slug) %>
                  <button
                    type="button"
                    id={"step-state-#{state.step_slug}"}
                    phx-click="select_step"
                    phx-value-step={state.step_slug}
                    aria-pressed={to_string(@selected_step == state.step_slug)}
                    class={[
                      "w-full px-4 py-3 text-left cursor-pointer transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-purple-500",
                      @selected_step == state.step_slug && "bg-purple-50 dark:bg-purple-900/20 border-l-2 border-l-purple-500 !border-b-transparent",
                      @selected_step != state.step_slug && "hover:bg-slate-50 dark:hover:bg-slate-700/50"
                    ]}
                  >
                    <div class="flex items-center justify-between">
                      <div class="flex items-center gap-3">
                        <StatusBadge.status_badge status={state.status} size={:sm} pulse={state.status == "started"} />
                        <span class="text-sm font-medium text-slate-900 dark:text-white">{state.step_slug}</span>
                      </div>
                      <span class="text-xs text-slate-950 dark:text-white">
                        {LiveHelpers.format_duration(step_state_duration(state))}
                      </span>
                    </div>
                    <div :if={Map.get(state, :skip_reason)} class="mt-1 text-xs text-slate-950 dark:text-white">
                      Skip reason: {format_skip_reason(state.skip_reason)}
                    </div>
                    <div :if={task_counts.total > 0} class="mt-2 text-xs text-slate-950 dark:text-white">
                      Tasks: {task_counts.completed}/{task_counts.total}
                      <span :if={task_counts.failed > 0} class="text-rose-900 dark:text-rose-100">({task_counts.failed} failed)</span>
                    </div>
                  </button>
                <% end %>
              <% end %>
            </div>
          </div>

          <!-- Gantt Timeline -->
          <GanttTimeline.gantt_timeline run={@run} step_states={@step_states} />
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
                  <.step_input_display
                    id={"step-input-#{@selected_step}"}
                    step_slug={@selected_step}
                    flow_steps={@flow_steps}
                    run={@run}
                  />
                <% else %>
                  <JsonViewer.json_viewer id="run-input-json" data={@run.input} />
                <% end %>
              </div>
            </div>

            <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
              <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700">
                <h2 class="text-sm font-semibold text-slate-900 dark:text-white">Output</h2>
              </div>
              <div class="p-4">
                <%= cond do %>
                  <% @selected_step && @step_tasks != [] -> %>
                  <div class="space-y-3">
                    <%= for task <- @step_tasks do %>
                      <div class="border-l-2 border-slate-300 dark:border-slate-600 pl-3">
                        <p class="text-xs text-slate-500 dark:text-slate-400 mb-1">Task {task.task_index}</p>
                        <JsonViewer.json_viewer
                          id={"step-output-#{@selected_step}-task-#{task.task_index}"}
                          data={task.output}
                        />
                        <p :if={task.error_message} class="text-xs text-rose-600 dark:text-rose-400 mt-2">
                          Error: {task.error_message}
                        </p>
                      </div>
                    <% end %>
                  </div>
                  <% @selected_step -> %>
                    <div class="rounded-md border border-dashed border-slate-300 bg-slate-50 px-4 py-8 text-center text-sm text-slate-500 dark:border-slate-700 dark:bg-slate-900/50 dark:text-slate-400">
                      No output was recorded for this step
                    </div>
                  <% true -> %>
                  <JsonViewer.json_viewer id="run-output-json" data={@run.output} />
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
  attr(:id, :string, required: true)
  attr(:step_slug, :string, required: true)
  attr(:flow_steps, :list, required: true)
  attr(:run, :map, required: true)

  defp step_input_display(assigns) do
    # Find the step's dependencies
    step = Enum.find(assigns.flow_steps, fn step -> step.step_slug == assigns.step_slug end)
    deps = if step, do: step.deps, else: []

    assigns = assign(assigns, :deps, deps)

    ~H"""
    <div class="space-y-3">
      <%= if @deps == [] do %>
        <p class="text-xs text-slate-500 dark:text-slate-400 italic">
          This step has no dependencies - it receives the run input directly.
        </p>
        <JsonViewer.json_viewer id={@id} data={@run.input} />
      <% else %>
        <p class="text-xs text-slate-950 dark:text-white italic mb-2">
          Input comes from dependencies: {Enum.join(@deps, ", ")}
        </p>
        <p class="text-xs text-slate-700 dark:text-slate-200">
          (Click on a dependency step to see its output)
        </p>
      <% end %>
    </div>
    """
  end

  defp format_skip_reason("condition_unmet"), do: "Condition not met"
  defp format_skip_reason("dependency_skipped"), do: "Dependency skipped"
  defp format_skip_reason("handler_failed"), do: "Handler failed"

  defp format_skip_reason(reason),
    do: reason |> to_string() |> String.replace("_", " ") |> String.capitalize()

  defp step_state_duration(state) do
    Map.get_lazy(state, :duration_ms, fn ->
      finished_at = state.completed_at || state.skipped_at || state.failed_at

      if state.started_at do
        duration_ms(state.started_at, finished_at)
      end
    end)
  end

  defp step_task_counts(task_counts, step_slug) do
    Map.get(task_counts, step_slug, %{total: 0, completed: 0, failed: 0})
  end
end
