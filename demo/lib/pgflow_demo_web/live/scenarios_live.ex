defmodule PgflowDemoWeb.ScenariosLive do
  @moduledoc """
  Feature catalogue and durable run inspection for demo scenarios.
  """

  use PgflowDemoWeb, :live_view

  alias PgFlow.LiveClient
  alias PgflowDemo.{ScenarioControls, ScenarioRunner, Scenarios}
  alias PgflowDemoWeb.Components.ScenarioSource

  @run_assign :scenario_run
  @max_tasks 100

  @impl true
  def mount(_params, _session, socket) do
    controls_enabled? = Application.get_env(:pgflow_demo, :scenario_controls_enabled, false)

    socket =
      socket
      |> LiveClient.init(pubsub: PgflowDemo.PubSub, as: @run_assign)
      |> assign(:scenarios, Scenarios.list())
      |> assign(:descriptor, nil)
      |> assign(:form, nil)
      |> assign(:form_errors, nil)
      |> assign(:busy?, false)
      |> assign(:controls_enabled?, controls_enabled?)
      |> assign(:controls_busy?, false)
      |> assign(:release_ready?, false)
      |> stream(:tasks, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    {:noreply, apply_live_action(socket, params)}
  end

  @impl true
  def handle_event("validate", _params, %{assigns: %{descriptor: nil}} = socket),
    do: {:noreply, socket}

  def handle_event("validate", %{"preset" => preset} = params, socket) do
    overrides = decode_overrides(Map.get(params, "input", ""))

    form =
      params
      |> Map.take(["preset", "input"])
      |> to_form()

    errors = validation_errors(socket.assigns.descriptor, preset, overrides)

    {:noreply,
     socket
     |> assign(:form, form)
     |> assign(:form_errors, errors)}
  end

  def handle_event("run", %{"preset" => preset} = params, socket) do
    descriptor = socket.assigns.descriptor
    overrides = decode_overrides(Map.get(params, "input", ""))

    with :executable <- scenario_kind(descriptor),
         :ok <- ensure_runnable(descriptor, socket.assigns.controls_enabled?),
         {:ok, overrides} <- overrides,
         :ok <- validate_or_error(descriptor, preset, overrides),
         {:ok, run_id} <- ScenarioRunner.start(descriptor.id, preset, overrides) do
      socket =
        socket
        |> assign(:busy?, true)
        |> assign(:form_errors, nil)
        |> subscribe_run(run_id)
        |> push_patch(to: scenario_path(descriptor.id, run_id))

      {:noreply, socket}
    else
      {:error, reason} ->
        {:noreply,
         socket
         |> assign(:form_errors, format_error(reason))
         |> assign(:busy?, false)}

      :walkthrough ->
        {:noreply, put_flash(socket, :error, "This scenario is a walkthrough only.")}
    end
  end

  def handle_event(event, _params, %{assigns: %{controls_enabled?: false}} = socket)
      when event in ["release_handler", "drain_worker", "restart_worker"] do
    {:noreply, put_flash(socket, :error, "Test-bed controls are disabled.")}
  end

  def handle_event(event, _params, %{assigns: %{controls_busy?: true}} = socket)
      when event in ["drain_worker", "restart_worker"] do
    {:noreply, put_flash(socket, :error, "Wait for the current worker drain to finish.")}
  end

  def handle_event("release_handler", _params, socket) do
    with run_id when is_binary(run_id) <- current_run_id(socket),
         true <- ScenarioControls.release_ready?(socket.assigns.descriptor.id, run_id),
         {:ok, :released} <-
           ScenarioControls.release_handler(socket.assigns.descriptor.id, run_id) do
      {:noreply,
       socket
       |> assign(:release_ready?, false)
       |> put_flash(:info, "Recovery handler released.")}
    else
      nil ->
        {:noreply, put_flash(socket, :error, "No run loaded")}

      false ->
        {:noreply,
         socket
         |> assign(:release_ready?, false)
         |> put_flash(:error, "Recovery handler is not ready for release yet.")}

      {:error, :not_found} ->
        {:noreply,
         socket
         |> assign(:release_ready?, false)
         |> put_flash(:error, "Recovery handler is not ready for release yet.")}

      {:error, reason} ->
        {:noreply,
         socket
         |> put_flash(:error, controls_error(reason))}
    end
  end

  def handle_event("drain_worker", _params, socket) do
    case current_run_id(socket) do
      nil ->
        {:noreply, put_flash(socket, :error, "No run loaded")}

      run_id ->
        scenario_id = socket.assigns.descriptor.id

        {:noreply,
         socket
         |> assign(:controls_busy?, true)
         |> start_async(:drain_worker, fn ->
           ScenarioControls.drain_worker(scenario_id, run_id)
         end)}
    end
  end

  def handle_event("restart_worker", _params, socket) do
    case current_run_id(socket) do
      nil ->
        {:noreply, put_flash(socket, :error, "No run loaded")}

      run_id ->
        case ScenarioControls.restart_worker(socket.assigns.descriptor.id, run_id) do
          {:ok, _pid} -> {:noreply, put_flash(socket, :info, "Worker restarted.")}
          {:error, reason} -> {:noreply, put_flash(socket, :error, controls_error(reason))}
        end
    end
  end

  @impl true
  def handle_async(:drain_worker, {:ok, :ok}, socket) do
    {:noreply, socket |> assign(:controls_busy?, false) |> put_flash(:info, "Worker drained.")}
  end

  def handle_async(:drain_worker, result, socket) do
    {:noreply,
     socket |> assign(:controls_busy?, false) |> put_flash(:error, controls_error(result))}
  end

  @impl true
  def handle_info({:pgflow, run_id, _event} = msg, socket) do
    if run_relevant?(socket, run_id) do
      socket =
        socket
        |> then(&LiveClient.handle_info(msg, &1))
        |> refresh_run_snapshot(run_id)

      {:noreply, socket}
    else
      {:noreply, socket}
    end
  end

  def handle_info({:recovery_handler_changed, run_id}, socket) do
    if current_run_id(socket) == run_id do
      {:noreply, refresh_release_readiness(socket, run_id)}
    else
      {:noreply, socket}
    end
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def render(%{live_action: :index} = assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <div id="scenario-catalogue" class="space-y-6">
        <header class="space-y-2">
          <p class="text-sm">
            <.link navigate={~p"/"} class="link link-primary">Flow demo</.link>
            <span aria-hidden="true"> · </span>
            <.link navigate={~p"/pgflow"} class="link link-primary">Dashboard</.link>
          </p>
          <h1 class="text-3xl font-bold">Scenario catalogue</h1>
          <p class="text-base-content/70">
            Runnable presets for supported PgFlow feature families. Each scenario persists run state
            in the database and can be reloaded from the URL.
          </p>
        </header>

        <ul class="divide-y divide-base-300 rounded-box border border-base-300">
          <%= for scenario <- @scenarios do %>
            <li class="flex flex-col gap-2 p-4 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <h2 class="font-semibold">{scenario.title}</h2>
                <p class="text-xs font-mono text-base-content/50">{scenario.id}</p>
                <p class="text-sm text-base-content/70">
                  {scenario_kind_label(scenario.kind)}
                  <%= if scenario.kind == :walkthrough do %>
                    — {scenario.walkthrough}
                  <% end %>
                </p>
              </div>
              <.link
                navigate={~p"/scenarios/#{scenario.id}"}
                class="btn btn-primary btn-sm w-full sm:w-auto"
                id={"scenario-link-#{scenario.id}"}
              >
                Open
              </.link>
            </li>
          <% end %>
        </ul>
      </div>
    </Layouts.app>
    """
  end

  def render(%{live_action: :show} = assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <div class="space-y-8">
        <header class="space-y-2">
          <p class="text-sm">
            <.link navigate={~p"/scenarios"} class="link link-primary">All scenarios</.link>
            <span aria-hidden="true"> · </span>
            <.link navigate={~p"/"} class="link link-primary">Flow demo</.link>
          </p>
          <h1 class="text-3xl font-bold">{@descriptor.title}</h1>
          <p class="text-base-content/70">{scenario_description(@descriptor)}</p>
        </header>

        <%= if @descriptor.kind == :executable do %>
          <section class="rounded-box border border-base-300 p-4 sm:p-6">
            <h2 class="mb-4 text-lg font-semibold">Run preset</h2>
            <.form
              for={@form}
              id="scenario-form"
              phx-change="validate"
              phx-submit="run"
              class="space-y-4"
            >
              <.input
                field={@form[:preset]}
                type="select"
                label="Preset"
                id="scenario-input"
                options={preset_options(@descriptor)}
                disabled={@busy?}
              />
              <.input
                field={@form[:input]}
                type="textarea"
                label="Input overrides (JSON)"
                id="scenario-input-overrides"
                rows="4"
                disabled={@busy?}
              />
              <div :if={@form_errors} id="scenario-form-errors" class="alert alert-error text-sm">
                {@form_errors}
              </div>
              <button
                type="submit"
                id="scenario-run-button"
                class="btn btn-primary w-full sm:w-auto"
                disabled={@busy? or (@descriptor.id == "recovery" and not @controls_enabled?)}
                aria-busy={@busy?}
              >
                <%= if @busy? do %>
                  Running…
                <% else %>
                  Run scenario
                <% end %>
              </button>
            </.form>
            <p :if={@descriptor.id == "recovery" and not @controls_enabled?} class="mt-3 text-sm">
              Recovery is a walkthrough here. Enable the local test-bed profile to run and release a handler.
            </p>
          </section>
        <% else %>
          <section class="rounded-box border border-base-300 p-4 sm:p-6">
            <p class="text-base-content/70">{@descriptor.walkthrough}</p>
            <.link navigate={~p"/"} class="btn btn-outline btn-sm mt-4">Open flow demo</.link>
          </section>
        <% end %>

        <section id="scenario-expected" class="rounded-box border border-base-300 p-4 sm:p-6">
          <h2 class="mb-2 text-lg font-semibold">Expected outcome</h2>
          <pre
            id="scenario-expected-body"
            class="overflow-x-auto rounded-lg bg-base-200 p-3 text-xs font-mono"
          ><%= format_json(selected_expected(@descriptor, @form)) %></pre>
        </section>

        <ScenarioSource.scenario_source
          module={@descriptor.module}
          walkthrough={if(@descriptor.kind == :walkthrough, do: @descriptor.walkthrough)}
        />

        <section
          :if={@scenario_run}
          id="scenario-run"
          data-run-id={@scenario_run.run_id}
          class="rounded-box border border-base-300 p-4 sm:p-6 space-y-4"
        >
          <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 class="text-lg font-semibold">Run state</h2>
              <p class="font-mono text-xs text-base-content/60">{@scenario_run.run_id}</p>
            </div>
            <span class={"badge badge-lg #{status_badge_class(@scenario_run.status)}"}>
              {@scenario_run.status}
            </span>
          </div>

          <p :if={terminal_disclaimer?(@descriptor)} class="text-xs text-base-content/60">
            A cancelled or skipped SQL task does not guarantee the handler process had no remaining
            external effects.
          </p>

          <div id="scenario-step-states" class="space-y-2">
            <div
              :for={step <- @scenario_run.step_states || []}
              id={"scenario-step-#{step.step_slug}"}
              class="rounded-lg border border-base-300 p-3 text-sm"
            >
              <div class="flex flex-wrap items-center gap-2">
                <span class="font-mono font-semibold">{step.step_slug}</span>
                <span class={"badge badge-sm #{status_badge_class(step.status)}"}>{step.status}</span>
              </div>
            </div>
          </div>

          <div id="scenario-tasks" phx-update="stream" class="space-y-2">
            <div id="scenario-tasks-empty" class="hidden only:block text-sm text-base-content/60">
              No tasks yet
            </div>
            <div
              :for={{id, task} <- @streams.tasks}
              id={id}
              class="rounded-lg border border-base-300 p-3 text-sm"
            >
              <div class="flex flex-wrap items-center gap-2">
                <span class="font-mono font-semibold">{task.step_slug}</span>
                <span class={"badge badge-sm #{status_badge_class(task.status)}"}>{task.status}</span>
                <span :if={task.attempts_count} class="text-xs text-base-content/60">
                  attempts: {task.attempts_count}
                </span>
                <span :if={task.queue_name} class="text-xs font-mono text-base-content/60">
                  queue: {task.queue_name}
                </span>
              </div>
              <pre
                :if={task.output}
                class="mt-2 overflow-x-auto rounded bg-base-200 p-2 text-xs font-mono"
              ><%= format_json(
                task.output
              ) %></pre>
            </div>
          </div>

          <p class="text-xs font-mono text-base-content/60">
            flow: {@descriptor.flow_slug}
          </p>

          <pre
            :if={@scenario_run.output}
            id="scenario-run-output"
            class="overflow-x-auto rounded-lg bg-base-200 p-3 text-xs font-mono"
          ><%= format_json(@scenario_run.output) %></pre>

          <.link
            id="scenario-dashboard-link"
            navigate={~p"/pgflow/runs/#{@scenario_run.run_id}"}
            class="btn btn-outline btn-sm w-full sm:w-auto"
          >
            Open in dashboard
          </.link>
        </section>

        <section
          :if={@controls_enabled? and @descriptor.id == "recovery"}
          id="scenario-controls"
          class="mt-8 rounded-box border-2 border-warning/40 bg-warning/5 p-4 sm:p-6 space-y-4"
        >
          <h2 class="text-lg font-semibold text-warning">Test-bed controls</h2>
          <p class="text-sm text-base-content/70">
            Operational recovery actions for demo-owned runs only. Disabled in default runtime config.
          </p>
          <div class="flex flex-col gap-3 sm:flex-row">
            <button
              type="button"
              id="scenario-release-handler"
              phx-click="release_handler"
              class="btn btn-warning btn-sm w-full sm:w-auto"
              disabled={not @release_ready?}
              aria-label="Release blocked recovery handler"
            >
              Release handler
            </button>
            <button
              type="button"
              id="scenario-drain-worker"
              phx-click="drain_worker"
              class="btn btn-outline btn-warning btn-sm w-full sm:w-auto"
              disabled={@controls_busy? or is_nil(@scenario_run)}
              aria-label="Drain recovery worker"
            >
              Drain worker
            </button>
            <button
              type="button"
              id="scenario-restart-worker"
              phx-click="restart_worker"
              class="btn btn-outline btn-sm"
              disabled={@controls_busy? or is_nil(@scenario_run)}
            >
              Restart worker
            </button>
          </div>
        </section>
      </div>
    </Layouts.app>
    """
  end

  defp apply_live_action(socket, %{"scenario_id" => scenario_id} = params) do
    case Scenarios.fetch(scenario_id) do
      {:ok, descriptor} ->
        prior_scenario_id = socket.assigns[:descriptor] && socket.assigns.descriptor.id
        run_id = Map.get(params, "run")

        socket =
          socket
          |> assign(:live_action, :show)
          |> assign(:descriptor, descriptor)
          |> assign(:form, scenario_form(descriptor))
          |> assign(:form_errors, nil)

        if prior_scenario_id && prior_scenario_id != scenario_id do
          socket
          |> reset_scenario_run()
          |> restore_run(run_id, descriptor)
        else
          restore_run(socket, run_id, descriptor)
        end

      {:error, :unknown_scenario} ->
        socket
        |> put_flash(:error, "Unknown scenario")
        |> push_navigate(to: ~p"/scenarios")
    end
  end

  defp apply_live_action(socket, _params) do
    socket =
      if socket.assigns[:live_action] == :show do
        reset_scenario_run(socket)
      else
        socket
      end

    socket
    |> assign(:live_action, :index)
    |> assign(:descriptor, nil)
  end

  defp restore_run(socket, run_id, descriptor) when is_binary(run_id) do
    if ScenarioRunner.demo_run?(descriptor.id, run_id) do
      socket =
        if current_run_id(socket) == run_id do
          refresh_run_snapshot(socket, run_id)
        else
          socket
          |> reset_scenario_run()
          |> subscribe_run(run_id)
          |> assign(:busy?, false)
        end

      restore_run_form(socket)
    else
      socket
      |> put_flash(:error, "Run does not belong to this scenario")
      |> push_patch(to: scenario_path(descriptor.id))
    end
  end

  defp restore_run(socket, _run_id, _descriptor), do: socket

  defp restore_run_form(
         %{assigns: %{scenario_run: %{input: %{"_scenario" => %{"preset" => preset}}}}} = socket
       ) do
    assign(socket, :form, to_form(Map.put(socket.assigns.form.params, "preset", preset)))
  end

  defp restore_run_form(socket), do: socket

  defp subscribe_run(socket, run_id) do
    if connected?(socket) do
      if previous_run_id = current_run_id(socket) do
        Phoenix.PubSub.unsubscribe(PgflowDemo.PubSub, recovery_topic(previous_run_id))
      end

      if socket.assigns.controls_enabled? and socket.assigns.descriptor.id == "recovery" do
        Phoenix.PubSub.subscribe(PgflowDemo.PubSub, recovery_topic(run_id))
      end
    end

    socket
    |> LiveClient.subscribe(run_id, as: @run_assign)
    |> refresh_run_snapshot(run_id)
  end

  defp refresh_run_snapshot(socket, run_id) do
    with {:ok, loaded} <- ScenarioRunner.load(run_id),
         {:ok, run} <- PgFlow.Client.get_run_with_states(run_id) do
      tasks =
        loaded.tasks
        |> Enum.take(@max_tasks)
        |> Enum.map(&task_for_stream/1)

      socket
      |> assign(@run_assign, run)
      |> assign(:busy?, run.status in ["started", "created"])
      |> refresh_release_readiness(run_id)
      |> stream(:tasks, tasks, reset: true)
    else
      _ -> socket
    end
  end

  defp refresh_release_readiness(socket, run_id) do
    ready? =
      socket.assigns.controls_enabled? and socket.assigns.descriptor.id == "recovery" and
        ScenarioControls.release_ready?(socket.assigns.descriptor.id, run_id)

    assign(socket, :release_ready?, ready?)
  end

  defp recovery_topic(run_id), do: "scenario:recovery:#{run_id}"

  defp task_for_stream(task) do
    %{
      id: "task-#{task.step_slug}-#{task.task_index}",
      step_slug: task.step_slug,
      status: task.status,
      attempts_count: task.attempts_count,
      queue_name: task.queue_name,
      output: task.output
    }
  end

  defp reset_scenario_run(socket) do
    run_id = current_run_id(socket)

    socket =
      if run_id do
        if connected?(socket) do
          Phoenix.PubSub.unsubscribe(PgflowDemo.PubSub, recovery_topic(run_id))
        end

        LiveClient.unsubscribe(socket, as: @run_assign)
      else
        socket
      end

    socket
    |> assign(:busy?, false)
    |> assign(:release_ready?, false)
    |> stream(:tasks, [], reset: true)
  end

  defp run_relevant?(socket, run_id) do
    descriptor = socket.assigns.descriptor

    descriptor &&
      ScenarioRunner.demo_run?(descriptor.id, run_id) &&
      current_run_id(socket) == run_id
  end

  defp current_run_id(%{assigns: assigns}) do
    case Map.get(assigns, @run_assign) do
      %{run_id: run_id} -> run_id
      _ -> nil
    end
  end

  defp current_run_id(_), do: nil

  defp scenario_form(%{presets: presets}) when map_size(presets) == 0,
    do: to_form(%{"preset" => nil, "input" => "{}"})

  defp scenario_form(%{presets: presets}) do
    {preset_key, _preset} = Enum.at(presets, 0)

    %{"preset" => preset_key, "input" => "{}"}
    |> to_form()
  end

  defp preset_options(%{presets: presets}) do
    Enum.map(presets, fn {key, preset} -> {preset.title, key} end)
  end

  defp selected_expected(%{presets: presets}, %{params: %{"preset" => preset_key}}) do
    Map.get(presets, preset_key, %{expected: %{}}).expected
  end

  defp selected_expected(%{presets: presets}, _form) do
    presets |> Map.values() |> List.first() |> Map.get(:expected, %{})
  end

  defp ensure_runnable(%{id: "recovery"}, false), do: {:error, :controls_disabled}
  defp ensure_runnable(_descriptor, _enabled), do: :ok

  defp decode_overrides(""), do: {:ok, %{}}

  defp decode_overrides(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, map} when is_map(map) -> {:ok, map}
      _ -> {:error, :invalid_json}
    end
  end

  defp decode_overrides(_), do: {:error, :invalid_json}

  defp validation_errors(_descriptor, _preset, {:error, :invalid_json}),
    do: "Input overrides must be valid JSON"

  defp validation_errors(descriptor, preset, {:ok, overrides}) do
    case validate_or_error(descriptor, preset, overrides) do
      :ok -> nil
      {:error, reason} -> format_error(reason)
    end
  end

  defp validate_or_error(descriptor, preset, overrides) do
    case Scenarios.validate_preset(descriptor.id, preset, overrides) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp format_error(:invalid_preset_bounds), do: "Preset input is out of allowed bounds"
  defp format_error(:unknown_preset), do: "Unknown preset"
  defp format_error(:walkthrough_only), do: "Walkthrough scenarios cannot be executed here"
  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error(reason), do: inspect(reason)

  defp controls_error(:controls_disabled), do: "Test-bed controls are disabled"
  defp controls_error(:foreign_run), do: "Run does not belong to this scenario"
  defp controls_error(:not_found), do: "Run or handler not found"
  defp controls_error(reason), do: inspect(reason)

  defp scenario_kind(%{kind: :executable}), do: :executable
  defp scenario_kind(_), do: :walkthrough

  defp scenario_kind_label(:executable), do: "Executable preset"
  defp scenario_kind_label(:walkthrough), do: "Walkthrough"

  defp scenario_description(%{kind: :walkthrough, walkthrough: text}), do: text

  defp scenario_description(%{features: features}) do
    "Demonstrates #{Enum.join(features, ", ")} with persisted run and task inspection."
  end

  defp scenario_path(scenario_id, run_id \\ nil) do
    if run_id do
      ~p"/scenarios/#{scenario_id}?#{%{run: run_id}}"
    else
      ~p"/scenarios/#{scenario_id}"
    end
  end

  defp status_badge_class("completed"), do: "badge-success"
  defp status_badge_class("failed"), do: "badge-error"
  defp status_badge_class("skipped"), do: "badge-ghost"
  defp status_badge_class("cancelled"), do: "badge-ghost"
  defp status_badge_class(_), do: "badge-info"

  defp terminal_disclaimer?(%{id: id}) when id in ["recovery", "timeout"], do: true
  defp terminal_disclaimer?(_), do: false

  defp format_json(value) do
    Jason.encode!(value, pretty: true)
  rescue
    _ -> inspect(value, pretty: true, limit: :infinity, printable_limit: :infinity)
  end
end
