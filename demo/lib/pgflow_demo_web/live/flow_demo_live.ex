defmodule PgflowDemoWeb.FlowDemoLive do
  @moduledoc """
  LiveView for visualizing PgFlow workflow execution in real-time.
  """

  use PgflowDemoWeb, :live_view

  require Logger

  alias PgFlow.Client
  alias PgflowDemoWeb.Components.{CronDSL, FlowDSL, PoweredBy}

  # UI Constants
  @node_radius 10
  @timer_interval_ms 100
  @max_log_entries 50

  @default_url "https://www.pgflow.dev/news/pgflow-0-13-1-cli-fix-step-output-storage-for-conditional-execution/"

  @flow_modules %{
    article: PgflowDemo.Flows.ArticleFlow,
    onboarding: PgflowDemo.Flows.OnboardingFlow
  }

  @flows %{
    article: %{
      slug: :article_flow,
      steps: [
        %{slug: :fetch_article, label: "Fetch", x: 100, y: 15},
        %{slug: :convert_to_markdown, label: "Markdown", x: 100, y: 80},
        %{slug: :summarize, label: "Summarize", x: 50, y: 125},
        %{slug: :extract_keywords, label: "Keywords", x: 150, y: 125},
        %{slug: :publish, label: "Publish", x: 100, y: 170}
      ],
      edges: [
        {:fetch_article, :convert_to_markdown},
        {:convert_to_markdown, :summarize},
        {:convert_to_markdown, :extract_keywords},
        {:summarize, :publish},
        {:extract_keywords, :publish}
      ]
    },
    onboarding: %{
      slug: :onboarding_flow,
      steps: [
        %{slug: :create_account, label: "Account", x: 100, y: 20},
        %{slug: :setup_premium, label: "Premium", x: 40, y: 90},
        %{slug: :activate_perk, label: "Perk", x: 40, y: 160},
        %{slug: :send_welcome, label: "Email", x: 160, y: 90},
        %{slug: :finish, label: "Finish", x: 100, y: 230}
      ],
      edges: [
        {:create_account, :setup_premium},
        {:setup_premium, :activate_perk},
        {:create_account, :send_welcome},
        {:create_account, :finish}
      ]
    }
  }

  @impl true
  def mount(_params, _session, socket) do
    article = flow_config(:article)

    socket =
      socket
      |> assign(:selected_flow, :article)
      |> assign(:plan, "free")
      |> assign(:fail_email, false)
      |> assign(:url, @default_url)
      |> assign(:run_id, nil)
      |> assign(:run_status, :idle)
      |> assign(:steps, initial_steps(article.steps))
      |> assign(:step_outputs, %{})
      |> assign(:error, nil)
      |> assign(:error_step, nil)
      |> assign(:duration, nil)
      |> assign(:start_time, nil)
      |> assign(:elapsed_ms, 0)
      |> assign(:event_log, [])
      |> assign(:steps_config, article.steps)
      |> assign(:edges, article.edges)
      |> assign(:active_edges, MapSet.new())
      |> assign(:highlighted_step, nil)
      |> assign(:node_radius, @node_radius)
      |> assign(:timer_ref, nil)
      |> assign(:output_step, nil)
      |> assign(:output_content, nil)
      |> assign(:output_loading, false)
      |> assign(:dsl_segments, FlowDSL.get_segments(flow_module(:article)))
      |> assign(:show_migration, false)
      |> assign(:migration_path, get_migration_path(:article))
      |> assign(:migration_content, get_migration_content(:article))
      |> assign(:cron_highlighted_source, CronDSL.get_highlighted_source())
      |> assign(:cron_next_run_info, CronDSL.get_next_run_info())

    {:ok, socket}
  end

  @impl true
  def terminate(_reason, socket) do
    cleanup_subscription(socket)
    cancel_timer(socket.assigns.timer_ref)
    :ok
  end

  @impl true
  def handle_event("update_url", %{"url" => url}, socket) do
    {:noreply, assign(socket, :url, url)}
  end

  @impl true
  def handle_event("update_onboarding", params, socket) do
    {:noreply,
     socket
     |> assign(:plan, params["plan"] || socket.assigns.plan)
     |> assign(:fail_email, params["fail_email"] == "true")}
  end

  @impl true
  def handle_event("select_flow", %{"flow" => flow}, socket) do
    case parse_flow_key(flow) do
      nil ->
        {:noreply, socket}

      selected ->
        {:noreply, switch_flow(socket, selected)}
    end
  end

  @impl true
  def handle_event("start_flow", params, %{assigns: %{selected_flow: :onboarding}} = socket) do
    plan = params["plan"] || socket.assigns.plan
    fail_email = params["fail_email"] == "true"

    socket = assign(socket, plan: plan, fail_email: fail_email)

    start_selected_flow(socket, :onboarding_flow, %{
      "plan" => plan,
      "fail_email" => fail_email
    })
  end

  @impl true
  def handle_event("start_flow", %{"url" => url}, socket) do
    case validate_url(url) do
      {:error, message} ->
        {:noreply, assign(socket, :error, message)}

      {:ok, valid_url} ->
        start_selected_flow(socket, :article_flow, %{"url" => valid_url})
    end
  end

  @impl true
  def handle_event("reset", _params, socket) do
    {:noreply, reset_run_state(socket)}
  end

  @impl true
  def handle_event("toggle_migration", _params, socket) do
    {:noreply, assign(socket, :show_migration, !socket.assigns.show_migration)}
  end

  @impl true
  def handle_event("highlight_step", %{"step" => step_slug}, socket) do
    # Event log entry click: highlight step and scroll within DSL pane
    case validate_step_slug(step_slug, socket.assigns.steps_config) do
      {:ok, step_atom} ->
        socket =
          socket
          |> assign(:highlighted_step, step_atom)
          |> push_event("scroll_dsl_pane", %{step: to_string(step_atom)})

        {:noreply, socket}

      {:error, _} ->
        {:noreply, socket}
    end
  end

  @impl true
  def handle_event("clear_highlight", _params, socket) do
    {:noreply, assign(socket, :highlighted_step, nil)}
  end

  @impl true
  def handle_event("click_node", %{"step" => step_slug}, socket) do
    # Clicking a graph node: scroll within DSL pane only (not webpage)
    handle_step_click(socket, step_slug, "scroll_dsl_pane")
  end

  @impl true
  def handle_event("click_dsl_step", %{"step" => step_slug}, socket) do
    # Clicking DSL code: scroll to center the step within the DSL pane
    handle_step_click(socket, step_slug, "scroll_dsl_pane")
  end

  @impl true
  def handle_event("view_step_output", %{"step" => step_slug}, socket) do
    # Event log click on completed step: show output and highlight
    case validate_step_slug(step_slug, socket.assigns.steps_config) do
      {:ok, nil} ->
        {:noreply, socket}

      {:ok, step_atom} ->
        step_status = Map.get(socket.assigns.steps, step_atom)

        # Only fetch output for completed steps
        if step_status == :completed and socket.assigns.run_id do
          socket =
            socket
            |> assign(:output_step, step_atom)
            |> assign(:output_loading, true)
            |> assign(:highlighted_step, step_atom)
            |> push_event("scroll_dsl_pane", %{step: to_string(step_atom)})

          # Fetch output asynchronously
          send(self(), {:fetch_step_output, step_atom})
          {:noreply, socket}
        else
          {:noreply, socket}
        end

      {:error, _} ->
        {:noreply, socket}
    end
  end

  # Shared handler for step clicks from graph nodes and DSL code
  defp handle_step_click(socket, step_slug, scroll_event) do
    case validate_step_slug(step_slug, socket.assigns.steps_config) do
      {:ok, nil} ->
        {:noreply, socket}

      {:ok, step_atom} ->
        step_status = Map.get(socket.assigns.steps, step_atom)

        socket =
          assign_step_click(
            socket,
            step_atom,
            step_status == :completed and socket.assigns.run_id,
            scroll_event
          )

        {:noreply, socket}

      {:error, _} ->
        {:noreply, socket}
    end
  end

  defp maybe_push_scroll_event(socket, nil, _step_atom), do: socket

  defp maybe_push_scroll_event(socket, event, step_atom) do
    push_event(socket, event, %{step: to_string(step_atom)})
  end

  defp maybe_start_tick_timer(socket) do
    if connected?(socket) do
      {:ok, ref} = :timer.send_interval(@timer_interval_ms, self(), :tick)
      ref
    end
  end

  defp assign_step_click(socket, step_atom, true, scroll_event) do
    send(self(), {:fetch_step_output, step_atom})

    socket
    |> assign(:output_step, step_atom)
    |> assign(:output_loading, true)
    |> assign(:highlighted_step, step_atom)
    |> maybe_push_scroll_event(scroll_event, step_atom)
  end

  defp assign_step_click(socket, step_atom, _completed?, scroll_event) do
    socket
    |> assign(:highlighted_step, step_atom)
    |> maybe_push_scroll_event(scroll_event, step_atom)
  end

  @impl true
  def handle_info({:fetch_step_output, step_atom}, socket) do
    output =
      fetch_step_output(socket.assigns.run_id, to_string(step_atom), socket.assigns.steps_config)

    socket =
      socket
      |> assign(:output_content, output)
      |> assign(:output_loading, false)

    {:noreply, socket}
  end

  @impl true
  def handle_info(:tick, %{assigns: %{run_status: :running, start_time: start_time}} = socket)
      when not is_nil(start_time) do
    elapsed = System.monotonic_time(:millisecond) - start_time
    {:noreply, assign(socket, :elapsed_ms, elapsed)}
  end

  def handle_info(:tick, socket), do: {:noreply, socket}

  # PgFlow PubSub events — new namespaced tuple format from Telemetry.PubSub bridge

  @impl true
  def handle_info(
        {:pgflow, _run_id, {:task_started, %{step_slug: step_slug, task_index: task_index}}},
        socket
      ) do
    case to_step_atom(step_slug, socket.assigns.steps_config) do
      nil ->
        {:noreply, socket}

      step_atom ->
        steps = update_step_status(socket.assigns.steps, step_atom, :running)
        active_edges = get_incoming_edges(step_atom, socket.assigns.edges)

        socket =
          socket
          |> assign(:steps, steps)
          |> assign(:active_edges, active_edges)
          |> assign(:highlighted_step, step_atom)
          |> push_event("scroll_dsl_pane", %{step: to_string(step_atom)})
          |> add_log(
            :processing,
            "Started",
            "#{format_step_label(step_atom)} [task #{task_index}]",
            step_atom
          )

        {:noreply, socket}
    end
  end

  @impl true
  def handle_info(
        {:pgflow, _run_id,
         {:task_completed, %{step_slug: step_slug, duration_ms: duration_ms, output: output}}},
        socket
      ) do
    case to_step_atom(step_slug, socket.assigns.steps_config) do
      nil ->
        {:noreply, socket}

      step_atom ->
        steps = update_step_status(socket.assigns.steps, step_atom, :completed)

        # Track that this step has output available (for UI indicators)
        step_outputs = Map.put(socket.assigns.step_outputs, step_atom, output != nil)

        socket =
          socket
          |> assign(:steps, steps)
          |> assign(:step_outputs, step_outputs)
          |> assign(:active_edges, MapSet.new())
          |> assign(:output_step, step_atom)
          |> assign(:output_content, output)
          |> assign(:output_loading, false)
          |> assign(:highlighted_step, step_atom)
          |> push_event("scroll_dsl_pane", %{step: to_string(step_atom)})
          |> add_log(
            :success,
            "Completed",
            "#{format_step_label(step_atom)} in #{duration_ms}ms",
            step_atom
          )

        {:noreply, socket}
    end
  end

  @impl true
  def handle_info(
        {:pgflow, _run_id,
         {:task_failed, %{step_slug: step_slug, error: error, duration_ms: duration_ms}}},
        socket
      ) do
    case to_step_atom(step_slug, socket.assigns.steps_config) do
      nil ->
        {:noreply, socket}

      step_atom ->
        steps = update_step_status(socket.assigns.steps, step_atom, :failed)

        socket =
          socket
          |> assign(:steps, steps)
          |> assign(:active_edges, MapSet.new())
          |> assign(:error, "Step #{step_slug} failed: #{inspect(error)}")
          |> assign(:error_step, step_atom)
          |> add_log(
            :error,
            "Failed",
            "#{format_step_label(step_atom)} after #{duration_ms}ms",
            step_atom
          )

        {:noreply, socket}
    end
  end

  @impl true
  def handle_info(
        {:pgflow, _run_id, {:step_skipped, %{step_slug: step_slug, skip_reason: reason}}},
        socket
      ) do
    case to_step_atom(step_slug, socket.assigns.steps_config) do
      nil ->
        {:noreply, socket}

      step_atom ->
        steps = update_step_status(socket.assigns.steps, step_atom, :skipped)

        {:noreply,
         socket
         |> assign(:steps, steps)
         |> clear_error_banner_for_step(step_atom)
         |> add_log(:info, "Skipped", "#{format_step_label(step_atom)} (#{reason})", step_atom)}
    end
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_started, _payload}}, socket) do
    {:noreply, assign(socket, :run_status, :running)}
  end

  # A run that reconcile_run_state/2 already resolved to a terminal status can
  # still have its run_completed/run_failed PubSub message sitting in the
  # mailbox (delivered between subscribe and the reconcile read). Without
  # these guards that late message would log "Flow Complete"/"Flow Failed" a
  # second time and re-run the terminal bookkeeping.
  @impl true
  def handle_info(
        {:pgflow, _run_id, {:run_completed, _payload}},
        %{assigns: %{run_status: status}} = socket
      )
      when status in [:completed, :failed] do
    {:noreply, socket}
  end

  @impl true
  def handle_info(
        {:pgflow, _run_id, {:run_failed, _payload}},
        %{assigns: %{run_status: status}} = socket
      )
      when status in [:completed, :failed] do
    {:noreply, socket}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_completed, _payload}}, socket) do
    cancel_timer(socket.assigns.timer_ref)

    elapsed_ms =
      if socket.assigns.start_time,
        do: System.monotonic_time(:millisecond) - socket.assigns.start_time,
        else: 0

    socket =
      socket
      |> cleanup_subscription()
      |> assign(:run_status, :completed)
      |> assign(:duration, elapsed_ms)
      |> assign(:active_edges, MapSet.new())
      |> assign(:timer_ref, nil)
      |> assign(:error, nil)
      |> assign(:error_step, nil)
      |> add_log(:success, "Flow Complete", "Total: #{elapsed_ms}ms")

    {:noreply, socket}
  end

  @impl true
  def handle_info({:pgflow, _run_id, {:run_failed, %{error: error}}}, socket) do
    cancel_timer(socket.assigns.timer_ref)

    elapsed_ms =
      if socket.assigns.start_time,
        do: System.monotonic_time(:millisecond) - socket.assigns.start_time,
        else: 0

    socket =
      socket
      |> cleanup_subscription()
      |> assign(:run_status, :failed)
      |> assign(:duration, elapsed_ms)
      |> assign(:error, "Flow failed: #{error}")
      |> assign(:error_step, nil)
      |> assign(:active_edges, MapSet.new())
      |> assign(:timer_ref, nil)
      |> add_log(:error, "Flow Failed", error)

    {:noreply, socket}
  end

  @impl true
  def handle_info(_msg, socket), do: {:noreply, socket}

  # Helpers

  defp flow_config(key), do: Map.fetch!(@flows, key)
  defp flow_module(key), do: Map.fetch!(@flow_modules, key)

  # Clears the global error banner when a DB-confirmed `step_skipped` arrives
  # for the same step that set it. A worker's task-failure event can race
  # ahead of a `when_exhausted: :skip` decision and raise the banner before
  # the DB decides to skip; once the skip is confirmed, the failure banner
  # is stale and must be cleared so a fail-soft run doesn't display as an
  # error.
  defp clear_error_banner_for_step(socket, step_atom) do
    if socket.assigns.error_step == step_atom do
      socket
      |> assign(:error, nil)
      |> assign(:error_step, nil)
    else
      socket
    end
  end

  defp parse_flow_key("article"), do: :article
  defp parse_flow_key("onboarding"), do: :onboarding
  defp parse_flow_key(_), do: nil

  defp start_selected_flow(socket, flow_slug, input) do
    case Client.start_flow(flow_slug, input) do
      {:ok, run_id} ->
        socket = cleanup_subscription(socket)

        # `Client.start_flow/2` can synchronously emit `step:skipped` and
        # `run:completed`/`run:failed` telemetry for a root-only skip that
        # resolves before any worker gets involved — that broadcast happens
        # *inside* start_flow, before this function ever sees a run_id.
        # Subscribing here (immediately once the run_id — and therefore the
        # topic name — exists) is as early as this LiveView can possibly
        # subscribe, but it can still be too late for that synchronous
        # broadcast, which already went out to zero subscribers and is gone
        # for good. `reconcile_run_state/2` below reads the run's current
        # DB state right after subscribing so any event that fired (and was
        # missed) before the subscription existed still lands in the UI —
        # this is the same subscribe-then-load-snapshot order used by
        # `PgFlow.LiveClient.subscribe_and_load/3`.
        Phoenix.PubSub.subscribe(PgflowDemo.PubSub, "pgflow:run:#{run_id}")

        cancel_timer(socket.assigns.timer_ref)

        socket =
          socket
          |> assign(:run_id, run_id)
          |> assign(:run_status, :running)
          |> assign(:error, nil)
          |> assign(:error_step, nil)
          |> assign(:steps, initial_steps(socket.assigns.steps_config))
          |> assign(:step_outputs, %{})
          |> assign(:duration, nil)
          |> assign(:start_time, System.monotonic_time(:millisecond))
          |> assign(:elapsed_ms, 0)
          |> assign(:event_log, [
            log_entry(:info, "Flow started", "Run ID: #{short_id(run_id)}")
          ])
          |> assign(:active_edges, MapSet.new())
          |> assign(:timer_ref, nil)
          |> assign(:output_step, nil)
          |> assign(:output_content, nil)
          |> assign(:output_loading, false)
          |> reconcile_run_state(run_id)

        # Only tick the elapsed-time clock if the run is (still) actually
        # running — reconcile_run_state/2 may have already resolved it to
        # :completed/:failed above.
        timer_ref =
          if socket.assigns.run_status == :running, do: maybe_start_tick_timer(socket)

        {:noreply, assign(socket, :timer_ref, timer_ref)}

      {:error, reason} ->
        {:noreply, assign(socket, :error, format_user_error(reason))}
    end
  end

  # Reads the run's current state from the DB and merges it into the socket.
  # Called right after subscribing in start_selected_flow/3 so a run that
  # already finished (or partially progressed) before the subscription
  # existed is still reflected in the UI instead of leaving it stuck on
  # "running". Exposed (not `defp`) so it can be exercised directly in
  # tests without needing to reproduce the exact synchronous-broadcast race
  # through the full LiveView start_flow event.
  @doc false
  def reconcile_run_state(socket, run_id) do
    case Client.get_run_with_states(run_id) do
      {:ok, run} ->
        apply_run_snapshot(socket, run)

      {:error, reason} ->
        # Deliberately non-fatal — live PubSub events still drive the UI, so
        # a failed reconcile read only risks missing events from before the
        # subscription existed. But say so, or a stuck-on-running UI caused
        # by this read failing is undebuggable.
        Logger.warning(
          "FlowDemoLive: failed to reconcile run #{run_id} after subscribe: #{inspect(reason)}"
        )

        socket
    end
  end

  defp apply_run_snapshot(socket, run) do
    steps_config = socket.assigns.steps_config

    socket =
      socket
      |> assign(:steps, merge_step_statuses(socket.assigns.steps, run.step_states, steps_config))
      |> assign(
        :step_outputs,
        merge_step_outputs(socket.assigns.step_outputs, run.step_states, steps_config)
      )

    case run.status do
      "completed" ->
        duration = elapsed_ms(run)

        # Unsubscribe on a reconciled terminal state, mirroring the live
        # run_completed/run_failed handlers — the run is over, so the
        # subscription has nothing left to deliver. (A terminal message
        # already in the mailbox is dropped by the guards on those handlers.)
        socket
        |> cleanup_subscription()
        |> assign(:run_status, :completed)
        |> assign(:duration, duration)
        |> assign(:active_edges, MapSet.new())
        |> assign(:error, nil)
        |> assign(:error_step, nil)
        |> add_log(:success, "Flow Complete", "Total: #{duration}ms")

      "failed" ->
        duration = elapsed_ms(run)
        error_message = run_failure_message(run)

        # :error_step stays nil here, same as the live run_failed handler
        # (see the Task 3 carried fix) — a run-level failure banner must
        # not be dismissable by a later step_skipped for any one step,
        # whether the banner came from a live event or from reconciliation.
        socket
        |> cleanup_subscription()
        |> assign(:run_status, :failed)
        |> assign(:duration, duration)
        |> assign(:error, "Flow failed: #{error_message}")
        |> assign(:error_step, nil)
        |> assign(:active_edges, MapSet.new())
        |> add_log(:error, "Flow Failed", error_message)

      _ ->
        socket
    end
  end

  defp merge_step_statuses(steps, step_states, steps_config) do
    Enum.reduce(step_states, steps, fn step_state, acc ->
      case to_step_atom(step_state.step_slug, steps_config) do
        nil -> acc
        step_atom -> Map.put(acc, step_atom, step_state_status(step_state.status))
      end
    end)
  end

  defp step_state_status("completed"), do: :completed
  defp step_state_status("failed"), do: :failed
  defp step_state_status("skipped"), do: :skipped
  defp step_state_status("started"), do: :running
  defp step_state_status(_), do: :pending

  defp merge_step_outputs(step_outputs, step_states, steps_config) do
    Enum.reduce(step_states, step_outputs, fn step_state, acc ->
      case to_step_atom(step_state.step_slug, steps_config) do
        nil ->
          acc

        step_atom ->
          if step_state.status == "completed" and not is_nil(step_state.output) do
            Map.put(acc, step_atom, true)
          else
            acc
          end
      end
    end)
  end

  # Finds the failed step_state (if any) to recover a run-failure error
  # message. Deliberately does not resolve/return a step atom: :error_step
  # stays nil for a reconciled failure, matching the live run_failed
  # handler (see the Task 3 carried fix) — a run-level failure banner must
  # not be tied to one step's later step_skipped.
  defp run_failure_message(run) do
    case Enum.find(run.step_states, &(&1.status == "failed")) do
      %{error_message: message} when is_binary(message) and message != "" -> message
      _ -> "condition unmet"
    end
  end

  defp elapsed_ms(run) do
    end_time = run.completed_at || run.failed_at

    case {run.started_at, end_time} do
      {%DateTime{} = started, %DateTime{} = ended} -> DateTime.diff(ended, started, :millisecond)
      _ -> 0
    end
  end

  defp switch_flow(socket, selected) do
    config = flow_config(selected)

    socket
    |> reset_run_state()
    |> assign(:selected_flow, selected)
    |> assign(:steps_config, config.steps)
    |> assign(:edges, config.edges)
    |> assign(:steps, initial_steps(config.steps))
    |> assign(:dsl_segments, FlowDSL.get_segments(flow_module(selected)))
    |> assign(:migration_path, get_migration_path(selected))
    |> assign(:migration_content, get_migration_content(selected))
  end

  defp reset_run_state(socket) do
    cancel_timer(socket.assigns.timer_ref)

    socket
    |> cleanup_subscription()
    |> assign(:url, @default_url)
    |> assign(:run_id, nil)
    |> assign(:run_status, :idle)
    |> assign(:steps, initial_steps(socket.assigns.steps_config))
    |> assign(:step_outputs, %{})
    |> assign(:error, nil)
    |> assign(:error_step, nil)
    |> assign(:duration, nil)
    |> assign(:start_time, nil)
    |> assign(:elapsed_ms, 0)
    |> assign(:event_log, [])
    |> assign(:active_edges, MapSet.new())
    |> assign(:highlighted_step, nil)
    |> assign(:timer_ref, nil)
    |> assign(:output_step, nil)
    |> assign(:output_content, nil)
    |> assign(:output_loading, false)
    |> assign(:show_migration, false)
  end

  defp initial_steps(steps_config),
    do: Map.new(steps_config, fn step -> {step.slug, :pending} end)

  defp cleanup_subscription(%{assigns: %{run_id: nil}} = socket), do: socket

  defp cleanup_subscription(%{assigns: %{run_id: run_id}} = socket) do
    Phoenix.PubSub.unsubscribe(PgflowDemo.PubSub, "pgflow:run:#{run_id}")
    socket
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: :timer.cancel(ref)

  defp step_slugs(steps_config) do
    MapSet.new(steps_config, & &1.slug)
  end

  defp step_slug_strings(steps_config) do
    Enum.map(steps_config, &to_string(&1.slug))
  end

  defp to_step_atom(step_slug, steps_config) when is_binary(step_slug) do
    if step_slug in step_slug_strings(steps_config) do
      String.to_existing_atom(step_slug)
    else
      nil
    end
  end

  defp to_step_atom(step_slug, steps_config) when is_atom(step_slug) do
    if MapSet.member?(step_slugs(steps_config), step_slug), do: step_slug, else: nil
  end

  defp to_step_atom(_, _), do: nil

  defp update_step_status(steps, step_slug, status), do: Map.put(steps, step_slug, status)

  defp get_incoming_edges(step_slug, edges) do
    edges |> Enum.filter(fn {_from, to} -> to == step_slug end) |> MapSet.new()
  end

  defp fetch_step_output(run_id, step_slug, steps_config)
       when is_binary(run_id) and is_binary(step_slug) do
    if step_slug in step_slug_strings(steps_config) do
      PgflowDemo.Flows.get_step_output(run_id, step_slug)
    else
      nil
    end
  end

  defp fetch_step_output(_, _, _), do: nil

  # Validation helpers

  defp validate_url(""), do: {:error, "Please enter a URL"}

  defp validate_url(url) do
    uri = URI.parse(url)

    cond do
      is_nil(uri.scheme) or uri.scheme not in ["http", "https"] ->
        {:error, "URL must start with http:// or https://"}

      is_nil(uri.host) or uri.host == "" ->
        {:error, "Invalid URL format"}

      true ->
        {:ok, url}
    end
  end

  defp validate_step_slug("", _steps_config), do: {:ok, nil}
  defp validate_step_slug(nil, _steps_config), do: {:ok, nil}

  defp validate_step_slug(step_slug, steps_config) when is_binary(step_slug) do
    case to_step_atom(step_slug, steps_config) do
      nil -> {:error, :invalid_step}
      atom -> {:ok, atom}
    end
  end

  defp format_user_error(reason) when is_binary(reason), do: "Failed to start flow: #{reason}"
  defp format_user_error(%{message: msg}), do: "Failed to start flow: #{msg}"
  defp format_user_error(_), do: "Failed to start flow. Please try again."

  defp short_id(run_id), do: String.slice(run_id, 0..7)

  defp log_entry(type, title, message, step_slug \\ nil) do
    %{
      type: type,
      title: title,
      message: message,
      step_slug: step_slug,
      timestamp: System.monotonic_time(:millisecond)
    }
  end

  defp add_log(socket, type, title, message, step_slug \\ nil) do
    entry = log_entry(type, title, message, step_slug)
    logs = [entry | socket.assigns.event_log] |> Enum.take(@max_log_entries)
    assign(socket, :event_log, logs)
  end

  defp step_color(:pending), do: "#4B5563"
  defp step_color(:running), do: "#8B5CF6"
  defp step_color(:completed), do: "#10B981"
  defp step_color(:failed), do: "#EF4444"
  defp step_color(:skipped), do: "#64748B"

  defp node_stroke(:running), do: "#A78BFA"
  defp node_stroke(:completed), do: "#34D399"
  defp node_stroke(:skipped), do: "#94A3B8"
  defp node_stroke(_), do: "#6B7280"

  defp node_label_fill(:running), do: "#A78BFA"
  defp node_label_fill(:completed), do: "#34D399"
  defp node_label_fill(:skipped), do: "#94A3B8"
  defp node_label_fill(_), do: "#D1D5DB"

  defp node_style(:completed), do: "cursor: pointer"
  defp node_style(:skipped), do: "cursor: default; opacity: 0.55"
  defp node_style(_), do: "cursor: default"

  defp flow_tab_class(true),
    do:
      "px-4 py-2 rounded-lg text-sm font-medium bg-purple-600 text-white shadow-lg shadow-purple-500/20"

  defp flow_tab_class(false),
    do:
      "px-4 py-2 rounded-lg text-sm font-medium bg-slate-800/70 text-purple-200/70 hover:bg-slate-700/70"

  defp status_text(:idle), do: "Ready"
  defp status_text(:running), do: "Running"
  defp status_text(:completed), do: "Completed"
  defp status_text(:failed), do: "Failed"

  defp status_color(:idle), do: "text-gray-400"
  defp status_color(:running), do: "text-purple-400"
  defp status_color(:completed), do: "text-emerald-400"
  defp status_color(:failed), do: "text-red-400"

  defp status_bg(:idle), do: "bg-gray-500/20"
  defp status_bg(:running), do: "bg-purple-500/20"
  defp status_bg(:completed), do: "bg-emerald-500/20"
  defp status_bg(:failed), do: "bg-red-500/20"

  defp log_icon(:info), do: "▶"
  defp log_icon(:processing), do: "⚡"
  defp log_icon(:success), do: "✓"
  defp log_icon(:error), do: "✗"

  defp log_color(:info), do: "text-blue-400"
  defp log_color(:processing), do: "text-purple-400"
  defp log_color(:success), do: "text-emerald-400"
  defp log_color(:error), do: "text-red-400"

  defp log_bg(:info), do: "bg-blue-500/20"
  defp log_bg(:processing), do: "bg-purple-500/20"
  defp log_bg(:success), do: "bg-emerald-500/20"
  defp log_bg(:error), do: "bg-red-500/20"

  defp format_duration(nil), do: ""
  defp format_duration(ms) when ms < 1000, do: "#{ms}ms"
  defp format_duration(ms), do: "#{Float.round(ms / 1000, 1)}s"

  defp format_step_label(slug) do
    slug
    |> to_string()
    |> String.replace("_", " ")
    |> String.split(" ")
    |> Enum.map_join(" ", &String.capitalize/1)
  end

  defp format_output(output) when is_map(output), do: Jason.encode!(output, pretty: true)
  defp format_output(output), do: inspect(output)

  defp get_step_coords(slug, steps_config) do
    case Enum.find(steps_config, &(&1.slug == slug)) do
      %{x: x, y: y} -> {x, y}
      _ -> {0, 0}
    end
  end

  defp dag_viewbox(steps_config) do
    max_y = steps_config |> Enum.map(& &1.y) |> Enum.max(fn -> 170 end)
    "0 0 200 #{max_y + 30}"
  end

  defp edge_active?(edge, active_edges), do: MapSet.member?(active_edges, edge)

  # Migration content for display with syntax highlighting
  # Use glob pattern to find migration file (timestamp varies)
  defp find_migration_file(selected_flow) do
    slug = flow_config(selected_flow).slug

    "priv/repo/migrations/*_compile_#{slug}.exs"
    |> Path.wildcard()
    |> List.first()
  end

  defp get_migration_path(selected_flow) do
    case find_migration_file(selected_flow) do
      nil ->
        slug = flow_config(selected_flow).slug
        "priv/repo/migrations/*_compile_#{slug}.exs"

      path ->
        path
    end
  end

  defp get_migration_content(selected_flow) do
    case find_migration_file(selected_flow) do
      nil ->
        module = flow_module(selected_flow)
        "# Migration file not found\n# Run: mix pgflow.gen.flow_migration #{inspect(module)}"

      path ->
        case File.read(path) do
          {:ok, content} ->
            Makeup.highlight(content, lexer: Makeup.Lexers.ElixirLexer)

          {:error, _} ->
            "Migration file not found"
        end
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900">
      <style>
        @keyframes pulse-glow {
          0%, 100% { filter: drop-shadow(0 0 4px #8B5CF6); }
          50% { filter: drop-shadow(0 0 12px #A78BFA); }
        }
        @keyframes dash-flow {
          to { stroke-dashoffset: -20; }
        }
        @keyframes border-spin {
          from { --angle: 0deg; }
          to { --angle: 360deg; }
        }
        @property --angle {
          syntax: '<angle>';
          initial-value: 0deg;
          inherits: false;
        }
        .border-traveling {
          animation: border-spin 3s linear infinite;
          background: conic-gradient(from var(--angle), transparent 80%, rgba(168, 85, 247, 0.8) 90%, rgba(192, 132, 252, 1) 95%, rgba(168, 85, 247, 0.8) 100%);
          -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
          mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
          -webkit-mask-composite: xor;
          mask-composite: exclude;
          padding: 2px;
        }
        .node-active { animation: pulse-glow 1s ease-in-out infinite; }
        .edge-active { animation: dash-flow 0.5s linear infinite; }
        .terminal-scroll::-webkit-scrollbar { width: 4px; }
        .terminal-scroll::-webkit-scrollbar-thumb { background: #6366f1; border-radius: 2px; }
      </style>

      <div class="container mx-auto px-4 py-8 max-w-6xl relative">
        <!-- Dashboard link (top left) -->
        <div class="absolute top-8 left-4 z-10">
          <div class="relative">
            <div class="absolute -inset-[2px] rounded-lg border-traveling"></div>
            <a
              href="/pgflow"
              class="relative inline-flex items-center gap-1.5 px-2 py-2 sm:px-3 sm:py-1.5 bg-gradient-to-r from-purple-600 to-purple-700 hover:from-purple-500 hover:to-purple-600 text-white text-sm font-medium rounded-lg shadow-lg shadow-purple-500/20 transition-all duration-200 hover:shadow-purple-500/30"
              title="Open Dashboard"
            >
              <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z"
                />
              </svg>
              <span class="hidden sm:inline">Open Dashboard</span>
            </a>
          </div>
        </div>
        
    <!-- GitHub link (top right) -->
        <a
          href="https://github.com/agoodway/pgflow"
          target="_blank"
          class="absolute top-8 right-4 text-purple-300/60 hover:text-purple-200 transition-colors"
          title="View Elixir implementation on GitHub"
        >
          <svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true">
            <path
              fill-rule="evenodd"
              d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
              clip-rule="evenodd"
            />
          </svg>
        </a>
        
    <!-- Header -->
        <div class="text-center mb-8">
          <h1 class="text-4xl font-bold text-white mb-2">
            <span class="text-transparent bg-clip-text bg-gradient-to-r from-purple-400 to-pink-400">
              PgFlow
            </span>
            <span class="text-gray-300 font-light ml-2">Demo</span>
          </h1>
          <p class="text-purple-300/70">
            Visualize PgFlow workflow execution in real-time.
          </p>
          <PoweredBy.powered_by size={:md} class="mt-1" />
        </div>
        
    <!-- Interactive tip -->
        <div class="mb-6 px-4 py-3 bg-purple-500/10 border border-purple-500/20 rounded-xl flex items-center justify-center gap-3">
          <span class="text-purple-400 text-lg" title="Tip">ⓘ</span>
          <p class="text-purple-300/80 text-sm">
            Click on
            <a href="#workflow" class="text-emerald-400 hover:underline underline-offset-2">
              Workflow
            </a>
            nodes,
            <a href="#flow-dsl" class="text-orange-400 hover:underline underline-offset-2">
              Flow DSL
            </a>
            steps, <a href="#cron-dsl" class="text-amber-400 hover:underline underline-offset-2">
              Cron DSL
            </a>, or
            <a href="#event-log" class="text-cyan-400 hover:underline underline-offset-2">
              Event Log
            </a>
            entries
            to highlight corresponding elements and view <a
              href="#step-output"
              class="text-purple-400 hover:underline underline-offset-2"
            >Step Output</a>.
          </p>
        </div>
        
    <!-- Input -->
        <div class="backdrop-blur-xl bg-white/5 rounded-2xl p-6 mb-6 border border-white/10">
          <div class="flex gap-2 mb-4">
            <button
              type="button"
              id="tab-article"
              phx-click="select_flow"
              phx-value-flow="article"
              class={flow_tab_class(@selected_flow == :article)}
            >
              Article
            </button>
            <button
              type="button"
              id="tab-onboarding"
              phx-click="select_flow"
              phx-value-flow="onboarding"
              class={flow_tab_class(@selected_flow == :onboarding)}
            >
              Onboarding
            </button>
          </div>

          <form
            :if={@selected_flow == :article}
            id="article-form"
            phx-submit="start_flow"
            class="flex gap-4"
          >
            <input
              type="url"
              name="url"
              value={@url}
              phx-change="update_url"
              placeholder="Enter article URL..."
              class="flex-1 px-4 py-3 rounded-xl bg-slate-800/50 text-white placeholder-gray-500 border border-white/10 focus:outline-none focus:ring-2 focus:ring-purple-500"
              disabled={@run_status == :running}
            />
            <button
              :if={@run_status != :running}
              type="submit"
              class="px-6 py-3 bg-gradient-to-r from-orange-500 to-orange-600 hover:from-orange-400 hover:to-orange-500 text-white font-semibold rounded-xl shadow-lg cursor-pointer"
            >
              Start Flow
            </button>
            <button
              :if={@run_status in [:completed, :failed]}
              type="button"
              phx-click="reset"
              class="px-6 py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl cursor-pointer"
            >
              Reset
            </button>
          </form>

          <form
            :if={@selected_flow == :onboarding}
            id="onboarding-form"
            phx-submit="start_flow"
            phx-change="update_onboarding"
            class="flex flex-wrap items-center gap-4"
          >
            <label class="flex items-center gap-2 text-sm text-purple-200/80">
              <span>Plan</span>
              <select
                name="plan"
                id="onboarding-plan"
                disabled={@run_status == :running}
                class="px-3 py-3 rounded-xl bg-slate-800/50 text-white border border-white/10 focus:outline-none focus:ring-2 focus:ring-purple-500"
              >
                <option value="free" selected={@plan == "free"}>free</option>
                <option value="premium" selected={@plan == "premium"}>premium</option>
              </select>
            </label>
            <label class="flex items-center gap-2 text-sm text-purple-200/80">
              <input
                type="checkbox"
                name="fail_email"
                id="onboarding-fail-email"
                value="true"
                checked={@fail_email}
                disabled={@run_status == :running}
                class="rounded border-white/20 bg-slate-800/50 text-orange-500 focus:ring-purple-500"
              />
              <span>Fail welcome email</span>
            </label>
            <button
              :if={@run_status != :running}
              type="submit"
              class="px-6 py-3 bg-gradient-to-r from-orange-500 to-orange-600 hover:from-orange-400 hover:to-orange-500 text-white font-semibold rounded-xl shadow-lg"
            >
              Start Flow
            </button>
            <button
              :if={@run_status in [:completed, :failed]}
              type="button"
              phx-click="reset"
              class="px-6 py-3 bg-slate-700 hover:bg-slate-600 text-white rounded-xl"
            >
              Reset
            </button>
          </form>

          <div class="mt-4 flex items-center justify-between">
            <div class="flex items-center gap-3">
              <div class={"flex items-center gap-2 px-3 py-1 rounded-full #{status_bg(@run_status)}"}>
                <div class={"w-2 h-2 rounded-full #{if @run_status == :running, do: "animate-pulse"} #{status_color(@run_status)} bg-current"}>
                </div>
                <span class={"text-sm font-medium #{status_color(@run_status)}"}>
                  {status_text(@run_status)}
                </span>
              </div>
              <span :if={@run_status == :running} class="text-purple-300/70 text-sm font-mono">
                {format_duration(@elapsed_ms)}
              </span>
              <span
                :if={@duration && @run_status != :running}
                class="text-emerald-400/70 text-sm font-mono"
              >
                {format_duration(@duration)}
              </span>
            </div>
            <span :if={@run_id} class="text-xs text-gray-500 font-mono">{short_id(@run_id)}</span>
          </div>

          <div :if={@error} class="mt-4 p-3 bg-red-500/10 border border-red-500/30 rounded-xl">
            <p class="text-red-300 text-sm">{@error}</p>
          </div>
        </div>
        
    <!-- Main Grid - Side by side on md+ screens, stacked on mobile -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
          <!-- Workflow -->
          <div
            id="workflow"
            class="backdrop-blur-xl bg-white/5 rounded-2xl p-6 border border-white/10 scroll-mt-4"
          >
            <h2 class="text-lg font-semibold text-white mb-4 flex items-center gap-2">
              <span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>
              <span class="text-emerald-400">Workflow</span>
            </h2>
            <svg viewBox={dag_viewbox(@steps_config)} class="w-full h-auto max-w-md mx-auto">
              <defs>
                <marker id="arrow" markerWidth="6" markerHeight="4" refX="5" refY="2" orient="auto">
                  <polygon points="0 0, 6 2, 0 4" fill="#6B7280" />
                </marker>
                <marker
                  id="arrow-active"
                  markerWidth="6"
                  markerHeight="4"
                  refX="5"
                  refY="2"
                  orient="auto"
                >
                  <polygon points="0 0, 6 2, 0 4" fill="#8B5CF6" />
                </marker>
                <marker
                  id="arrow-done"
                  markerWidth="6"
                  markerHeight="4"
                  refX="5"
                  refY="2"
                  orient="auto"
                >
                  <polygon points="0 0, 6 2, 0 4" fill="#10B981" />
                </marker>
              </defs>
              
    <!-- Edges -->
              <%= for {from, to} <- @edges do %>
                <% {x1, y1} = get_step_coords(from, @steps_config) %>
                <% {x2, y2} = get_step_coords(to, @steps_config) %>
                <% is_active = edge_active?({from, to}, @active_edges) %>
                <% from_status = Map.get(@steps, from) %>
                <% from_done = from_status in [:completed, :skipped] %>
                <% from_skipped = from_status == :skipped %>
                <% dx = x2 - x1
                dy = y2 - y1
                dist = :math.sqrt(dx * dx + dy * dy)
                sx = x1 + dx / dist * @node_radius
                sy = y1 + dy / dist * @node_radius
                ex = x2 - dx / dist * (@node_radius + 5)
                ey = y2 - dy / dist * (@node_radius + 5) %>
                <line
                  x1={sx}
                  y1={sy}
                  x2={ex}
                  y2={ey}
                  stroke={
                    cond do
                      is_active -> "#8B5CF6"
                      from_skipped -> "#64748B"
                      from_done -> "#10B981"
                      true -> "#4B5563"
                    end
                  }
                  stroke-width={if is_active, do: "2", else: "1.5"}
                  stroke-dasharray={
                    cond do
                      is_active -> "4 4"
                      from_skipped -> "3 2"
                      true -> "none"
                    end
                  }
                  class={if is_active, do: "edge-active", else: ""}
                  marker-end={
                    if is_active,
                      do: "url(#arrow-active)",
                      else: if(from_done, do: "url(#arrow-done)", else: "url(#arrow)")
                  }
                />
              <% end %>
              
    <!-- Nodes -->
              <%= for step <- @steps_config do %>
                <% status = Map.get(@steps, step.slug, :pending) %>
                <% highlighted = step.slug == @highlighted_step %>
                <g
                  class={if status == :running, do: "node-active", else: ""}
                  phx-click="click_node"
                  phx-value-step={step.slug}
                  style={node_style(status)}
                >
                  <%= if highlighted do %>
                    <circle
                      cx={step.x}
                      cy={step.y}
                      r={@node_radius + 2}
                      fill="none"
                      stroke="#F472B6"
                      stroke-width="1.5"
                    />
                  <% end %>
                  <circle
                    cx={step.x}
                    cy={step.y}
                    r={@node_radius}
                    fill={step_color(status)}
                    stroke={node_stroke(status)}
                    stroke-width={if status == :running, do: "1.5", else: "1"}
                    stroke-dasharray={if status == :skipped, do: "3 2", else: "none"}
                  />
                  <%= if status == :completed do %>
                    <text
                      x={step.x}
                      y={step.y + 1}
                      text-anchor="middle"
                      dominant-baseline="middle"
                      fill="white"
                      font-size="7"
                      font-weight="bold"
                    >
                      ✓
                    </text>
                  <% end %>
                  <%= if status == :failed do %>
                    <text
                      x={step.x}
                      y={step.y + 1}
                      text-anchor="middle"
                      dominant-baseline="middle"
                      fill="white"
                      font-size="7"
                      font-weight="bold"
                    >
                      ✗
                    </text>
                  <% end %>
                  <%= if status == :skipped do %>
                    <text
                      x={step.x}
                      y={step.y + 1}
                      text-anchor="middle"
                      dominant-baseline="middle"
                      fill="white"
                      font-size="8"
                      font-weight="bold"
                    >
                      –
                    </text>
                  <% end %>
                  <%= if status == :running do %>
                    <circle
                      cx={step.x}
                      cy={step.y}
                      r="4"
                      fill="none"
                      stroke="white"
                      stroke-width="1"
                      stroke-dasharray="4 4"
                      class="edge-active"
                    />
                  <% end %>
                  <rect
                    x={step.x - String.length(step.label) * 2.2 - 4}
                    y={step.y + @node_radius + 2}
                    width={String.length(step.label) * 4.4 + 8}
                    height="12"
                    fill="#0f172a"
                    rx="2"
                  />
                  <text
                    x={step.x}
                    y={step.y + @node_radius + 10}
                    text-anchor="middle"
                    fill={node_label_fill(status)}
                    font-size="7"
                    font-weight="500"
                  >
                    {step.label}
                  </text>
                </g>
              <% end %>
            </svg>
          </div>
          
    <!-- Event Log -->
          <div
            id="event-log"
            class="backdrop-blur-xl bg-white/5 rounded-2xl p-6 border border-white/10 flex flex-col scroll-mt-4"
          >
            <h2 class="text-lg font-semibold text-white mb-4 flex items-center gap-2">
              <span class="w-1.5 h-1.5 rounded-full bg-cyan-500"></span>
              <span class="text-cyan-400">Event Log</span>
            </h2>
            <div class="bg-slate-900/80 rounded-xl p-3 flex-1 min-h-[14rem] overflow-y-auto terminal-scroll font-mono text-xs">
              <%= if Enum.empty?(@event_log) do %>
                <div class="text-gray-600 text-center py-8">
                  <p>No events yet</p>
                  <p class="text-xs mt-1">Start a flow to see events</p>
                </div>
              <% else %>
                <div class="space-y-1.5">
                  <%= for entry <- Enum.reverse(@event_log) do %>
                    <% has_output = entry.step_slug && Map.get(@step_outputs, entry.step_slug, false) %>
                    <% is_completed = entry.type == :success && entry.step_slug %>
                    <div
                      class={"flex items-center gap-2 px-2 py-1 rounded #{log_bg(entry.type)} #{if entry.step_slug, do: "cursor-pointer hover:ring-1 hover:ring-purple-500/50"} #{if entry.step_slug == @highlighted_step, do: "ring-1 ring-purple-500"}"}
                      phx-click={
                        if is_completed,
                          do: "view_step_output",
                          else: if(entry.step_slug, do: "highlight_step")
                      }
                      phx-value-step={if entry.step_slug, do: to_string(entry.step_slug)}
                    >
                      <span class={log_color(entry.type)}>{log_icon(entry.type)}</span>
                      <span class={log_color(entry.type) <> " font-medium"}>{entry.title}</span>
                      <span class="text-gray-400 truncate flex-1">{entry.message}</span>
                      <%= if has_output do %>
                        <span class="text-purple-400 text-[10px]" title="View output">📄</span>
                      <% end %>
                    </div>
                  <% end %>
                </div>
              <% end %>
            </div>
          </div>
        </div>
        
    <!-- Flow DSL -->
        <div
          id="flow-dsl"
          class="mt-6 backdrop-blur-xl bg-white/5 rounded-2xl p-6 border border-white/10 scroll-mt-4"
        >
          <h2 class="text-lg font-semibold text-white mb-4 flex items-center gap-2">
            <span class="w-1.5 h-1.5 rounded-full bg-orange-500"></span>
            <span class="text-orange-400">Flow DSL</span>
          </h2>
          <div
            id="flow-dsl-container"
            class="bg-slate-900/80 rounded-xl p-4 max-h-[32rem] overflow-y-auto terminal-scroll"
          >
            <FlowDSL.flow_dsl
              segments={@dsl_segments}
              steps={@steps}
              highlighted_step={@highlighted_step}
            />
          </div>
          <p class="mt-3 text-sm text-gray-400">
            This Flow DSL is compiled to an
            <button
              phx-click="toggle_migration"
              class="text-cyan-400 hover:text-cyan-300 underline underline-offset-2 cursor-pointer"
            >
              Ecto migration
            </button>
            that creates the flow definition in PostgreSQL.
          </p>
          <%= if @show_migration do %>
            <div class="mt-4 bg-slate-900/80 rounded-xl p-4 max-h-[24rem] overflow-y-auto terminal-scroll">
              <div class="flex items-center justify-between mb-2">
                <span class="text-xs text-gray-400 font-mono">
                  {@migration_path}
                </span>
                <button phx-click="toggle_migration" class="text-xs text-gray-500 hover:text-gray-400">
                  Close
                </button>
              </div>
              <div class="font-mono text-xs leading-relaxed">
                {Phoenix.HTML.raw(@migration_content)}
              </div>
            </div>
          <% end %>
        </div>
        
    <!-- Step Output -->
        <div
          id="step-output"
          class="mt-6 backdrop-blur-xl bg-white/5 rounded-2xl p-6 border border-white/10 scroll-mt-4"
        >
          <h2 class="text-lg font-semibold text-white mb-4 flex items-center gap-2">
            <span class="w-1.5 h-1.5 rounded-full bg-purple-500"></span>
            <span class="text-purple-400">Step Output</span>
            <%= if @output_step do %>
              <span class="text-sm font-normal text-gray-400">
                — {format_step_label(@output_step)}
              </span>
            <% end %>
          </h2>
          <div
            class="bg-slate-900/80 rounded-xl p-4 max-h-[20rem] overflow-y-auto terminal-scroll"
            style={if @output_content, do: "", else: "min-height: 12rem"}
          >
            <%= if @output_loading do %>
              <div class="flex items-center justify-center py-12">
                <div class="animate-spin rounded-full h-6 w-6 border-2 border-purple-500 border-t-transparent">
                </div>
              </div>
            <% else %>
              <%= if @output_content do %>
                <pre class="text-gray-300 text-xs whitespace-pre-wrap font-mono"><%= format_output(@output_content) %></pre>
              <% else %>
                <div class="text-gray-600 text-center py-12">
                  <p>No output yet</p>
                  <p class="text-xs mt-1">Run a flow or click a completed step to view its output</p>
                </div>
              <% end %>
            <% end %>
          </div>
        </div>
        
    <!-- Cron DSL -->
        <div
          id="cron-dsl"
          class="mt-6 backdrop-blur-xl bg-white/5 rounded-2xl p-6 border border-white/10 scroll-mt-4"
        >
          <h2 class="text-lg font-semibold text-white mb-4 flex items-center gap-2">
            <span class="w-1.5 h-1.5 rounded-full bg-amber-500"></span>
            <span class="text-amber-400">Cron DSL</span>
            <span class="text-sm font-normal text-gray-400 ml-2">— Scheduled cleanup job</span>
          </h2>
          <div class="bg-slate-900/80 rounded-xl p-4 max-h-[32rem] overflow-y-auto terminal-scroll">
            <CronDSL.cron_dsl
              highlighted_source={@cron_highlighted_source}
              next_run_info={@cron_next_run_info}
            />
          </div>
          <p class="mt-3 text-sm text-gray-400">
            Scheduled job that prunes old article_flow runs hourly.
            <a
              href="/pgflow/crons/article_flow_cleanup"
              class="text-amber-400 hover:text-amber-300 underline underline-offset-2"
            >
              View in Dashboard
            </a>
          </p>
        </div>
        
    <!-- Footer -->
        <footer class="mt-8 text-center max-w-2xl mx-auto">
          <div class="flex flex-wrap items-center justify-center gap-x-3 gap-y-1">
            <PoweredBy.powered_by size={:sm} />
            <span class="text-xs text-purple-300/40 hidden sm:inline">·</span>
            <a
              href="https://github.com/agoodway/pgflow"
              target="_blank"
              class="text-xs text-purple-300/60 hover:text-purple-200 underline underline-offset-2 whitespace-nowrap"
            >
              GitHub
            </a>
          </div>
        </footer>
      </div>
    </div>
    """
  end
end
