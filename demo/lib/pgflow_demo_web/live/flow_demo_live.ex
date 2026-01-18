defmodule PgflowDemoWeb.FlowDemoLive do
  @moduledoc """
  LiveView for visualizing PgFlow workflow execution in real-time.
  """

  use PgflowDemoWeb, :live_view

  alias PgFlow.Client
  alias PgflowDemoWeb.Components.FlowDSL
  alias PgflowDemoWeb.TelemetryBroadcaster

  # UI Constants
  @node_radius 10
  @timer_interval_ms 100
  @max_log_entries 50

  # DAG layout - tight spacing
  @steps [
    %{slug: :fetch_article, label: "Fetch", x: 100, y: 15},
    %{slug: :convert_to_markdown, label: "Markdown", x: 100, y: 80},
    %{slug: :summarize, label: "Summarize", x: 50, y: 125},
    %{slug: :extract_keywords, label: "Keywords", x: 150, y: 125},
    %{slug: :publish, label: "Publish", x: 100, y: 170}
  ]

  @edges [
    {:fetch_article, :convert_to_markdown},
    {:convert_to_markdown, :summarize},
    {:convert_to_markdown, :extract_keywords},
    {:summarize, :publish},
    {:extract_keywords, :publish}
  ]

  @default_url "https://www.pgflow.dev/news/pgflow-0-13-1-cli-fix-step-output-storage-for-conditional-execution/"

  # Valid step slugs for validation
  @valid_step_slugs MapSet.new([
                      :fetch_article,
                      :convert_to_markdown,
                      :summarize,
                      :extract_keywords,
                      :publish
                    ])

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:url, @default_url)
      |> assign(:run_id, nil)
      |> assign(:run_status, :idle)
      |> assign(:steps, initial_steps())
      |> assign(:step_outputs, %{})
      |> assign(:error, nil)
      |> assign(:duration, nil)
      |> assign(:start_time, nil)
      |> assign(:elapsed_ms, 0)
      |> assign(:event_log, [])
      |> assign(:steps_config, @steps)
      |> assign(:edges, @edges)
      |> assign(:active_edges, MapSet.new())
      |> assign(:highlighted_step, nil)
      |> assign(:node_radius, @node_radius)
      |> assign(:timer_ref, nil)
      |> assign(:output_step, nil)
      |> assign(:output_content, nil)
      |> assign(:output_loading, false)
      |> assign(:dsl_segments, FlowDSL.get_segments())
      |> assign(:show_migration, false)
      |> assign(:migration_content, get_migration_content())

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
  def handle_event("start_flow", %{"url" => url}, socket) do
    case validate_url(url) do
      {:error, message} ->
        {:noreply, assign(socket, :error, message)}

      {:ok, valid_url} ->
        case Client.start_flow(:article_flow, %{"url" => valid_url}) do
          {:ok, run_id} ->
            # Cleanup previous subscription before subscribing to new one
            socket = cleanup_subscription(socket)
            Phoenix.PubSub.subscribe(PgflowDemo.PubSub, TelemetryBroadcaster.topic(run_id))

            cancel_timer(socket.assigns.timer_ref)

            timer_ref =
              if connected?(socket) do
                {:ok, ref} = :timer.send_interval(@timer_interval_ms, self(), :tick)
                ref
              else
                nil
              end

            socket =
              socket
              |> assign(:run_id, run_id)
              |> assign(:run_status, :running)
              |> assign(:error, nil)
              |> assign(:steps, initial_steps())
              |> assign(:step_outputs, %{})
              |> assign(:duration, nil)
              |> assign(:start_time, System.monotonic_time(:millisecond))
              |> assign(:elapsed_ms, 0)
              |> assign(:event_log, [
                log_entry(:info, "Flow started", "Run ID: #{short_id(run_id)}")
              ])
              |> assign(:active_edges, MapSet.new())
              |> assign(:timer_ref, timer_ref)

            {:noreply, socket}

          {:error, reason} ->
            {:noreply, assign(socket, :error, format_user_error(reason))}
        end
    end
  end

  @impl true
  def handle_event("reset", _params, socket) do
    cancel_timer(socket.assigns.timer_ref)

    socket =
      socket
      |> cleanup_subscription()
      |> assign(:url, @default_url)
      |> assign(:run_id, nil)
      |> assign(:run_status, :idle)
      |> assign(:steps, initial_steps())
      |> assign(:step_outputs, %{})
      |> assign(:error, nil)
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

    {:noreply, socket}
  end

  @impl true
  def handle_event("toggle_migration", _params, socket) do
    {:noreply, assign(socket, :show_migration, !socket.assigns.show_migration)}
  end

  @impl true
  def handle_event("highlight_step", %{"step" => step_slug}, socket) do
    # Event log entry click: highlight step and scroll within DSL pane
    case validate_step_slug(step_slug) do
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
    case validate_step_slug(step_slug) do
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
    case validate_step_slug(step_slug) do
      {:ok, nil} ->
        {:noreply, socket}

      {:ok, step_atom} ->
        step_status = Map.get(socket.assigns.steps, step_atom)

        socket =
          if step_status == :completed and socket.assigns.run_id do
            # Completed step: show output and highlight
            socket
            |> assign(:output_step, step_atom)
            |> assign(:output_loading, true)
            |> assign(:highlighted_step, step_atom)
            |> maybe_push_scroll_event(scroll_event, step_atom)
            |> tap(fn _ -> send(self(), {:fetch_step_output, step_atom}) end)
          else
            # Non-completed step: just highlight
            socket
            |> assign(:highlighted_step, step_atom)
            |> maybe_push_scroll_event(scroll_event, step_atom)
          end

        {:noreply, socket}

      {:error, _} ->
        {:noreply, socket}
    end
  end

  defp maybe_push_scroll_event(socket, nil, _step_atom), do: socket

  defp maybe_push_scroll_event(socket, event, step_atom) do
    push_event(socket, event, %{step: to_string(step_atom)})
  end

  @impl true
  def handle_info({:fetch_step_output, step_atom}, socket) do
    output = fetch_step_output(socket.assigns.run_id, to_string(step_atom))

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

  @impl true
  def handle_info({:task_started, step_slug, task_index}, socket) do
    case to_step_atom(step_slug) do
      nil ->
        {:noreply, socket}

      step_atom ->
        steps = update_step_status(socket.assigns.steps, step_atom, :running)
        active_edges = get_incoming_edges(step_atom)

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
  def handle_info({:task_completed, step_slug, _task_index, duration_ms, output}, socket) do
    case to_step_atom(step_slug) do
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

        # Check if all steps are completed (detect run completion since pgflow doesn't emit run_completed)
        socket = maybe_complete_run(socket, steps)

        {:noreply, socket}
    end
  end

  @impl true
  def handle_info({:task_failed, step_slug, _task_index, error, duration_ms}, socket) do
    case to_step_atom(step_slug) do
      nil ->
        {:noreply, socket}

      step_atom ->
        steps = update_step_status(socket.assigns.steps, step_atom, :failed)

        socket =
          socket
          |> assign(:steps, steps)
          |> assign(:active_edges, MapSet.new())
          |> assign(:error, "Step #{step_slug} failed: #{inspect(error)}")
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
  def handle_info({:run_started, _flow_slug}, socket) do
    {:noreply, assign(socket, :run_status, :running)}
  end

  @impl true
  def handle_info({:run_completed, duration_ms}, socket) do
    cancel_timer(socket.assigns.timer_ref)

    socket =
      socket
      |> cleanup_subscription()
      |> assign(:run_status, :completed)
      |> assign(:duration, duration_ms)
      |> assign(:active_edges, MapSet.new())
      |> assign(:timer_ref, nil)
      |> add_log(:success, "Flow Complete", "Total: #{duration_ms}ms")

    {:noreply, socket}
  end

  @impl true
  def handle_info({:run_failed, error, duration_ms}, socket) do
    cancel_timer(socket.assigns.timer_ref)

    socket =
      socket
      |> cleanup_subscription()
      |> assign(:run_status, :failed)
      |> assign(:duration, duration_ms)
      |> assign(:error, "Flow failed: #{inspect(error)}")
      |> assign(:active_edges, MapSet.new())
      |> assign(:timer_ref, nil)
      |> add_log(:error, "Flow Failed", "#{inspect(error)}")

    {:noreply, socket}
  end

  @impl true
  def handle_info(_msg, socket), do: {:noreply, socket}

  # Helpers

  defp initial_steps, do: Map.new(@steps, fn step -> {step.slug, :pending} end)

  defp cleanup_subscription(%{assigns: %{run_id: nil}} = socket), do: socket

  defp cleanup_subscription(%{assigns: %{run_id: run_id}} = socket) do
    Phoenix.PubSub.unsubscribe(PgflowDemo.PubSub, TelemetryBroadcaster.topic(run_id))
    socket
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: :timer.cancel(ref)

  @valid_step_slugs_strings [
    "fetch_article",
    "convert_to_markdown",
    "summarize",
    "extract_keywords",
    "publish"
  ]

  defp to_step_atom(step_slug) when is_binary(step_slug) do
    if step_slug in @valid_step_slugs_strings do
      String.to_existing_atom(step_slug)
    else
      nil
    end
  end

  defp to_step_atom(step_slug) when is_atom(step_slug), do: step_slug
  defp to_step_atom(_), do: nil

  defp update_step_status(steps, step_slug, status), do: Map.put(steps, step_slug, status)

  defp get_incoming_edges(step_slug) do
    @edges |> Enum.filter(fn {_from, to} -> to == step_slug end) |> MapSet.new()
  end

  defp fetch_step_output(run_id, step_slug) when is_binary(run_id) and is_binary(step_slug) do
    if step_slug in @valid_step_slugs_strings do
      PgflowDemo.Flows.get_step_output(run_id, step_slug)
    else
      nil
    end
  end

  defp fetch_step_output(_, _), do: nil

  defp maybe_complete_run(%{assigns: %{run_status: :running}} = socket, steps) do
    if all_steps_completed?(steps) do
      elapsed_ms = System.monotonic_time(:millisecond) - socket.assigns.start_time
      cancel_timer(socket.assigns.timer_ref)

      socket
      |> assign(:run_status, :completed)
      |> assign(:duration, elapsed_ms)
      |> assign(:active_edges, MapSet.new())
      |> assign(:timer_ref, nil)
      |> add_log(:success, "Flow Complete", "Total: #{elapsed_ms}ms")
    else
      socket
    end
  end

  defp maybe_complete_run(socket, _steps), do: socket

  defp all_steps_completed?(steps) do
    Enum.all?(steps, fn {_slug, status} -> status == :completed end)
  end

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

  defp validate_step_slug(""), do: {:ok, nil}
  defp validate_step_slug(nil), do: {:ok, nil}

  defp validate_step_slug(step_slug) when is_binary(step_slug) do
    atom = String.to_existing_atom(step_slug)

    if MapSet.member?(@valid_step_slugs, atom) do
      {:ok, atom}
    else
      {:error, :invalid_step}
    end
  rescue
    ArgumentError -> {:error, :invalid_step}
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

  defp get_step_coords(slug) do
    case Enum.find(@steps, &(&1.slug == slug)) do
      %{x: x, y: y} -> {x, y}
      _ -> {0, 0}
    end
  end

  defp edge_active?(edge, active_edges), do: MapSet.member?(active_edges, edge)

  # Migration content for display with syntax highlighting
  @migration_path "priv/repo/migrations/20260118130454_compile_article_flow.exs"

  defp get_migration_content do
    case File.read(@migration_path) do
      {:ok, content} ->
        Makeup.highlight(content, lexer: Makeup.Lexers.ElixirLexer)

      {:error, _} ->
        "Migration file not found"
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
        .node-active { animation: pulse-glow 1s ease-in-out infinite; }
        .edge-active { animation: dash-flow 0.5s linear infinite; }
        .terminal-scroll::-webkit-scrollbar { width: 4px; }
        .terminal-scroll::-webkit-scrollbar-thumb { background: #6366f1; border-radius: 2px; }
      </style>

      <div class="container mx-auto px-4 py-8 max-w-6xl relative">
        <!-- GitHub link -->
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
          <p class="text-purple-300/50 text-sm mt-1">
            Powered by <a
              href="https://www.postgresql.org"
              target="_blank"
              class="text-purple-300 hover:text-purple-200 underline underline-offset-2"
            >PostgreSQL</a>, <a
              href="https://github.com/pgmq/pgmq"
              target="_blank"
              class="text-purple-300 hover:text-purple-200 underline underline-offset-2"
            >PGMQ</a>, <a
              href="https://pgflow.dev"
              target="_blank"
              class="text-purple-300 hover:text-purple-200 underline underline-offset-2"
            >PgFlow</a>,
            <a
              href="https://elixir-lang.org"
              target="_blank"
              class="text-purple-300 hover:text-purple-200 underline underline-offset-2"
            >
              Elixir
            </a>
            and
            <a
              href="https://phoenixframework.org"
              target="_blank"
              class="text-purple-300 hover:text-purple-200 underline underline-offset-2"
            >
              Phoenix LiveView
            </a>
          </p>
        </div>

    <!-- Interactive tip -->
        <div class="mb-6 px-4 py-3 bg-purple-500/10 border border-purple-500/20 rounded-xl flex items-center justify-center gap-3">
          <span class="text-purple-400 text-lg" title="Tip">ⓘ</span>
          <p class="text-purple-300/80 text-sm">
            Click on
            <a href="#workflow-graph" class="text-emerald-400 hover:underline underline-offset-2">
              Workflow Graph
            </a>
            nodes,
            <a href="#flow-dsl" class="text-orange-400 hover:underline underline-offset-2">Flow DSL</a>
            steps, or
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
          <form phx-submit="start_flow" class="flex gap-4">
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
          <!-- DAG -->
          <div
            id="workflow-graph"
            class="backdrop-blur-xl bg-white/5 rounded-2xl p-6 border border-white/10 scroll-mt-4"
          >
            <h2 class="text-lg font-semibold text-white mb-4 flex items-center gap-2">
              <span class="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>
              <span class="text-emerald-400">Workflow Graph</span>
            </h2>
            <svg viewBox="0 0 200 200" class="w-full h-auto max-w-md mx-auto">
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
                <% {x1, y1} = get_step_coords(from) %>
                <% {x2, y2} = get_step_coords(to) %>
                <% is_active = edge_active?({from, to}, @active_edges) %>
                <% from_done = Map.get(@steps, from) == :completed %>
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
                    if is_active, do: "#8B5CF6", else: if(from_done, do: "#10B981", else: "#4B5563")
                  }
                  stroke-width={if is_active, do: "2", else: "1.5"}
                  stroke-dasharray={if is_active, do: "4 4", else: "none"}
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
                  style={if status == :completed, do: "cursor: pointer", else: "cursor: default"}
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
                    stroke={
                      if status == :running,
                        do: "#A78BFA",
                        else: if(status == :completed, do: "#34D399", else: "#6B7280")
                    }
                    stroke-width={if status == :running, do: "1.5", else: "1"}
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
                    fill={
                      if status == :running,
                        do: "#A78BFA",
                        else: if(status == :completed, do: "#34D399", else: "#D1D5DB")
                    }
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
                  priv/repo/migrations/20260118130454_compile_article_flow.exs
                </span>
                <button
                  phx-click="toggle_migration"
                  class="text-xs text-gray-500 hover:text-gray-400"
                >
                  Close
                </button>
              </div>
              <div class="font-mono text-xs leading-relaxed">
                {Phoenix.HTML.raw(@migration_content)}
              </div>
            </div>
          <% end %>
        </div>

    <!-- Output Panel -->
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

    <!-- Footer -->
        <footer class="mt-8 text-center text-purple-300/40 text-xs">
          Powered by <a
            href="https://www.postgresql.org"
            target="_blank"
            class="text-purple-300/60 hover:text-purple-200 underline underline-offset-2"
          >PostgreSQL</a>, <a
            href="https://github.com/pgmq/pgmq"
            target="_blank"
            class="text-purple-300/60 hover:text-purple-200 underline underline-offset-2"
          >PGMQ</a>, <a
            href="https://pgflow.dev"
            target="_blank"
            class="text-purple-300/60 hover:text-purple-200 underline underline-offset-2"
          >PgFlow</a>,
          <a
            href="https://elixir-lang.org"
            target="_blank"
            class="text-purple-300/60 hover:text-purple-200 underline underline-offset-2"
          >
            Elixir
          </a>
          and
          <a
            href="https://phoenixframework.org"
            target="_blank"
            class="text-purple-300/60 hover:text-purple-200 underline underline-offset-2"
          >
            Phoenix LiveView
          </a>
          <span class="mx-3">·</span>
          <a
            href="https://github.com/agoodway/pgflow"
            target="_blank"
            class="text-purple-300/60 hover:text-purple-200 underline underline-offset-2"
          >
            GitHub
          </a>
        </footer>
      </div>
    </div>
    """
  end
end
