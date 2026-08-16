defmodule PgFlow.LiveClient do
  @moduledoc """
  LiveView-native client for tracking flow and job runs in real-time.

  Provides functions to start flows, subscribe to existing runs, and handle
  PubSub updates — all operating on `%PgFlow.Schema.Run{}` structs stored
  in socket assigns.

  ## Usage

      defmodule MyAppWeb.FlowLive do
        use MyAppWeb, :live_view

        alias PgFlow.LiveClient

        def mount(_params, _session, socket) do
          {:ok, LiveClient.init(socket, pubsub: MyApp.PubSub)}
        end

        def handle_event("start", %{"url" => url}, socket) do
          case LiveClient.start_flow(socket, :article_flow, %{"url" => url}) do
            {:ok, socket} -> {:noreply, socket}
            {:error, _reason, socket} -> {:noreply, socket}
          end
        end

        def handle_info({:pgflow, _, _} = msg, socket) do
          {:noreply, LiveClient.handle_info(msg, socket)}
        end
      end

  ## Multiple Runs

  Track multiple runs simultaneously using the `:as` option:

      socket = LiveClient.start_flow(socket, :flow_a, input, as: :run_a)
      socket = LiveClient.subscribe(socket, some_run_id, as: :run_b)

  Then access `@run_a` and `@run_b` independently in templates.

  ## Jobs

  Jobs work identically — use `enqueue/4` for naming parity with `PgFlow.enqueue/2`:

      LiveClient.enqueue(socket, MyApp.Jobs.SendEmail, %{"to" => "user@example.com"})

  """

  alias PgFlow.Client
  alias PgFlow.Schema.Run

  @default_assign_key :flow_run
  @private_key :pgflow_live_client

  @doc """
  Initializes the socket for flow tracking.

  Sets the default assign to `nil` and stores PubSub config in socket private.
  Safe to call during both connected and disconnected mount phases.

  ## Options

    * `:pubsub` — (required) the Phoenix.PubSub module
    * `:as` — assign key for the run (default: `:flow_run`)
    * `:repo` — Ecto repo for snapshot loading (default: uses configured PgFlow repo)
  """
  @spec init(Phoenix.LiveView.Socket.t(), keyword()) :: Phoenix.LiveView.Socket.t()
  def init(socket, opts) do
    pubsub = Keyword.fetch!(opts, :pubsub)
    assign_key = Keyword.get(opts, :as, @default_assign_key)

    socket
    |> Phoenix.Component.assign(assign_key, nil)
    |> put_private(%{pubsub: pubsub, subscriptions: %{}})
  end

  @doc """
  Starts a flow and subscribes to real-time updates.

  Creates the flow run via `PgFlow.Client.start_flow/2`, subscribes to the
  run's PubSub topic, loads the current state from the database, and assigns
  the `%Run{}` struct.

  Returns `{:ok, socket}` on success or `{:error, reason, socket}` on failure.

  ## Options

    * `:as` — assign key (default: `:flow_run`)
  """
  @spec start_flow(Phoenix.LiveView.Socket.t(), module() | atom() | String.t(), map(), keyword()) ::
          {:ok, Phoenix.LiveView.Socket.t()} | {:error, term(), Phoenix.LiveView.Socket.t()}
  def start_flow(socket, flow_module_or_slug, input, opts \\ []) do
    case Client.start_flow(flow_module_or_slug, input) do
      {:ok, run_id} ->
        assign_key = Keyword.get(opts, :as, @default_assign_key)
        {:ok, subscribe_and_load(socket, run_id, assign_key)}

      {:error, reason} ->
        {:error, reason, socket}
    end
  end

  @doc """
  Starts a job and subscribes to real-time updates.

  Convenience wrapper providing naming parity with `PgFlow.enqueue/2`.
  Behaves identically to `start_flow/4`.

  ## Options

    * `:as` — assign key (default: `:flow_run`)
  """
  @spec enqueue(Phoenix.LiveView.Socket.t(), module(), map(), keyword()) ::
          {:ok, Phoenix.LiveView.Socket.t()} | {:error, term(), Phoenix.LiveView.Socket.t()}
  def enqueue(socket, job_module, input, opts \\ []) do
    start_flow(socket, job_module, input, opts)
  end

  @doc """
  Subscribes to an existing run's real-time updates.

  Loads the current state from the database and subscribes to PubSub.
  Useful for observing runs started elsewhere (e.g., run detail pages).

  ## Options

    * `:as` — assign key (default: `:flow_run`)
  """
  @spec subscribe(Phoenix.LiveView.Socket.t(), String.t(), keyword()) ::
          Phoenix.LiveView.Socket.t()
  def subscribe(socket, run_id, opts \\ []) do
    assign_key = Keyword.get(opts, :as, @default_assign_key)
    subscribe_and_load(socket, run_id, assign_key)
  end

  @doc """
  Unsubscribes from a run's updates and resets the assign to nil.

  ## Options

    * `:as` — assign key to unsubscribe (default: `:flow_run`)
  """
  @spec unsubscribe(Phoenix.LiveView.Socket.t(), keyword()) :: Phoenix.LiveView.Socket.t()
  def unsubscribe(socket, opts \\ []) do
    assign_key = Keyword.get(opts, :as, @default_assign_key)
    config = get_private(socket)

    case Map.get(config.subscriptions, assign_key) do
      nil ->
        socket

      run_id ->
        Phoenix.PubSub.unsubscribe(config.pubsub, "pgflow:run:#{run_id}")
        subscriptions = Map.delete(config.subscriptions, assign_key)

        socket
        |> Phoenix.Component.assign(assign_key, nil)
        |> put_private(%{config | subscriptions: subscriptions})
    end
  end

  @doc """
  Handles a PgFlow PubSub message, updating the tracked run in assigns.

  Call this from your LiveView's `handle_info/2`:

      def handle_info({:pgflow, _, _} = msg, socket) do
        {:noreply, LiveClient.handle_info(msg, socket)}
      end

  Returns the socket unchanged if the message is for an untracked run.
  """
  @spec handle_info({:pgflow, String.t(), tuple()}, Phoenix.LiveView.Socket.t()) ::
          Phoenix.LiveView.Socket.t()
  def handle_info({:pgflow, run_id, event}, socket) do
    config = get_private(socket)

    config.subscriptions
    |> find_assign_keys(run_id)
    |> Enum.reduce(socket, fn assign_key, acc ->
      case acc.assigns[assign_key] do
        nil -> acc
        run -> Phoenix.Component.assign(acc, assign_key, apply_event(run, event))
      end
    end)
  end

  # Applies a PubSub event to a %Run{} struct, returning an updated struct.
  # Updates are applied in-memory without DB queries.
  defp apply_event(%Run{} = run, {:task_started, %{step_slug: step_slug} = payload}) do
    update_step(run, step_slug, fn step ->
      if status_advances?(step.status, "started") do
        %{step | status: "started", started_at: payload[:timestamp]}
      else
        step
      end
    end)
  end

  defp apply_event(%Run{} = run, {:task_completed, %{step_slug: step_slug} = payload}) do
    update_step(run, step_slug, fn step ->
      if status_advances?(step.status, "completed") do
        %{step | status: "completed", output: payload[:output], completed_at: payload[:timestamp]}
      else
        step
      end
    end)
  end

  defp apply_event(%Run{} = run, {:task_failed, %{step_slug: step_slug} = payload}) do
    update_step(run, step_slug, fn step ->
      if status_advances?(step.status, "failed") do
        %{step | status: "failed", error_message: payload[:error], failed_at: payload[:timestamp]}
      else
        step
      end
    end)
  end

  defp apply_event(%Run{} = run, {:step_skipped, %{step_slug: step_slug} = payload}) do
    update_step(run, step_slug, fn step ->
      # The DB is authoritative for `step_skipped`: it is only emitted once
      # `pgflow.fail_task` (or its condition-check equivalent) has decided
      # to skip the step. A worker's task-failure event can race ahead of
      # that decision and mark the step "failed" locally first, so a
      # DB-confirmed skip is allowed to supersede a locally-inferred
      # failure even though both sit at the same terminal precedence.
      if status_advances?(step.status, "skipped") or step.status == "failed" do
        %{
          step
          | status: "skipped",
            skip_reason: payload[:skip_reason],
            skipped_at: payload[:timestamp]
        }
      else
        step
      end
    end)
  end

  defp apply_event(%Run{} = run, {:run_started, _payload}) do
    # Run is already started when created, this is mostly informational
    run
  end

  defp apply_event(%Run{} = run, {:run_completed, payload}) do
    if status_advances?(run.status, "completed") do
      %{run | status: "completed", output: payload[:output], completed_at: payload[:timestamp]}
    else
      run
    end
  end

  defp apply_event(%Run{} = run, {:run_failed, payload}) do
    if status_advances?(run.status, "failed") do
      %{
        run
        | status: "failed",
          output: %{"error" => payload[:error]},
          failed_at: payload[:timestamp]
      }
    else
      run
    end
  end

  defp apply_event(run, _unknown_event), do: run

  # Subscribes to a run's PubSub topic, loads the snapshot, and assigns it.
  # Cleans up any previous subscription on the same assign key.
  defp subscribe_and_load(socket, run_id, assign_key) do
    config = get_private(socket)

    # Clean up previous subscription on this assign key
    socket =
      case Map.get(config.subscriptions, assign_key) do
        nil -> socket
        old_run_id -> do_unsubscribe(socket, config, old_run_id)
      end

    config = get_private(socket)

    # Subscribe to PubSub
    Phoenix.PubSub.subscribe(config.pubsub, "pgflow:run:#{run_id}")

    # Load snapshot from DB
    run =
      case Client.get_run_with_states(run_id) do
        {:ok, run} -> run
        {:error, _} -> nil
      end

    # Track subscription and assign
    subscriptions = Map.put(config.subscriptions, assign_key, run_id)

    socket
    |> Phoenix.Component.assign(assign_key, run)
    |> put_private(%{config | subscriptions: subscriptions})
  end

  defp do_unsubscribe(socket, config, run_id) do
    Phoenix.PubSub.unsubscribe(config.pubsub, "pgflow:run:#{run_id}")
    socket
  end

  defp find_assign_keys(subscriptions, run_id) do
    subscriptions
    |> Enum.filter(fn {_key, tracked_id} -> tracked_id == run_id end)
    |> Enum.map(fn {key, _} -> key end)
  end

  # Updates a step_state within the run's preloaded step_states list
  defp update_step(%Run{} = run, step_slug, update_fn) do
    step_states =
      case Enum.find_index(run.step_states, &(&1.step_slug == step_slug)) do
        nil ->
          # Unknown step — could be a step not in the snapshot. Ignore.
          run.step_states

        idx ->
          List.update_at(run.step_states, idx, update_fn)
      end

    %{run | step_states: step_states}
  end

  # Status precedence: "created" < "started" < terminal ("completed" | "failed" | "skipped")
  @status_order %{
    "created" => 0,
    "started" => 1,
    "completed" => 2,
    "failed" => 2,
    "skipped" => 2
  }

  defp status_advances?(current, new) do
    Map.get(@status_order, new, -1) > Map.get(@status_order, current, -1)
  end

  defp get_private(socket) do
    socket.private[@private_key] || %{pubsub: nil, subscriptions: %{}}
  end

  defp put_private(socket, config) do
    Phoenix.LiveView.put_private(socket, @private_key, config)
  end
end
