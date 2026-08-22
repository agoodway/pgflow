defmodule PgFlow.Worker.Server do
  @moduledoc """
  GenServer that polls pgmq and executes flow tasks.

  Each worker is responsible for a single flow/queue and can execute
  multiple tasks concurrently up to the configured limit.

  Implements the two-phase protocol:
  1. pgmq.read() - Reserve messages from pgmq (non-blocking)
  2. start_tasks() - Create step_tasks records and get task details

  ## Delivery Guarantee (at-least-once)

  Task delivery is at-least-once, and skipping does not change that.
  `pgflow.start_tasks` hands out a task only while its step is still `started`,
  and any SQL path that skips a step archives that step's queued/started pgmq
  messages in the same transaction — so a worker cannot start a task for a step
  that was already skipped. What it *can* do is start a task microseconds before
  the skip commits: the step is then recorded as `skipped` while the handler is
  already running, and no worker-side check can prevent that. When the handler
  finishes, `complete_task`/`fail_task` hit their "late callback" guard (the
  step is no longer `started`), mutate nothing, and archive the message.

  Consequences for handler authors:

    * A handler may run for a step the database later reports as `skipped`
      (including retries and stalled-task requeues), so handlers must be
      idempotent and must not treat "my step completed" as implied by the
      handler having run.
    * Side effects that must not happen for a skipped step need their own guard
      (an idempotency key, a conditional write, or a check inside the same
      transaction as the effect) rather than relying on the worker.

  ## Signal Strategies

  The worker supports two signal strategies for detecting new messages:

    * `:polling` - Adaptive jittered exponential backoff (1s → 5s).
      Polls fast when busy, backs off when idle.
    * `:notify` - LISTEN/NOTIFY via pgmq's `enable_notify_insert`.
      Near-instant wake-ups with a 30s fallback poll.

  ## Configuration

  Workers are configured via the `config` parameter passed to `start_link/1`:

      config = %{
        flow_module: MyApp.Flows.ProcessOrder,
        repo: MyApp.Repo,
        worker_id: "550e8400-e29b-41d4-a716-446655440000",
        max_concurrency: 10,
        batch_size: 10,
        signal_strategy: :polling,
        min_poll_interval: 1_000,
        max_poll_interval: 5_000,
        notify_fallback_interval: 30_000
      }

  ## State Structure

  The worker maintains the following state:

    * `flow_module` - The flow module being processed
    * `flow_slug` - String slug for the queue name
    * `worker_id` - UUID for this worker (string format)
    * `repo` - Ecto repo module
    * `task_supervisor` - PID of Task.Supervisor for async execution
    * `active_tasks` - Map of task_ref => task_metadata (includes timeout_timer_ref)
    * `max_concurrency` - Max parallel tasks (default: 10)
    * `batch_size` - Messages per poll (default: 10)
    * `visibility_timeout` - Seconds for message visibility (derived from flow's opt_timeout)
    * `signal_strategy` - `:polling` or `:notify`
    * `signal_state` - Adaptive backoff state for `:polling` strategy
    * `lifecycle` - Worker lifecycle state machine (see `PgFlow.Worker.Lifecycle`)

  ## Lifecycle

  1. **Initialization** - Worker registers itself in the database, starts polling loop
  2. **Polling** - Continuously polls the queue, dispatches tasks to Task.Supervisor
  3. **Task Execution** - Tasks run concurrently, worker tracks completion/failure
  4. **Graceful Shutdown** - Worker stops accepting tasks, waits for active tasks
  5. **Cleanup** - Marks worker as stopped in database

  ## Telemetry Events

  The worker emits the following telemetry events:

    * `[:pgflow, :worker, :start]` - Worker started
    * `[:pgflow, :worker, :stop]` - Worker stopped
    * `[:pgflow, :worker, :poll, :start]` - Poll cycle started
    * `[:pgflow, :worker, :poll, :stop]` - Poll cycle completed
    * `[:pgflow, :worker, :task, :start]` - Task execution started
    * `[:pgflow, :worker, :task, :stop]` - Task execution completed
    * `[:pgflow, :worker, :task, :exception]` - Task execution failed

  """

  use GenServer
  require Logger

  alias PgFlow.Context
  alias PgFlow.Logger, as: PgLogger
  alias PgFlow.Queries.Flows
  alias PgFlow.Queries.Workers, as: WorkerQueries
  alias PgFlow.Telemetry
  alias PgFlow.Worker.Lifecycle
  alias PgFlow.Worker.TaskRow

  # Upper bound on runs whose announced skips this worker remembers. Entries are
  # released as soon as a run goes terminal, so this only ever holds in-flight
  # runs plus any run this worker touched but never saw finish.
  @max_tracked_skip_runs 512

  @type task_metadata :: %{
          run_id: String.t(),
          step_slug: String.t(),
          task_index: non_neg_integer(),
          msg_id: pos_integer(),
          timeout_timer_ref: reference() | nil,
          task_pid: pid() | nil
        }

  @type signal_state :: %{
          current_interval: pos_integer(),
          min_interval: pos_integer(),
          max_interval: pos_integer(),
          poll_timer_ref: reference() | nil
        }

  @type state :: %{
          flow_module: module(),
          flow_slug: String.t(),
          worker_id: String.t(),
          worker_name: String.t(),
          repo: module(),
          task_supervisor: pid(),
          active_tasks: %{reference() => task_metadata()},
          max_concurrency: pos_integer(),
          batch_size: pos_integer(),
          visibility_timeout: pos_integer(),
          signal_strategy: :polling | :notify,
          signal_state: signal_state(),
          notify_fallback_interval: pos_integer(),
          fallback_timer_ref: reference() | nil,
          flow_def: term(),
          lifecycle: Lifecycle.t(),
          emitted_skips: %{String.t() => MapSet.t(String.t())},
          emitted_skip_runs: [String.t()]
        }

  # Client API

  @doc """
  Starts a worker GenServer.

  ## Options

    * `:flow_module` - (required) The flow module to process
    * `:repo` - (required) The Ecto repository module
    * `:worker_id` - (optional) UUID string for worker identification (generated if not provided)
    * `:task_supervisor` - (optional) PID of Task.Supervisor (uses PgFlow.TaskSupervisor if not provided)
    * `:max_concurrency` - (optional) Maximum concurrent tasks (default: 10)
    * `:batch_size` - (optional) Messages to fetch per poll (default: 10)
    * `:signal_strategy` - (optional) Signal strategy, `:polling` or `:notify` (default: `:polling`)
    * `:min_poll_interval` - (optional) Minimum ms between polls (default: 1000)
    * `:max_poll_interval` - (optional) Maximum ms between polls (default: 5000)
    * `:notify_fallback_interval` - (optional) Fallback poll interval for `:notify` strategy (default: 30000)

  """
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(config) do
    GenServer.start_link(__MODULE__, config)
  end

  @doc """
  Gracefully stops the worker.

  The worker will stop accepting new tasks and wait for active tasks to complete.
  """
  @spec stop(pid()) :: :ok
  def stop(pid) do
    GenServer.call(pid, :stop, :infinity)
  end

  @doc """
  Returns the current state of the worker for debugging.
  """
  @spec get_state(pid()) :: map()
  def get_state(pid) do
    GenServer.call(pid, :get_state)
  end

  # Server Callbacks

  @impl true
  def init(config) do
    # Validate required config
    flow_module = Map.fetch!(config, :flow_module)
    repo = Map.fetch!(config, :repo)

    # Get flow definition
    flow_def = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(flow_def.slug)

    # Derive visibility_timeout from flow's opt_timeout (default: 60s)
    visibility_timeout = Keyword.get(flow_def.opts, :timeout, 60)

    # Signal strategy config
    signal_strategy = Map.get(config, :signal_strategy, :polling)
    min_poll_interval = Map.get(config, :min_poll_interval, 1_000)
    max_poll_interval = Map.get(config, :max_poll_interval, 5_000)
    notify_fallback_interval = Map.get(config, :notify_fallback_interval, 30_000)

    # Generate or use provided worker_id
    worker_id = Map.get(config, :worker_id, Ecto.UUID.generate())

    # Generate or use provided worker_name (human-readable identifier for logs)
    worker_name = Map.get(config, :worker_name) || "pgflow-#{flow_slug}"

    # Get or default task supervisor
    task_supervisor = Map.get(config, :task_supervisor, Process.whereis(PgFlow.TaskSupervisor))

    with task_supervisor when is_pid(task_supervisor) <- task_supervisor,
         :ok <- validate_flow_exists(repo, flow_slug, flow_module) do
      # Build signal state for adaptive backoff
      signal_state = %{
        current_interval: min_poll_interval,
        min_interval: min_poll_interval,
        max_interval: max_poll_interval,
        poll_timer_ref: nil
      }

      # Build state
      state = %{
        flow_module: flow_module,
        flow_slug: flow_slug,
        worker_id: worker_id,
        worker_name: worker_name,
        repo: repo,
        task_supervisor: task_supervisor,
        active_tasks: %{},
        max_concurrency: Map.fetch!(config, :max_concurrency),
        batch_size: Map.fetch!(config, :batch_size),
        visibility_timeout: visibility_timeout,
        signal_strategy: signal_strategy,
        signal_state: signal_state,
        notify_fallback_interval: notify_fallback_interval,
        fallback_timer_ref: nil,
        flow_def: flow_def,
        lifecycle:
          Lifecycle.new() |> Lifecycle.transition!(:starting) |> Lifecycle.transition!(:running),
        emitted_skips: %{},
        emitted_skip_runs: []
      }

      # Register worker in database
      case register_worker(state) do
        {:ok, _} ->
          # Log startup banner with flow compilation status
          PgLogger.startup_banner(%{
            worker_name: worker_name,
            worker_id: worker_id,
            queue_name: flow_slug,
            flows: [%{flow_slug: flow_slug, status: :ready}]
          })

          # Emit telemetry event
          emit_telemetry([:worker, :start], %{}, %{
            worker_id: worker_id,
            worker_name: worker_name,
            flow_slug: flow_slug
          })

          # Start signal loop based on strategy
          state = schedule_initial_poll(state)

          {:ok, state}

        {:error, reason} ->
          Logger.error("Failed to register worker #{worker_id}: #{inspect(reason)}")
          {:stop, {:registration_failed, reason}}
      end
    else
      nil -> {:stop, :task_supervisor_not_found}
    end
  end

  @impl true
  def handle_info(:poll, %{lifecycle: lifecycle} = state) do
    if Lifecycle.can_accept_work?(lifecycle) do
      state = do_poll_cycle(state)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:poll_now, %{lifecycle: lifecycle} = state) do
    if Lifecycle.can_accept_work?(lifecycle) do
      # Cancel any pending poll timer to avoid double-polling
      state = cancel_poll_timer(state)
      # Reset fallback timer since we got a NOTIFY (for :notify strategy)
      state = reset_fallback_timer(state)
      state = do_poll_cycle(state)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:fallback_poll, %{lifecycle: lifecycle} = state) do
    if Lifecycle.can_accept_work?(lifecycle) do
      state = do_poll_cycle(state)
      # Reschedule the fallback timer and track the ref
      fallback_ref = Process.send_after(self(), :fallback_poll, state.notify_fallback_interval)
      {:noreply, %{state | fallback_timer_ref: fallback_ref}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    # Task completed successfully
    case Map.pop(state.active_tasks, ref) do
      {nil, _} ->
        # Unknown task reference, ignore
        {:noreply, state}

      {task_meta, new_active_tasks} ->
        # This completion message doubles as the worker's slot-freed signal
        # (the OTP analogue of upstream edge-worker's waitForSlot/notify
        # mechanism) — capture fullness before the task leaves the ledger.
        was_at_capacity = map_size(state.active_tasks) >= state.max_concurrency
        cancel_task_timeout(task_meta)

        state =
          apply_task_result(result, task_meta, new_active_tasks, was_at_capacity, state)

        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    # Task crashed or exited
    case Map.pop(state.active_tasks, ref) do
      {nil, _} ->
        # Unknown task reference, ignore
        {:noreply, state}

      {task_meta, new_active_tasks} ->
        # Cancel the timeout timer
        cancel_task_timeout(task_meta)
        state = handle_task_failure(task_meta, reason, state)
        state = %{state | active_tasks: new_active_tasks}

        # For failures, always poll if we can accept work - retries get re-queued
        # and we want to pick them up promptly
        state =
          if Lifecycle.can_accept_work?(state.lifecycle) do
            schedule_immediate_poll(state)
          else
            state
          end

        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:task_timeout, ref}, state) do
    case Map.pop(state.active_tasks, ref) do
      {nil, _} ->
        # Task already completed, ignore
        {:noreply, state}

      {task_meta, new_active_tasks} ->
        # Terminate the task process gracefully
        if task_meta.task_pid do
          Task.Supervisor.terminate_child(state.task_supervisor, task_meta.task_pid)
        end

        # Resolve the timeout value for the error message
        timeout_seconds = resolve_task_timeout(state, task_meta.step_slug)

        # Report failure
        state =
          handle_task_failure(
            task_meta,
            "Task timed out after #{timeout_seconds}s",
            state
          )

        state = %{state | active_tasks: new_active_tasks}

        # Same rationale as the :DOWN clause: the timeout freed a slot and
        # fail_task may have requeued a retry — poll promptly instead of
        # waiting for the next timer. (The killed task's :DOWN can't cover
        # this: by the time it arrives, its ref is already out of
        # active_tasks and is ignored.)
        state =
          if Lifecycle.can_accept_work?(state.lifecycle) do
            schedule_immediate_poll(state)
          else
            state
          end

        {:noreply, state}
    end
  end

  # Catch-all for messages this worker never subscribed to. Workers routinely
  # end up in some other process's $callers chain (e.g. Task.Supervisor async
  # tasks), and libraries like Swoosh's Test adapter broadcast captured
  # messages to every pid in that chain. Without this clause any such stray
  # message crashes the GenServer with a FunctionClauseError.
  @impl true
  def handle_info(msg, state) do
    Logger.debug("Worker #{state.worker_id} received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, do_stop(state)}
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  # Private function to handle graceful stop
  defp do_stop(state) do
    # Transition to stopping state
    lifecycle = Lifecycle.transition!(state.lifecycle, :stopping)
    state = %{state | lifecycle: lifecycle}

    # Log waiting phase if there are active tasks
    if map_size(state.active_tasks) > 0 do
      PgLogger.shutdown(state.worker_name, :waiting)
    end

    # Wait for all active tasks to complete
    wait_for_tasks(state)

    # Mark worker as stopped in database
    mark_worker_stopped(state)

    # Transition to stopped state
    lifecycle = Lifecycle.transition!(state.lifecycle, :stopped)
    state = %{state | lifecycle: lifecycle}

    # Log stopped phase
    PgLogger.shutdown(state.worker_name, :stopped)

    # Emit telemetry event
    emit_telemetry([:worker, :stop], %{}, %{
      worker_id: state.worker_id,
      worker_name: state.worker_name,
      flow_slug: state.flow_slug
    })

    state
  end

  @impl true
  def terminate(reason, state) do
    Logger.debug("Worker #{state.worker_id} terminating: #{inspect(reason)}")

    # Only mark stopped if do_stop hasn't already done it
    unless Lifecycle.stopped?(state.lifecycle) do
      mark_worker_stopped(state)
    end

    :ok
  end

  # Private Functions

  @spec register_worker(state()) :: {:ok, term()} | {:error, term()}
  defp register_worker(state) do
    function_name = "elixir:#{state.flow_module}"
    WorkerQueries.register_worker(state.repo, state.worker_id, state.flow_slug, function_name)
  end

  @spec mark_worker_stopped(state()) :: :ok
  defp mark_worker_stopped(state) do
    case WorkerQueries.mark_worker_stopped(state.repo, state.worker_id) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to mark worker as stopped: #{inspect(reason)}")
        :ok
    end
  end

  defp validate_flow_exists(repo, flow_slug, flow_module) do
    case Flows.flow_exists?(repo, flow_slug) do
      {:ok, true} ->
        :ok

      {:ok, false} ->
        raise """
        Flow "#{flow_slug}" is not compiled in the database.

        The PGMQ queue for this flow does not exist. Run the migration to compile it:

            mix pgflow.gen.flow_migration #{inspect(flow_module)}
            mix ecto.migrate

        Or if you haven't created the migration yet, generate it first.
        """

      {:error, error} ->
        Logger.warning("Failed to check if flow exists: #{inspect(error)}")
        :ok
    end
  end

  # Schedules the initial poll based on signal strategy
  defp schedule_initial_poll(state) do
    case state.signal_strategy do
      :polling ->
        schedule_next_poll(state, :initial)

      :notify ->
        # For notify strategy, start the fallback timer and do an initial poll
        fallback_ref = Process.send_after(self(), :fallback_poll, state.notify_fallback_interval)
        state = %{state | fallback_timer_ref: fallback_ref}
        # Do an immediate poll to pick up any existing messages
        ref = Process.send_after(self(), :poll, 0)
        put_in(state, [:signal_state, :poll_timer_ref], ref)
    end
  end

  # Schedules the next poll with jittered exponential backoff
  @spec schedule_next_poll(state(), :found_messages | :empty | :initial) :: state()
  defp schedule_next_poll(state, poll_result) do
    signal_state = state.signal_state

    next_interval =
      case poll_result do
        :found_messages ->
          # Reset to fast polling
          signal_state.min_interval

        :initial ->
          # Start with minimum interval
          signal_state.min_interval

        :empty ->
          # Decorrelated jitter: rand_uniform(min, prev * 3), capped at max
          max_jitter = min(signal_state.current_interval * 3, signal_state.max_interval)
          Enum.random(signal_state.min_interval..max(signal_state.min_interval, max_jitter))
      end

    ref = Process.send_after(self(), :poll, next_interval)

    signal_state = %{signal_state | current_interval: next_interval, poll_timer_ref: ref}
    %{state | signal_state: signal_state}
  end

  # Schedules an immediate poll (used when capacity frees up)
  defp schedule_immediate_poll(state) do
    state = cancel_poll_timer(state)
    signal_state = %{state.signal_state | current_interval: state.signal_state.min_interval}
    ref = Process.send_after(self(), :poll, 0)
    %{state | signal_state: %{signal_state | poll_timer_ref: ref}}
  end

  # Cancels the pending poll timer if one exists and flushes any already-sent message
  @spec cancel_poll_timer(state()) :: state()
  defp cancel_poll_timer(state) do
    case state.signal_state.poll_timer_ref do
      nil ->
        state

      ref ->
        Process.cancel_timer(ref)

        # Flush any already-sent :poll message from mailbox
        receive do
          :poll -> :ok
        after
          0 -> :ok
        end

        put_in(state, [:signal_state, :poll_timer_ref], nil)
    end
  end

  # Resets the fallback timer when a NOTIFY is received (for :notify strategy only)
  # This reduces unnecessary polling when NOTIFY is working properly
  @spec reset_fallback_timer(state()) :: state()
  defp reset_fallback_timer(%{signal_strategy: :notify, fallback_timer_ref: ref} = state) do
    if ref do
      Process.cancel_timer(ref)

      # Flush any already-sent :fallback_poll message from mailbox
      receive do
        :fallback_poll -> :ok
      after
        0 -> :ok
      end
    end

    new_ref = Process.send_after(self(), :fallback_poll, state.notify_fallback_interval)
    %{state | fallback_timer_ref: new_ref}
  end

  defp reset_fallback_timer(state), do: state

  # Executes a full poll cycle: read, dispatch, schedule next
  defp do_poll_cycle(state) do
    start_time = System.monotonic_time()

    emit_telemetry([:worker, :poll, :start], %{}, %{
      worker_id: state.worker_id,
      flow_slug: state.flow_slug,
      active_tasks: map_size(state.active_tasks)
    })

    {state, poll_result} = poll_and_dispatch(state)

    duration = System.monotonic_time() - start_time

    emit_telemetry([:worker, :poll, :stop], %{duration: duration}, %{
      worker_id: state.worker_id,
      flow_slug: state.flow_slug,
      active_tasks: map_size(state.active_tasks)
    })

    # Schedule next poll based on strategy
    case state.signal_strategy do
      :polling ->
        schedule_next_poll(state, poll_result)

      :notify ->
        # For notify strategy, don't schedule another poll — wait for notification
        # (the fallback timer handles the safety net)
        state
    end
  end

  @spec poll_and_dispatch(state()) :: {state(), :found_messages | :empty}
  defp poll_and_dispatch(state) do
    # Calculate how many tasks we can accept
    available_slots = state.max_concurrency - map_size(state.active_tasks)

    if available_slots <= 0 do
      # At capacity, don't poll
      {state, :empty}
    else
      # Log polling activity
      PgLogger.polling(state.worker_name)

      # Read messages (non-blocking, limited by available slots and batch size)
      batch_size = min(available_slots, state.batch_size)

      case Flows.read(
             state.repo,
             state.flow_slug,
             state.visibility_timeout,
             batch_size
           ) do
        {:ok, []} ->
          PgLogger.task_count(state.worker_name, 0)
          {state, :empty}

        {:ok, messages} ->
          PgLogger.task_count(state.worker_name, length(messages))
          state = start_and_dispatch_tasks(state, messages)
          {state, :found_messages}

        {:error, reason} ->
          Logger.error("Failed to poll queue #{state.flow_slug}: #{inspect(reason)}")
          {state, :empty}
      end
    end
  end

  @spec start_and_dispatch_tasks(state(), list(list())) :: state()
  defp start_and_dispatch_tasks(state, messages) do
    # Extract message IDs
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)

    # Call start_tasks to create step_tasks records and get task details
    case Flows.start_tasks(state.repo, state.flow_slug, msg_ids, state.worker_id) do
      {:ok, task_details} ->
        # Dispatch each task
        state =
          Enum.reduce(task_details, state, fn task_detail, acc_state ->
            dispatch_task(acc_state, task_detail)
          end)

        reap_undispatched_messages(state, msg_ids, task_details)
        state

      {:error, reason} ->
        # Fail safe: a failed start_tasks means the state of these messages is
        # unknown, so dispatch nothing this cycle. The messages were only made
        # invisible, not consumed, so they redeliver once the visibility timeout
        # expires - far better than running handlers for tasks SQL never started.
        Logger.error("Failed to start tasks for flow #{state.flow_slug}: #{inspect(reason)}")
        state
    end
  end

  # `pgflow.start_tasks` declining a polled message is normally the benign
  # half of a race: the step went terminal between `pgmq.read` and
  # `start_tasks`, and the same SQL transaction that ended the step archived
  # the message (the archive invariant dispatch_task/2 relies on). This is
  # the tripwire for the day a SQL bundle regresses that invariant: a
  # declined message still queued for a terminal step would otherwise
  # redeliver — and be declined — forever, silently. Archive it and say so.
  @spec reap_undispatched_messages(state(), [integer()], [list()]) :: :ok
  defp reap_undispatched_messages(state, polled_msg_ids, task_details) do
    started_ids = MapSet.new(task_details, &TaskRow.decode(&1).msg_id)

    case Enum.reject(polled_msg_ids, &MapSet.member?(started_ids, &1)) do
      [] -> :ok
      declined -> reap_orphaned_messages(state, declined)
    end
  end

  defp reap_orphaned_messages(state, declined_msg_ids) do
    case Flows.orphaned_queue_messages(state.repo, state.flow_slug, declined_msg_ids) do
      {:ok, []} ->
        # Benign race: the messages were archived in the same transaction
        # that ended their steps — exactly what the invariant promises.
        :ok

      {:ok, orphans} ->
        {terminal, in_flight} =
          Enum.split_with(orphans, &(&1.step_status in ["skipped", "completed", "failed"]))

        archive_terminal_orphans(state, terminal)
        warn_in_flight_orphans(state, in_flight)
        :ok

      {:error, reason} ->
        Logger.warning(
          "Failed to probe undispatched messages for flow #{state.flow_slug}: " <>
            "#{inspect(reason)}; leaving them to redeliver"
        )

        :ok
    end
  end

  defp archive_terminal_orphans(_state, []), do: :ok

  defp archive_terminal_orphans(state, orphans) do
    msg_ids = Enum.map(orphans, & &1.msg_id)
    steps = Enum.map(orphans, &{&1.step_slug, &1.step_status})

    Logger.error(
      "pgflow archive invariant violated for flow #{state.flow_slug}: " <>
        "messages #{inspect(msg_ids)} are still queued for terminal steps " <>
        "#{inspect(steps)}; archiving them so they stop redelivering"
    )

    case Flows.archive_messages(state.repo, state.flow_slug, msg_ids) do
      {:ok, _archived} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to archive orphaned messages #{inspect(msg_ids)} for flow " <>
            "#{state.flow_slug}: #{inspect(reason)}; they will redeliver"
        )

        :ok
    end
  end

  defp warn_in_flight_orphans(_state, []), do: :ok

  defp warn_in_flight_orphans(state, orphans) do
    # A still-queued message for a non-terminal step usually means a
    # redelivered message for a task another worker is still executing (its
    # visibility timeout lapsed mid-run). Not ours to archive — the running
    # attempt's complete/fail path owns the message — but worth a warning,
    # since recurring hits mean the visibility timeout is too short.
    details = Enum.map(orphans, &{&1.msg_id, &1.step_slug, &1.step_status})

    Logger.warning(
      "start_tasks declined still-queued messages for flow #{state.flow_slug}: " <>
        "#{inspect(details)}; leaving them to redeliver"
    )
  end

  @spec dispatch_task(state(), list()) :: state()
  defp dispatch_task(state, task_detail) do
    # Row shape and the seven/eight column split live in `TaskRow`:
    # - input: step-specific input (raw element for map, {} for root, deps for dependent)
    # - flow_input: original flow input (only for root non-map steps, NULL otherwise)
    # - attempt: 1-indexed, from attempts_count, which start_tasks increments before dispatch
    %{run_id: run_id_bin} = row = TaskRow.decode(task_detail)

    # Convert binary UUID to string format
    run_id = Ecto.UUID.load!(run_id_bin)

    # No skip check happens here on purpose. `pgflow.start_tasks` only returns a
    # task whose `step_states` row is still `started`, so a task for a skipped
    # step is filtered out before it can reach this function, and every SQL skip
    # path (`_cascade_force_skip_steps`, `fail_task`'s `when_exhausted`) archives
    # that step's queued/started pgmq messages in the same transaction (note:
    # `cascade_resolve_conditions`' condition-skip branch only marks steps never-
    # started, so there are no messages to archive). A
    # per-task `step_skipped?` probe here could therefore only ever observe a
    # skip that committed in the microseconds between `start_tasks` returning
    # and the probe's own query - a window it cannot close anyway, since a skip
    # committing one microsecond later still lets the handler run. See the
    # at-least-once note in this module's docs. If the archive half of that
    # invariant ever regresses in a future SQL bundle,
    # `reap_undispatched_messages/3` catches the fallout at the batch level.
    %{
      step_slug: step_slug,
      input: input,
      msg_id: msg_id,
      task_index: task_index,
      flow_input: flow_input,
      attempt: attempt
    } = row

    # Looked up by the database's string slug. The atom is then taken off the
    # compiled definition rather than built from the row, so a queue carrying an
    # unknown slug cannot add to the atom table on its way to the raise below.
    step_def = get_step_definition(state.flow_module, step_slug)

    unless step_def do
      defined_steps =
        state.flow_module.__pgflow_definition__().steps
        |> Enum.map(& &1.slug)

      raise """
      Step #{inspect(step_slug)} not found in #{inspect(state.flow_module)} definition.

      Steps defined in module: #{inspect(defined_steps)}
      Step slug (queue) from database: #{inspect(step_slug)}

      This usually means the database schema is out of sync with your Elixir code.
      Re-run the migration to sync: mix ecto.migrate
      """
    end

    step_slug_atom = step_def.slug

    # `input` and `flow_input` arrive from jsonb columns, so Postgrex has already
    # decoded them into native Elixir terms. A binary here IS a JSON string value
    # (e.g. a UUID element of a mapped array), not undecoded JSON text - decoding
    # it again would raise on plain strings and silently rewrite ones that happen
    # to parse ("123" -> 123). The TypeScript reference worker decodes at this
    # point because its driver hands back raw JSON text; Postgrex does not.
    #
    # Route input based on step type (matching TypeScript reference pattern):
    # - Map steps: receive raw array element directly
    # - Root steps (no deps): receive flow_input directly
    # - Dependent steps: receive deps object {dep1: val1, dep2: val2, ...}
    handler_input = route_handler_input(step_def, input, flow_input)

    # Get handler function from flow module
    handler = state.flow_module.__pgflow_handler__(step_slug_atom)

    # Build context struct with flow_input available for lazy access
    context = %Context{
      run_id: run_id,
      step_slug: step_slug_atom,
      task_index: task_index,
      attempt: attempt,
      repo: state.repo,
      flow_input: flow_input || :not_loaded,
      flow_slug: state.flow_slug,
      message_id: msg_id
    }

    # Start task under supervisor
    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        start_time = System.monotonic_time()

        emit_telemetry([:worker, :task, :start], %{}, %{
          worker_id: state.worker_id,
          flow_slug: state.flow_slug,
          run_id: run_id,
          step_slug: step_slug,
          task_index: task_index
        })

        try do
          result = handler.(handler_input, context)
          duration = System.monotonic_time() - start_time

          emit_telemetry([:worker, :task, :stop], %{duration: duration}, %{
            worker_id: state.worker_id,
            flow_slug: state.flow_slug,
            run_id: run_id,
            step_slug: step_slug,
            task_index: task_index,
            output: result
          })

          {:ok, result}
        catch
          :throw, {:pgflow_await, :parked} ->
            {:await_parked}

          kind, reason ->
            duration = System.monotonic_time() - start_time
            stacktrace = __STACKTRACE__

            emit_telemetry([:worker, :task, :exception], %{duration: duration}, %{
              worker_id: state.worker_id,
              flow_slug: state.flow_slug,
              run_id: run_id,
              step_slug: step_slug,
              task_index: task_index,
              kind: kind,
              reason: reason
            })

            {:error, Exception.format(kind, reason, stacktrace)}
        end
      end)

    # Schedule task timeout
    timeout_ms = resolve_task_timeout(state, step_slug) * 1_000
    timeout_timer_ref = Process.send_after(self(), {:task_timeout, task.ref}, timeout_ms)

    # Track task with timeout timer and pid
    task_meta = %{
      run_id: run_id,
      step_slug: step_slug,
      task_index: task_index,
      msg_id: msg_id,
      timeout_timer_ref: timeout_timer_ref,
      task_pid: task.pid
    }

    active_tasks = Map.put(state.active_tasks, task.ref, task_meta)
    %{state | active_tasks: active_tasks}
  end

  # Resolves the timeout for a task: step-level override > flow-level > default 60s
  defp resolve_task_timeout(state, step_slug) do
    step_def = get_step_definition(state.flow_module, step_slug)
    flow_timeout = Keyword.get(state.flow_def.opts, :timeout, 60)

    if step_def && step_def.timeout, do: step_def.timeout, else: flow_timeout
  end

  # Cancels a task's timeout timer if it exists and flushes any already-sent message
  defp cancel_task_timeout(%{timeout_timer_ref: ref}) when is_reference(ref) do
    Process.cancel_timer(ref)

    receive do
      {:task_timeout, ^ref} -> :ok
    after
      0 -> :ok
    end
  end

  defp cancel_task_timeout(_task_meta), do: :ok

  # Message already archived by park_waiting_task; do not fail or complete.
  defp apply_task_result({:await_parked}, task_meta, new_active_tasks, _was_at_capacity, state) do
    emit_telemetry([:worker, :task, :waiting], %{}, %{
      worker_id: state.worker_id,
      flow_slug: state.flow_slug,
      run_id: task_meta.run_id,
      step_slug: task_meta.step_slug,
      task_index: task_meta.task_index
    })

    state = %{state | active_tasks: new_active_tasks}

    if Lifecycle.can_accept_work?(state.lifecycle) do
      schedule_immediate_poll(state)
    else
      state
    end
  end

  defp apply_task_result(result, task_meta, new_active_tasks, was_at_capacity, state) do
    state = handle_task_success(task_meta, result, state)
    state = %{state | active_tasks: new_active_tasks}

    # Poll immediately when either:
    # a) the completed step has downstream dependents — complete_task may
    #    enqueue downstream tasks via start_ready_steps, and the NOTIFY
    #    for those inserts may be throttled (pgmq throttle_interval_ms),
    #    so without this the worker would wait for fallback_poll (30s);
    # b) this completion freed a slot on a full worker — queued sibling
    #    tasks (e.g. a map step wider than max_concurrency) sent their
    #    NOTIFYs at enqueue time, so nothing else picks them up before
    #    the fallback timer.
    # A terminal-step completion on a non-full worker still skips the
    # poll: there is no downstream work and no starving sibling.
    has_dependents = step_has_dependents?(state.flow_module, task_meta.step_slug)

    if (has_dependents or was_at_capacity) and Lifecycle.can_accept_work?(state.lifecycle) do
      schedule_immediate_poll(state)
    else
      state
    end
  end

  @spec handle_task_success(task_metadata(), term(), state()) :: state()
  defp handle_task_success(task_meta, {:ok, output}, state) do
    serialized = serialize_handler_output(output)

    case Flows.complete_task(
           state.repo,
           task_meta.run_id,
           task_meta.step_slug,
           task_meta.task_index,
           serialized
         ) do
      {:ok, _} ->
        state = emit_new_skips(state, task_meta.run_id)
        state = emit_run_terminal(state, task_meta.run_id, nil)
        # Delete message from queue ONLY after DB state is confirmed updated.
        # If we delete unconditionally (including on the :error branch below),
        # a transient complete_task failure (deadlock, conn hiccup, serializable
        # rollback) orphans the task: step_tasks row stays un-updated but pgmq
        # message is gone, so no worker can re-pick it up and stalled_recovery
        # can't resurrect it. The step hangs forever waiting on a task that
        # will never run.
        delete_message(state, task_meta.msg_id)
        state

      {:error, reason} ->
        Logger.error(
          "Failed to mark task as completed: #{task_meta.step_slug}[#{task_meta.task_index}] - #{inspect(reason)}"
        )

        # Intentionally DO NOT delete the message. pgmq visibility timeout
        # will expire and the task will be re-delivered. Handlers must be
        # idempotent (the same contract required for pgflow retries on
        # genuine task crashes).
        state
    end
  end

  defp handle_task_success(task_meta, {:error, error_message}, state) do
    handle_task_failure(task_meta, error_message, state)
  end

  defp handle_task_success(task_meta, unexpected_result, state) do
    Logger.warning("Task returned unexpected result format: #{inspect(unexpected_result)}")

    handle_task_failure(
      task_meta,
      "Task returned unexpected result: #{inspect(unexpected_result)}",
      state
    )
  end

  @spec handle_task_failure(task_metadata(), term(), state()) :: state()
  defp handle_task_failure(task_meta, reason, state) do
    error_message =
      case reason do
        :normal -> "Task exited normally without result"
        :shutdown -> "Task was shut down"
        {:shutdown, _} -> "Task was shut down"
        msg when is_binary(msg) -> msg
        other -> inspect(other)
      end

    # Build logging context
    log_ctx = %{
      worker_name: state.worker_name,
      worker_id: state.worker_id,
      flow_slug: state.flow_slug,
      step_slug: task_meta.step_slug,
      run_id: task_meta.run_id,
      task_index: task_meta.task_index,
      msg_id: task_meta.msg_id
    }

    # Don't delete the message on failure - let it be retried via visibility timeout
    case Flows.fail_task(
           state.repo,
           task_meta.run_id,
           task_meta.step_slug,
           task_meta.task_index,
           error_message
         ) do
      {:ok, result} ->
        # Extract retry info from the fail_task result if available
        # The fail_task function returns step_task record with attempts_count and max_attempts
        retry_info = extract_retry_info(result, state)
        PgLogger.task_failed(log_ctx, error_message, retry_info)

        state
        |> emit_new_skips(task_meta.run_id)
        |> emit_run_terminal(task_meta.run_id, error_message)

      {:error, fail_reason} ->
        Logger.error(
          "Failed to mark task as failed: #{task_meta.step_slug}[#{task_meta.task_index}] - #{inspect(fail_reason)}"
        )

        state
    end
  end

  # Extract retry information from fail_task result
  # The result format depends on what pgflow.fail_task returns
  defp extract_retry_info(nil, _state), do: nil

  defp extract_retry_info(result, state) when is_list(result) do
    # pgflow.fail_task returns: (flow_slug, run_id, step_slug, task_index, status, attempts_count, ...)
    # We need attempts_count (index 5) and we can get max_attempts from flow definition
    case result do
      [_flow_slug, _run_id, _step_slug, _task_index, _status, attempts_count | _rest]
      when is_integer(attempts_count) ->
        flow_def = state.flow_def
        max_attempts = Keyword.get(flow_def.opts, :max_attempts, 3)
        base_delay = Keyword.get(flow_def.opts, :base_delay, 1)

        # Calculate next retry delay using exponential backoff
        delay_seconds = base_delay * :math.pow(2, attempts_count - 1)

        %{
          attempt: attempts_count,
          max_attempts: max_attempts,
          delay_seconds: Float.round(delay_seconds, 1)
        }

      _ ->
        nil
    end
  end

  defp extract_retry_info(_, _state), do: nil

  @spec delete_message(state(), pos_integer()) :: :ok
  defp delete_message(state, msg_id) do
    case Flows.delete_message(state.repo, state.flow_slug, msg_id) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to delete message #{msg_id}: #{inspect(reason)}")
        :ok
    end
  end

  # Validates handler output is JSON-serializable before storing.
  # Gracefully falls back to inspected output on serialization failure.
  defp serialize_handler_output(nil), do: %{}

  defp serialize_handler_output(output) when is_map(output) or is_list(output) do
    case Jason.encode(output) do
      {:ok, _} ->
        output

      {:error, reason} ->
        Logger.warning("Handler output not JSON-serializable: #{inspect(reason)}")
        %{"_serialization_error" => inspect(reason), "_raw" => inspect(output)}
    end
  end

  defp serialize_handler_output(output), do: %{"_raw" => inspect(output)}

  # Get step definition from flow module
  # Accepts the slug as either the atom the flow module defined or the string the
  # database stores. Matching the string form against the compiled steps is what
  # lets callers avoid `String.to_atom/1` on a database value: a stale or
  # unrecognized queue row simply finds nothing, rather than growing the atom
  # table on the way to the same conclusion.
  defp get_step_definition(flow_module, step_slug) when is_atom(step_slug) do
    flow_module.__pgflow_definition__().steps
    |> Enum.find(&(&1.slug == step_slug))
  end

  defp get_step_definition(flow_module, step_slug) when is_binary(step_slug) do
    flow_module.__pgflow_definition__().steps
    |> Enum.find(&(Atom.to_string(&1.slug) == step_slug))
  end

  # Check if a step has downstream dependents (other steps that depend on it)
  # Used to determine if we should poll after completing a task - only poll
  # if the completed step might trigger downstream work
  # Compares in string form so a slug that arrived from the database never has to
  # become an atom; `depends_on` holds the compiled atoms, which are safe to render.
  defp step_has_dependents?(flow_module, step_slug) do
    slug = to_string(step_slug)

    flow_module.__pgflow_definition__().steps
    |> Enum.any?(fn step -> Enum.any?(step.depends_on, &(Atom.to_string(&1) == slug)) end)
  end

  # Route handler input based on step type (matching TypeScript reference pattern)
  # See: pgflow-reference/pkgs/edge-worker/src/flow/StepTaskExecutor.ts lines 108-119
  defp route_handler_input(step_def, input_data, flow_input_data) do
    cond do
      # Map steps: receive raw array element directly
      step_def.step_type == :map ->
        input_data

      # Root steps (no dependencies): receive flow_input directly
      Enum.empty?(step_def.depends_on) ->
        flow_input_data

      # Dependent steps: receive deps object {dep1: val1, dep2: val2, ...}
      true ->
        input_data
    end
  end

  @spec wait_for_tasks(state()) :: :ok
  defp wait_for_tasks(%{active_tasks: active_tasks}) when map_size(active_tasks) == 0 do
    :ok
  end

  defp wait_for_tasks(state) do
    receive do
      {ref, result} when is_reference(ref) ->
        # Process the task result during shutdown
        case Map.pop(state.active_tasks, ref) do
          {nil, _} ->
            wait_for_tasks(state)

          {task_meta, new_active_tasks} ->
            cancel_task_timeout(task_meta)
            state = handle_task_success(task_meta, result, state)
            wait_for_tasks(%{state | active_tasks: new_active_tasks})
        end

      {:DOWN, ref, :process, _pid, reason} ->
        # Process the task failure during shutdown
        case Map.pop(state.active_tasks, ref) do
          {nil, _} ->
            wait_for_tasks(state)

          {task_meta, new_active_tasks} ->
            cancel_task_timeout(task_meta)
            state = handle_task_failure(task_meta, reason, state)
            wait_for_tasks(%{state | active_tasks: new_active_tasks})
        end

      {:task_timeout, ref} ->
        case Map.pop(state.active_tasks, ref) do
          {nil, _} ->
            # Already completed, ignore
            wait_for_tasks(state)

          {task_meta, new_active_tasks} ->
            # Terminate the task and report failure (same as normal handler)
            if task_meta.task_pid do
              Task.Supervisor.terminate_child(state.task_supervisor, task_meta.task_pid)
            end

            timeout_seconds = resolve_task_timeout(state, task_meta.step_slug)

            state =
              handle_task_failure(
                task_meta,
                "Task timed out after #{timeout_seconds}s",
                state
              )

            wait_for_tasks(%{state | active_tasks: new_active_tasks})
        end
    after
      30_000 ->
        Logger.warning(
          "Timeout waiting for tasks to complete, #{map_size(state.active_tasks)} tasks still active"
        )

        :ok
    end
  end

  # Announces skips this worker has not announced yet for `run_id`.
  #
  # Skips are decided in SQL, so the only way a worker learns about one is by
  # re-reading step_states after each complete_task/fail_task. Without the
  # per-run seen set, every sweep re-announces every skip on the run, and a
  # handler that counts (rather than one that assigns state, like LiveClient)
  # overcounts by a factor of the number of tasks left in the run.
  @spec emit_new_skips(state(), String.t()) :: state()
  defp emit_new_skips(state, run_id) do
    seen = Map.get(state.emitted_skips, run_id, MapSet.new())
    updated = Telemetry.emit_skipped_steps(state.repo, state.flow_slug, run_id, seen)

    if MapSet.size(updated) == 0 do
      # Runs without skips - the overwhelming majority - stay out of the map.
      state
    else
      remember_skips(state, run_id, updated)
    end
  end

  # Checks the run's status after a task settled and emits the matching run
  # lifecycle event. `error` is nil on the completion path, which is what keeps
  # a run that failed on complete_task (e.g. a map step's TYPE_VIOLATION) from
  # emitting a run:failed with no error to report; that path stays silent, as
  # it was before. Terminal statuses are mutually exclusive, so at most one
  # event fires and a single get_run serves both checks.
  @spec emit_run_terminal(state(), String.t(), String.t() | nil) :: state()
  defp emit_run_terminal(state, run_id, error) do
    case Flows.get_run(state.repo, run_id) do
      {:ok, %{status: "completed", output: output}} ->
        :telemetry.execute(
          [:pgflow, :run, :completed],
          %{system_time: System.system_time()},
          %{flow_slug: state.flow_slug, run_id: run_id, output: output}
        )

        forget_skips(state, run_id)

      {:ok, %{status: "failed"}} ->
        if error do
          :telemetry.execute(
            [:pgflow, :run, :failed],
            %{system_time: System.system_time()},
            %{flow_slug: state.flow_slug, run_id: run_id, error: error}
          )
        end

        forget_skips(state, run_id)

      _ ->
        state
    end
  end

  # Bounds the seen set. Entries are normally dropped the moment the run goes
  # terminal, but a worker in a pool may never process a partially-handled
  # run's final task, so tracked runs are also capped and evicted oldest-first.
  # Evicting a still-live run costs at most a repeat announcement of its skips.
  @spec remember_skips(state(), String.t(), MapSet.t(String.t())) :: state()
  defp remember_skips(state, run_id, seen) do
    if Map.has_key?(state.emitted_skips, run_id) do
      %{state | emitted_skips: Map.put(state.emitted_skips, run_id, seen)}
    else
      runs = [run_id | state.emitted_skip_runs]
      emitted = Map.put(state.emitted_skips, run_id, seen)

      if length(runs) > @max_tracked_skip_runs do
        {kept, evicted} = Enum.split(runs, @max_tracked_skip_runs)
        %{state | emitted_skip_runs: kept, emitted_skips: Map.drop(emitted, evicted)}
      else
        %{state | emitted_skip_runs: runs, emitted_skips: emitted}
      end
    end
  end

  @spec forget_skips(state(), String.t()) :: state()
  defp forget_skips(state, run_id) do
    if Map.has_key?(state.emitted_skips, run_id) do
      %{
        state
        | emitted_skips: Map.delete(state.emitted_skips, run_id),
          emitted_skip_runs: List.delete(state.emitted_skip_runs, run_id)
      }
    else
      state
    end
  end

  @spec emit_telemetry(list(atom()), map(), map()) :: :ok
  defp emit_telemetry(event_name, measurements, metadata) do
    :telemetry.execute([:pgflow] ++ event_name, measurements, metadata)
  end
end
