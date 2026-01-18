defmodule PgFlow.Worker.Server do
  @moduledoc """
  GenServer that polls pgmq and executes flow tasks.

  Each worker is responsible for a single flow/queue and can execute
  multiple tasks concurrently up to the configured limit.

  Implements the two-phase polling protocol:
  1. read_with_poll() - Reserve messages from pgmq
  2. start_tasks() - Create step_tasks records and get task details

  ## Configuration

  Workers are configured via the `config` parameter passed to `start_link/1`:

      config = %{
        flow_module: MyApp.Flows.ProcessOrder,
        repo: MyApp.Repo,
        worker_id: "550e8400-e29b-41d4-a716-446655440000",
        max_concurrency: 10,
        batch_size: 10,
        poll_interval: 100,
        visibility_timeout: 30,
        heartbeat_interval: 10_000
      }

  ## State Structure

  The worker maintains the following state:

    * `flow_module` - The flow module being processed
    * `flow_slug` - String slug for the queue name
    * `worker_id` - UUID for this worker (string format)
    * `repo` - Ecto repo module
    * `task_supervisor` - PID of Task.Supervisor for async execution
    * `active_tasks` - Map of task_ref => task_metadata
    * `max_concurrency` - Max parallel tasks (default: 10)
    * `batch_size` - Messages per poll (default: 10)
    * `poll_interval` - Milliseconds between polls (default: 100)
    * `visibility_timeout` - Seconds for message visibility (default: 30)
    * `heartbeat_interval` - Milliseconds between heartbeats (default: 10_000)
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

  alias PgFlow.Logger, as: PgLogger
  alias PgFlow.Queries
  alias PgFlow.Worker.Lifecycle

  @type task_metadata :: %{
          run_id: String.t(),
          step_slug: String.t(),
          task_index: non_neg_integer(),
          msg_id: pos_integer()
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
          poll_interval: pos_integer(),
          visibility_timeout: pos_integer(),
          heartbeat_interval: pos_integer(),
          heartbeat_timer: reference() | nil,
          lifecycle: Lifecycle.t()
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
    * `:poll_interval` - (optional) Milliseconds between polls (default: 100)
    * `:visibility_timeout` - (optional) Seconds for message visibility (default: 30)
    * `:heartbeat_interval` - (optional) Milliseconds between heartbeats (default: 10_000)

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

    # Generate or use provided worker_id
    worker_id = Map.get(config, :worker_id, Ecto.UUID.generate())

    # Generate or use provided worker_name (human-readable identifier for logs)
    worker_name = Map.get(config, :worker_name) || "pgflow-#{flow_slug}"

    # Get or default task supervisor
    task_supervisor = Map.get(config, :task_supervisor, Process.whereis(PgFlow.TaskSupervisor))

    unless task_supervisor do
      {:stop, :task_supervisor_not_found}
    end

    # Check if flow exists in database before starting
    case Queries.flow_exists?(repo, flow_slug) do
      {:ok, true} ->
        :ok

      {:ok, false} ->
        raise """
        Flow "#{flow_slug}" is not compiled in the database.

        The PGMQ queue for this flow does not exist. Run the migration to compile it:

            mix pgflow.gen.flow #{inspect(flow_module)}
            mix ecto.migrate

        Or if you haven't created the migration yet, generate it first.
        """

      {:error, error} ->
        Logger.warning("Failed to check if flow exists: #{inspect(error)}")
        :ok
    end

    # Build state
    state = %{
      flow_module: flow_module,
      flow_slug: flow_slug,
      worker_id: worker_id,
      worker_name: worker_name,
      repo: repo,
      task_supervisor: task_supervisor,
      active_tasks: %{},
      max_concurrency: Map.get(config, :max_concurrency, 10),
      batch_size: Map.get(config, :batch_size, 10),
      poll_interval: Map.get(config, :poll_interval, 100),
      visibility_timeout: Map.get(config, :visibility_timeout, 30),
      heartbeat_interval: Map.get(config, :heartbeat_interval, 10_000),
      heartbeat_timer: nil,
      lifecycle:
        Lifecycle.new() |> Lifecycle.transition!(:starting) |> Lifecycle.transition!(:running)
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

        # Start polling loop
        schedule_poll(state)

        # Start heartbeat timer
        heartbeat_timer = schedule_heartbeat(state)

        {:ok, %{state | heartbeat_timer: heartbeat_timer}}

      {:error, reason} ->
        Logger.error("Failed to register worker #{worker_id}: #{inspect(reason)}")
        {:stop, {:registration_failed, reason}}
    end
  end

  @impl true
  def handle_info(:poll, %{lifecycle: lifecycle} = state) do
    if Lifecycle.can_accept_work?(lifecycle) do
      start_time = System.monotonic_time()

      emit_telemetry([:worker, :poll, :start], %{}, %{
        worker_id: state.worker_id,
        flow_slug: state.flow_slug,
        active_tasks: map_size(state.active_tasks)
      })

      state = poll_and_dispatch(state)

      duration = System.monotonic_time() - start_time

      emit_telemetry([:worker, :poll, :stop], %{duration: duration}, %{
        worker_id: state.worker_id,
        flow_slug: state.flow_slug,
        active_tasks: map_size(state.active_tasks)
      })

      # Schedule next poll
      schedule_poll(state)

      {:noreply, state}
    else
      # Don't poll if not running (deprecated, stopping, or stopped)
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:heartbeat, state) do
    # Send heartbeat and check deprecation status in one query
    case Queries.send_heartbeat(state.repo, state.worker_id) do
      {:ok, %{is_deprecated: true}} ->
        send(self(), :deprecated)

      {:ok, %{is_deprecated: false}} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "Failed to send heartbeat for worker #{state.worker_id}: #{inspect(reason)}"
        )
    end

    # Schedule next heartbeat
    heartbeat_timer = schedule_heartbeat(state)

    {:noreply, %{state | heartbeat_timer: heartbeat_timer}}
  end

  @impl true
  def handle_info(:deprecated, state) do
    PgLogger.shutdown(state.worker_name, :deprecating)
    # Transition to deprecated state
    lifecycle = Lifecycle.transition!(state.lifecycle, :deprecated)
    state = %{state | lifecycle: lifecycle}
    # Initiate graceful stop
    {:stop, :normal, do_stop(state)}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    # Task completed successfully
    case Map.pop(state.active_tasks, ref) do
      {nil, _} ->
        # Unknown task reference, ignore
        {:noreply, state}

      {task_meta, new_active_tasks} ->
        handle_task_success(task_meta, result, state)
        {:noreply, %{state | active_tasks: new_active_tasks}}
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
        handle_task_failure(task_meta, reason, state)
        {:noreply, %{state | active_tasks: new_active_tasks}}
    end
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

    # Cancel heartbeat timer
    if state.heartbeat_timer do
      Process.cancel_timer(state.heartbeat_timer)
    end

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

    # Cancel heartbeat timer if still running
    if state.heartbeat_timer do
      Process.cancel_timer(state.heartbeat_timer)
    end

    # Ensure worker is marked as stopped
    mark_worker_stopped(state)

    :ok
  end

  # Private Functions

  @spec register_worker(state()) :: {:ok, term()} | {:error, term()}
  defp register_worker(state) do
    function_name = "elixir:#{state.flow_module}"
    Queries.register_worker(state.repo, state.worker_id, state.flow_slug, function_name)
  end

  @spec mark_worker_stopped(state()) :: :ok
  defp mark_worker_stopped(state) do
    case Queries.mark_worker_stopped(state.repo, state.worker_id) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to mark worker as stopped: #{inspect(reason)}")
        :ok
    end
  end

  @spec schedule_poll(state()) :: reference()
  defp schedule_poll(state) do
    Process.send_after(self(), :poll, state.poll_interval)
  end

  @spec schedule_heartbeat(state()) :: reference()
  defp schedule_heartbeat(state) do
    Process.send_after(self(), :heartbeat, state.heartbeat_interval)
  end

  @spec poll_and_dispatch(state()) :: state()
  defp poll_and_dispatch(state) do
    # Calculate how many tasks we can accept
    available_slots = state.max_concurrency - map_size(state.active_tasks)

    if available_slots <= 0 do
      # At capacity, don't poll
      state
    else
      # Log polling activity
      PgLogger.polling(state.worker_name)

      # Poll for messages (limited by available slots and batch size)
      batch_size = min(available_slots, state.batch_size)

      case Queries.read_with_poll(
             state.repo,
             state.flow_slug,
             state.visibility_timeout,
             batch_size
           ) do
        {:ok, []} ->
          # Log no tasks found
          PgLogger.task_count(state.worker_name, 0)
          state

        {:ok, messages} ->
          # Log task count
          PgLogger.task_count(state.worker_name, length(messages))
          # Phase 2: Start tasks and get details
          start_and_dispatch_tasks(state, messages)

        {:error, reason} ->
          Logger.error("Failed to poll queue #{state.flow_slug}: #{inspect(reason)}")
          state
      end
    end
  end

  @spec start_and_dispatch_tasks(state(), list(list())) :: state()
  defp start_and_dispatch_tasks(state, messages) do
    # Extract message IDs
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)

    # Call start_tasks to create step_tasks records and get task details
    case Queries.start_tasks(state.repo, state.flow_slug, msg_ids, state.worker_id) do
      {:ok, task_details} ->
        # Dispatch each task
        Enum.reduce(task_details, state, fn task_detail, acc_state ->
          dispatch_task(acc_state, task_detail)
        end)

      {:error, reason} ->
        Logger.error("Failed to start tasks for flow #{state.flow_slug}: #{inspect(reason)}")
        state
    end
  end

  @spec dispatch_task(state(), list()) :: state()
  defp dispatch_task(state, task_detail) do
    # Parse task detail row from pgflow.start_tasks:
    # [flow_slug, run_id (binary UUID), step_slug, input, msg_id, task_index, flow_input]
    # - input: step-specific input (raw element for map, {} for root, deps for dependent)
    # - flow_input: original flow input (only for root non-map steps, NULL otherwise)
    [_flow_slug, run_id_bin, step_slug, input, msg_id, task_index, flow_input] = task_detail

    # Convert binary UUID to string format
    run_id = Ecto.UUID.load!(run_id_bin)

    # Get step definition to determine input routing
    step_slug_atom = String.to_existing_atom(step_slug)
    step_def = get_step_definition(state.flow_module, step_slug_atom)

    # Parse inputs - Postgrex returns JSONB as maps/lists/primitives
    input_data = decode_json_if_needed(input)
    flow_input_data = decode_json_if_needed(flow_input)

    # Route input based on step type (matching TypeScript reference pattern):
    # - Map steps: receive raw array element directly
    # - Root steps (no deps): receive flow_input directly
    # - Dependent steps: receive deps object {dep1: val1, dep2: val2, ...}
    handler_input = route_handler_input(step_def, input_data, flow_input_data)

    # Get handler function from flow module
    handler = state.flow_module.__pgflow_handler__(step_slug_atom)

    # Build context with flow_input available for lazy access
    context = %{
      run_id: run_id,
      step_slug: step_slug,
      task_index: task_index,
      flow_input: flow_input_data,
      repo: state.repo
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

    # Track task
    task_meta = %{
      run_id: run_id,
      step_slug: step_slug,
      task_index: task_index,
      msg_id: msg_id
    }

    active_tasks = Map.put(state.active_tasks, task.ref, task_meta)
    %{state | active_tasks: active_tasks}
  end

  @spec handle_task_success(task_metadata(), term(), state()) :: :ok
  defp handle_task_success(task_meta, {:ok, output}, state) do
    case Queries.complete_task(
           state.repo,
           task_meta.run_id,
           task_meta.step_slug,
           task_meta.task_index,
           output || %{}
         ) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to mark task as completed: #{task_meta.step_slug}[#{task_meta.task_index}] - #{inspect(reason)}"
        )
    end

    # Delete message from queue
    delete_message(state, task_meta.msg_id)
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

  @spec handle_task_failure(task_metadata(), term(), state()) :: :ok
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

    case Queries.fail_task(
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

      {:error, fail_reason} ->
        Logger.error(
          "Failed to mark task as failed: #{task_meta.step_slug}[#{task_meta.task_index}] - #{inspect(fail_reason)}"
        )
    end

    # Don't delete message on failure - let it be retried via visibility timeout
    :ok
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
        flow_def = state.flow_module.__pgflow_definition__()
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
    case Queries.delete_message(state.repo, state.flow_slug, msg_id) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to delete message #{msg_id}: #{inspect(reason)}")
        :ok
    end
  end

  # Helper to decode JSON only if needed - Postgrex returns JSONB as native Elixir types
  defp decode_json_if_needed(nil), do: nil
  defp decode_json_if_needed(value) when is_map(value), do: value
  defp decode_json_if_needed(value) when is_list(value), do: value
  defp decode_json_if_needed(value) when is_binary(value), do: Jason.decode!(value)
  # Handle primitives (numbers, booleans) that come from JSONB array elements
  defp decode_json_if_needed(value) when is_number(value), do: value
  defp decode_json_if_needed(value) when is_boolean(value), do: value

  # Get step definition from flow module
  defp get_step_definition(flow_module, step_slug) do
    definition = flow_module.__pgflow_definition__()

    Enum.find(definition.steps, fn step ->
      step.slug == step_slug
    end)
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
            handle_task_success(task_meta, result, state)
            wait_for_tasks(%{state | active_tasks: new_active_tasks})
        end

      {:DOWN, ref, :process, _pid, reason} ->
        # Process the task failure during shutdown
        case Map.pop(state.active_tasks, ref) do
          {nil, _} ->
            wait_for_tasks(state)

          {task_meta, new_active_tasks} ->
            handle_task_failure(task_meta, reason, state)
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

  @spec emit_telemetry(list(atom()), map(), map()) :: :ok
  defp emit_telemetry(event_name, measurements, metadata) do
    :telemetry.execute([:pgflow] ++ event_name, measurements, metadata)
  end
end
