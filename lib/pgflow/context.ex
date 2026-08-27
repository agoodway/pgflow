defmodule PgFlow.Context do
  @moduledoc """
  Context struct passed to step handler functions.

  The context provides metadata about the current execution environment and
  utilities for accessing flow data.

  ## Fields

    * `:run_id` - UUID of the current flow run
    * `:step_slug` - Slug of the current step being executed
    * `:task_index` - Index of the task within the step (0 for single steps)
    * `:attempt` - Current attempt number (1-indexed)
    * `:flow_input` - Lazy-loaded flow input (use `get_flow_input/1` to access)
    * `:repo` - Ecto repository module for database access
    * `:flow_slug` - Queue/flow slug (set by the worker; optional)
    * `:message_id` - pgmq message id of the in-flight task (set by the worker; optional)

  ## Usage

  Step handlers receive the context as their second argument:

      step :process, depends_on: [:fetch] do
        fn deps, ctx ->
          # Access context fields
          IO.puts("Running step \#{ctx.step_slug} for run \#{ctx.run_id}")
          IO.puts("This is attempt \#{ctx.attempt}")

          # Get flow input if needed
          input = PgFlow.Context.get_flow_input(ctx)

          # Use dependencies from previous steps
          %{result: deps.fetch.data}
        end
      end

  """

  alias PgFlow.Queries.Signals
  alias PgFlow.Schema.Run

  @default_wait_timeout_ms 5_000
  @live_wait_poll_ms 100

  @type t :: %__MODULE__{
          run_id: Ecto.UUID.t(),
          step_slug: atom(),
          task_index: non_neg_integer(),
          attempt: pos_integer(),
          flow_input: map() | :not_loaded,
          repo: module(),
          flow_slug: String.t() | nil,
          message_id: integer() | nil
        }

  @enforce_keys [:run_id, :step_slug, :task_index, :attempt, :repo]
  defstruct [
    :run_id,
    :step_slug,
    :task_index,
    :attempt,
    :repo,
    flow_input: :not_loaded,
    flow_slug: nil,
    message_id: nil
  ]

  @doc """
  Creates a new context struct.

  ## Examples

      ctx = PgFlow.Context.new(
        run_id: "550e8400-e29b-41d4-a716-446655440000",
        step_slug: :process,
        task_index: 0,
        attempt: 1,
        repo: MyApp.Repo
      )

  """
  @spec new(keyword()) :: t()
  def new(opts) do
    struct!(__MODULE__, opts)
  end

  @doc """
  Loads the flow input from the database.

  The flow input is lazily loaded to avoid unnecessary database queries when
  the input is not needed by the step handler.

  Returns the flow input as a map, or raises if the run cannot be found.

  ## Examples

      input = PgFlow.Context.get_flow_input(ctx)
      #=> %{"order_id" => 123, "customer_id" => 456}

  """
  @spec get_flow_input(t()) :: map()
  def get_flow_input(%__MODULE__{flow_input: input}) when is_map(input) do
    input
  end

  def get_flow_input(%__MODULE__{flow_input: :not_loaded, run_id: run_id, repo: repo} = ctx) do
    case repo.get(Run, run_id) do
      nil ->
        raise "Run #{run_id} not found"

      run ->
        input = run.input
        # Cache the loaded input in the context
        loaded_ctx = %{ctx | flow_input: input}
        loaded_ctx.flow_input
    end
  end

  @doc """
  Preloads the flow input into the context.

  This is useful when you want to load the flow input eagerly, such as when
  processing multiple tasks that will all need access to the flow input.

  ## Examples

      ctx = PgFlow.Context.preload_flow_input(ctx)
      # flow_input is now loaded and cached in the context

  """
  @spec preload_flow_input(t()) :: t()
  def preload_flow_input(%__MODULE__{flow_input: input} = ctx) when is_map(input) do
    ctx
  end

  def preload_flow_input(%__MODULE__{run_id: run_id, repo: repo} = ctx) do
    case repo.get(Run, run_id) do
      nil ->
        raise "Run #{run_id} not found"

      run ->
        %{ctx | flow_input: run.input}
    end
  end

  @doc """
  Pauses the current task until `PgFlow.signal/3` delivers a JSON payload.

  If a payload is already buffered for this `run_id` + `step_slug` (+
  `task_index`), it is consumed immediately. Otherwise this live-waits up to
  `:wait_timeout` milliseconds, then atomically consumes or parks the task as
  `waiting` so the worker can free the slot.

  ## Handler and retry contract

  This function requires the worker-issued context passed to a running handler;
  hand-built contexts without the worker's current `message_id` are rejected.

  Parking ends the current handler execution. When the task is resumed, the
  handler starts again from its top, so every effect before this call must be
  idempotent. V1 supports one await point per task.

  Handler code must not catch PgFlow's internal await control throw; doing so allows execution to
  continue after the database has already parked the task.

  Do not call this function inside a caller-owned `Repo.transaction/1` (or
  equivalent transaction): it raises `PgFlow.AwaitSignalTransactionError`.
  The first PostgreSQL-computed `:wait_for` deadline is retained across retries.
  After a signal is accepted, or after the deadline times out, the result is
  replayed on an ordinary handler retry until the task reaches terminal
  completion or failure.

  ## Options

  * `:wait_for` - Total wait budget from first park. `:infinity` (default),
      a positive integer number of seconds, or `{n, unit}` where `unit` is one
      of `:second`, `:seconds`, `:minute`, `:minutes`, `:hour`, `:hours`,
      `:day`, or `:days`.
  * `:wait_timeout` - Milliseconds to block in-process before parking
      (default: `5_000`). `0` parks immediately when the buffer is empty. It
      is polling time only, not a durable wait deadline, and must not exceed
      the handler's configured task timeout.

  ## Returns

    * `{:ok, payload}` - Buffered or live-waited JSON map/list
    * `{:error, :timeout}` - The persisted `:wait_for` deadline expired

  """
  @spec await_signal(t(), keyword()) :: {:ok, map() | list()} | {:error, :timeout}
  def await_signal(%__MODULE__{} = ctx, opts \\ []) when is_list(opts) do
    ensure_not_in_transaction!(ctx.repo)
    ensure_dispatch_identity!(ctx)

    wait_timeout =
      normalize_wait_timeout!(Keyword.get(opts, :wait_timeout, @default_wait_timeout_ms))

    wait_for_seconds = normalize_wait_for!(Keyword.get(opts, :wait_for, :infinity))
    step_slug = to_string(ctx.step_slug)

    case await_once(ctx, step_slug, wait_for_seconds, false) do
      :empty -> live_wait_or_park(ctx, step_slug, wait_timeout, wait_for_seconds)
      outcome -> handle_await_outcome(ctx, outcome)
    end
  end

  defp ensure_not_in_transaction!(repo) do
    if repo.in_transaction?(), do: raise(PgFlow.AwaitSignalTransactionError)
  end

  defp ensure_dispatch_identity!(%__MODULE__{message_id: message_id}) when is_integer(message_id),
    do: :ok

  defp ensure_dispatch_identity!(_ctx) do
    raise ArgumentError,
          "PgFlow.Context.await_signal/2 requires a worker-issued context with a message_id"
  end

  defp normalize_wait_timeout!(value) when is_integer(value) and value >= 0, do: value

  defp normalize_wait_timeout!(_value),
    do: raise(ArgumentError, "wait_timeout must be a non-negative integer number of milliseconds")

  defp normalize_wait_for!(:infinity), do: nil
  defp normalize_wait_for!(seconds) when is_integer(seconds) and seconds > 0, do: seconds

  defp normalize_wait_for!({n, unit})
       when is_integer(n) and n > 0 and unit in [:second, :seconds], do: n

  defp normalize_wait_for!({n, unit})
       when is_integer(n) and n > 0 and unit in [:minute, :minutes],
       do: n * 60

  defp normalize_wait_for!({n, unit}) when is_integer(n) and n > 0 and unit in [:hour, :hours],
    do: n * 3_600

  defp normalize_wait_for!({n, unit}) when is_integer(n) and n > 0 and unit in [:day, :days],
    do: n * 86_400

  defp normalize_wait_for!(_value),
    do:
      raise(
        ArgumentError,
        "wait_for must be :infinity or a positive duration in seconds, minutes, hours, or days"
      )

  defp await_once(ctx, step_slug, wait_for_seconds, park?) do
    Signals.await_task_signal(
      ctx.repo,
      ctx.run_id,
      step_slug,
      ctx.task_index,
      ctx.attempt,
      ctx.message_id,
      wait_for_seconds,
      park?
    )
  end

  defp live_wait_or_park(ctx, step_slug, 0, wait_for_seconds) do
    handle_await_outcome(ctx, await_once(ctx, step_slug, wait_for_seconds, true))
  end

  defp live_wait_or_park(ctx, step_slug, timeout_ms, wait_for_seconds) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    live_wait_loop(ctx, step_slug, deadline, wait_for_seconds)
  end

  defp live_wait_loop(ctx, step_slug, deadline, wait_for_seconds) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      handle_await_outcome(ctx, await_once(ctx, step_slug, wait_for_seconds, true))
    else
      Process.sleep(min(@live_wait_poll_ms, remaining))

      case await_once(ctx, step_slug, wait_for_seconds, false) do
        :empty -> live_wait_loop(ctx, step_slug, deadline, wait_for_seconds)
        outcome -> handle_await_outcome(ctx, outcome)
      end
    end
  end

  defp handle_await_outcome(_ctx, {:ok, payload}), do: {:ok, payload}
  defp handle_await_outcome(_ctx, :timeout), do: {:error, :timeout}
  defp handle_await_outcome(_ctx, :empty), do: :empty

  defp handle_await_outcome(ctx, outcome) when outcome in [:parked, :stale, :terminal] do
    throw({:pgflow_await, outcome, ctx.attempt, ctx.message_id})
  end

  defp handle_await_outcome(_ctx, :missing), do: raise("await_signal task no longer exists")

  defp handle_await_outcome(_ctx, {:error, reason}),
    do: raise("await_signal database error: #{inspect(reason)}")
end
