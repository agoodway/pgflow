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
  `:wait_timeout` milliseconds, then parks the task as `waiting` and throws
  `{:pgflow_await, :parked}` so the worker can free the slot.

  ## Options

    * `:wait_for` - Total wait budget from first park. `:infinity` (default),
      an integer number of seconds, or `{n, :seconds | :minutes | :hours | :days}`.
    * `:wait_timeout` - Milliseconds to block in-process before parking
      (default: `5_000`). `0` parks immediately when the buffer is empty.

  ## Returns

    * `{:ok, payload}` - Buffered or live-waited JSON map/list
    * `{:error, :timeout}` - The persisted `:wait_for` deadline expired

  """
  @spec await_signal(t(), keyword()) :: {:ok, map() | list()} | {:error, :timeout}
  def await_signal(%__MODULE__{} = ctx, opts \\ []) when is_list(opts) do
    wait_timeout = Keyword.get(opts, :wait_timeout, @default_wait_timeout_ms)
    wait_for = Keyword.get(opts, :wait_for, :infinity)
    step_slug = to_string(ctx.step_slug)
    task_index = ctx.task_index

    case consume(ctx, step_slug, task_index) do
      {:ok, payload} ->
        {:ok, payload}

      {:error, :timeout} ->
        {:error, :timeout}

      :empty ->
        case live_wait(ctx, step_slug, task_index, wait_timeout) do
          {:ok, payload} -> {:ok, payload}
          {:error, :timeout} -> {:error, :timeout}
          :empty -> park_and_throw(ctx, step_slug, task_index, wait_for)
        end
    end
  end

  defp consume(ctx, step_slug, task_index) do
    case Signals.consume_task_signal(ctx.repo, ctx.run_id, step_slug, task_index) do
      {:ok, payload} -> {:ok, payload}
      {:error, :timeout} -> {:error, :timeout}
      :empty -> :empty
      {:error, err} -> raise "Failed to consume task signal: #{inspect(err)}"
    end
  end

  defp live_wait(_ctx, _step_slug, _task_index, timeout_ms) when timeout_ms <= 0, do: :empty

  defp live_wait(ctx, step_slug, task_index, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    live_wait_loop(ctx, step_slug, task_index, deadline)
  end

  defp live_wait_loop(ctx, step_slug, task_index, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      consume(ctx, step_slug, task_index)
    else
      Process.sleep(min(@live_wait_poll_ms, remaining))

      case consume(ctx, step_slug, task_index) do
        :empty -> live_wait_loop(ctx, step_slug, task_index, deadline)
        other -> other
      end
    end
  end

  defp park_and_throw(ctx, step_slug, task_index, wait_for) do
    deadline = wait_deadline_at(wait_for)

    case Signals.park_waiting_task(ctx.repo, ctx.run_id, step_slug, task_index, deadline) do
      :ok -> throw({:pgflow_await, :parked})
      {:error, err} -> raise "Failed to park waiting task: #{inspect(err)}"
    end
  end

  defp wait_deadline_at(:infinity), do: nil

  defp wait_deadline_at(seconds) when is_integer(seconds),
    do: DateTime.add(DateTime.utc_now(), seconds, :second)

  defp wait_deadline_at({n, unit}) when is_integer(n) and unit in [:second, :seconds],
    do: wait_deadline_at(n)

  defp wait_deadline_at({n, unit}) when is_integer(n) and unit in [:minute, :minutes],
    do: wait_deadline_at(n * 60)

  defp wait_deadline_at({n, unit}) when is_integer(n) and unit in [:hour, :hours],
    do: wait_deadline_at(n * 3600)

  defp wait_deadline_at({n, unit}) when is_integer(n) and unit in [:day, :days],
    do: wait_deadline_at(n * 86_400)
end
