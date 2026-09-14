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

  alias PgFlow.Schema.Run

  @type flow_input_value :: term()
  @type flow_input :: flow_input_value() | :not_loaded

  @type t :: %__MODULE__{
          run_id: Ecto.UUID.t(),
          step_slug: atom(),
          task_index: non_neg_integer(),
          attempt: pos_integer(),
          flow_input: flow_input(),
          repo: module()
        }

  @enforce_keys [:run_id, :step_slug, :task_index, :attempt, :repo]
  defstruct [:run_id, :step_slug, :task_index, :attempt, :repo, flow_input: :not_loaded]

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
  Returns whether flow input has been loaded into the context.

  Loaded values include JSON `false`, `null`, arrays, and scalars — they are
  distinct from `:not_loaded`, which means the value has not been fetched yet.
  """
  @spec flow_input_loaded?(t()) :: boolean()
  def flow_input_loaded?(%__MODULE__{flow_input: :not_loaded}), do: false
  def flow_input_loaded?(%__MODULE__{}), do: true

  @doc """
  Normalizes a `flow_input` column from SQL into a context field.

  This convenience form treats decoded `nil` as an absent snapshot. Call
  `normalize_flow_input/2` when claim context is available and JSON `null`
  must be distinguished from SQL `NULL`.
  """
  @spec normalize_flow_input(term()) :: flow_input()
  def normalize_flow_input(value), do: normalize_flow_input(value, not is_nil(value))

  @doc """
  Normalizes a claimed `flow_input` using whether the SQL claim included its
  snapshot.

  The explicit indicator preserves JSON `null` as loaded even though Postgrex
  decodes both JSON `null` and SQL `NULL` to `nil`.
  """
  @spec normalize_flow_input(term(), boolean()) :: flow_input()
  def normalize_flow_input(value, true), do: value
  def normalize_flow_input(_value, false), do: :not_loaded

  @doc """
  Loads the flow input from the database.

  The flow input is lazily loaded to avoid unnecessary database queries when
  the input is not needed by the step handler.

  Returns the cached JSON term, or raises if the run cannot be found.

  ## Examples

      input = PgFlow.Context.get_flow_input(ctx)
      #=> %{"order_id" => 123, "customer_id" => 456}

  """
  @spec get_flow_input(t()) :: flow_input_value()
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

  def get_flow_input(%__MODULE__{flow_input: input}) do
    input
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
  def preload_flow_input(%__MODULE__{flow_input: input} = ctx) when input != :not_loaded do
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
end
