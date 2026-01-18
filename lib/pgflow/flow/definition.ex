defmodule PgFlow.Flow.Definition do
  @moduledoc """
  Represents a compiled flow definition.

  A flow definition contains all the information needed to execute a workflow:
  - A unique slug identifier
  - The module that defined it
  - Flow-level configuration options
  - An ordered list of steps with their dependencies

  Flow definitions are validated at compile time to ensure:
  - No circular dependencies exist
  - All step dependencies reference existing steps
  - Map steps have at most one dependency

  ## Examples

      %PgFlow.Flow.Definition{
        slug: :user_onboarding,
        module: MyApp.Flows.UserOnboarding,
        opts: [max_attempts: 3, base_delay: 1000, timeout: 30_000],
        steps: [
          %PgFlow.Flow.Step{slug: :create_account},
          %PgFlow.Flow.Step{slug: :send_welcome_email, depends_on: [:create_account]}
        ]
      }
  """

  alias PgFlow.Flow.Step

  @enforce_keys [:slug, :module, :steps]
  defstruct [
    :slug,
    :module,
    opts: [],
    steps: []
  ]

  @type t :: %__MODULE__{
          slug: atom(),
          module: module(),
          opts: keyword(),
          steps: [Step.t()]
        }

  @doc """
  Converts a flow slug (atom) to a string for database storage.

  ## Examples

      iex> PgFlow.Flow.Definition.slug_to_string(:user_onboarding)
      "user_onboarding"

      iex> PgFlow.Flow.Definition.slug_to_string(:payment_processing)
      "payment_processing"
  """
  @spec slug_to_string(t() | atom()) :: String.t()
  def slug_to_string(%__MODULE__{slug: slug}), do: Atom.to_string(slug)
  def slug_to_string(slug) when is_atom(slug), do: Atom.to_string(slug)

  @doc """
  Converts raw step tuples from the DSL into Step structs.

  ## Examples

      iex> PgFlow.Flow.Definition.build_steps([:fetch_user, {:send_email, depends_on: [:fetch_user]}])
      [
        %PgFlow.Flow.Step{slug: :fetch_user, step_type: :single, depends_on: []},
        %PgFlow.Flow.Step{slug: :send_email, step_type: :single, depends_on: [:fetch_user]}
      ]
  """
  @spec build_steps([atom() | {atom(), keyword()}]) :: [Step.t()]
  def build_steps(step_tuples) when is_list(step_tuples) do
    Enum.map(step_tuples, &Step.from_tuple/1)
  end

  @doc """
  Validates a flow definition.

  Performs the following checks:
  1. No circular dependencies exist in the step graph
  2. All step dependencies reference existing steps
  3. Map steps have at most one dependency

  Returns `{:ok, definition}` if valid, or `{:error, reason}` if invalid.

  ## Examples

      iex> definition = %PgFlow.Flow.Definition{
      ...>   slug: :test_flow,
      ...>   module: TestFlow,
      ...>   steps: [
      ...>     %PgFlow.Flow.Step{slug: :step1},
      ...>     %PgFlow.Flow.Step{slug: :step2, depends_on: [:step1]}
      ...>   ]
      ...> }
      iex> PgFlow.Flow.Definition.validate(definition)
      {:ok, definition}

      iex> definition = %PgFlow.Flow.Definition{
      ...>   slug: :test_flow,
      ...>   module: TestFlow,
      ...>   steps: [
      ...>     %PgFlow.Flow.Step{slug: :step1, depends_on: [:step2]},
      ...>     %PgFlow.Flow.Step{slug: :step2, depends_on: [:step1]}
      ...>   ]
      ...> }
      iex> PgFlow.Flow.Definition.validate(definition)
      {:error, "Circular dependency detected in flow :test_flow"}
  """
  @spec validate(t()) :: {:ok, t()} | {:error, String.t()}
  def validate(%__MODULE__{} = definition) do
    # Check dependencies exist first, before cycle detection
    with :ok <- validate_dependencies_exist(definition),
         :ok <- validate_no_cycles(definition),
         :ok <- validate_map_step_constraints(definition) do
      {:ok, definition}
    end
  end

  @doc """
  Validates that the flow has no circular dependencies.

  Uses Kahn's algorithm for topological sorting to detect cycles.
  If a topological sort cannot be completed, a cycle exists.

  ## Examples

      iex> definition = %PgFlow.Flow.Definition{
      ...>   slug: :test_flow,
      ...>   module: TestFlow,
      ...>   steps: [
      ...>     %PgFlow.Flow.Step{slug: :a, depends_on: [:b]},
      ...>     %PgFlow.Flow.Step{slug: :b, depends_on: [:a]}
      ...>   ]
      ...> }
      iex> PgFlow.Flow.Definition.validate_no_cycles(definition)
      {:error, "Circular dependency detected in flow :test_flow"}
  """
  @spec validate_no_cycles(t()) :: :ok | {:error, String.t()}
  def validate_no_cycles(%__MODULE__{slug: flow_slug, steps: steps}) do
    # Build adjacency list and in-degree map
    step_map = Map.new(steps, fn step -> {step.slug, step} end)
    in_degree = Map.new(steps, fn step -> {step.slug, length(step.depends_on)} end)

    # Kahn's algorithm: start with nodes that have no incoming edges
    queue =
      steps
      |> Enum.filter(fn step -> step.depends_on == [] end)
      |> Enum.map(fn step -> step.slug end)

    case topological_sort(queue, step_map, in_degree, []) do
      {:ok, sorted} ->
        if length(sorted) == length(steps) do
          :ok
        else
          {:error, "Circular dependency detected in flow :#{flow_slug}"}
        end

      {:error, _} = error ->
        error
    end
  end

  # Performs topological sort using Kahn's algorithm
  @spec topological_sort([atom()], map(), map(), [atom()]) ::
          {:ok, [atom()]} | {:error, String.t()}
  defp topological_sort([], _step_map, _in_degree, sorted) do
    {:ok, Enum.reverse(sorted)}
  end

  defp topological_sort([current | rest], step_map, in_degree, sorted) do
    # Add current to sorted list
    new_sorted = [current | sorted]

    # Find all steps that depend on current
    dependents =
      step_map
      |> Enum.filter(fn {_slug, step} -> current in step.depends_on end)
      |> Enum.map(fn {slug, _step} -> slug end)

    # Reduce in-degree for dependents
    {new_queue, new_in_degree} =
      Enum.reduce(dependents, {rest, in_degree}, fn dependent, {queue_acc, degree_acc} ->
        new_degree = Map.update!(degree_acc, dependent, &(&1 - 1))

        if new_degree[dependent] == 0 do
          {[dependent | queue_acc], new_degree}
        else
          {queue_acc, new_degree}
        end
      end)

    topological_sort(new_queue, step_map, new_in_degree, new_sorted)
  end

  @doc """
  Validates that all step dependencies reference existing steps.

  ## Examples

      iex> definition = %PgFlow.Flow.Definition{
      ...>   slug: :test_flow,
      ...>   module: TestFlow,
      ...>   steps: [
      ...>     %PgFlow.Flow.Step{slug: :step1, depends_on: [:nonexistent]}
      ...>   ]
      ...> }
      iex> PgFlow.Flow.Definition.validate_dependencies_exist(definition)
      {:error, "Step :step1 in flow :test_flow depends on non-existent step :nonexistent"}
  """
  @spec validate_dependencies_exist(t()) :: :ok | {:error, String.t()}
  def validate_dependencies_exist(%__MODULE__{slug: flow_slug, steps: steps}) do
    step_slugs = MapSet.new(steps, fn step -> step.slug end)

    invalid_dependencies =
      Enum.flat_map(steps, fn step ->
        Enum.filter(step.depends_on, fn dep ->
          not MapSet.member?(step_slugs, dep)
        end)
        |> Enum.map(fn dep -> {step.slug, dep} end)
      end)

    case invalid_dependencies do
      [] ->
        :ok

      [{step_slug, invalid_dep} | _] ->
        {:error,
         "Step :#{step_slug} in flow :#{flow_slug} depends on non-existent step :#{invalid_dep}"}
    end
  end

  @doc """
  Validates that map steps have at most one dependency.

  Map steps process arrays from a single source, so they can only depend on one step.

  ## Examples

      iex> definition = %PgFlow.Flow.Definition{
      ...>   slug: :test_flow,
      ...>   module: TestFlow,
      ...>   steps: [
      ...>     %PgFlow.Flow.Step{
      ...>       slug: :process_items,
      ...>       step_type: :map,
      ...>       depends_on: [:step1, :step2]
      ...>     }
      ...>   ]
      ...> }
      iex> PgFlow.Flow.Definition.validate_map_step_constraints(definition)
      {:error, "Map step :process_items in flow :test_flow must have exactly one dependency (the array source), but has 2"}
  """
  @spec validate_map_step_constraints(t()) :: :ok | {:error, String.t()}
  def validate_map_step_constraints(%__MODULE__{slug: flow_slug, steps: steps}) do
    invalid_map_steps =
      Enum.filter(steps, fn step ->
        step.step_type == :map and length(step.depends_on) != 1
      end)

    case invalid_map_steps do
      [] ->
        :ok

      [step | _] ->
        dep_count = length(step.depends_on)

        {:error,
         "Map step :#{step.slug} in flow :#{flow_slug} must have exactly one dependency (the array source), but has #{dep_count}"}
    end
  end
end
