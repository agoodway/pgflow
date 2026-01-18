defmodule PgFlow.Flow.Step do
  @moduledoc """
  Represents a single step in a flow definition.

  A step is the basic unit of work in a PgFlow workflow. Each step has:
  - A unique slug identifier
  - A type (`:single` or `:map`)
  - Optional dependencies on other steps
  - Optional retry configuration (max_attempts, base_delay, timeout)
  - Optional start delay

  ## Step Types

  - `:single` - Executes once with the flow's input data
  - `:map` - Executes multiple times, once for each item in an array from a dependency

  ## Examples

      # Simple step with no dependencies
      %PgFlow.Flow.Step{
        slug: :fetch_user,
        step_type: :single
      }

      # Step with dependencies and custom retry settings
      %PgFlow.Flow.Step{
        slug: :send_email,
        step_type: :single,
        depends_on: [:fetch_user, :prepare_template],
        max_attempts: 5,
        base_delay: 2000
      }

      # Map step that processes an array
      %PgFlow.Flow.Step{
        slug: :process_items,
        step_type: :map,
        depends_on: [:fetch_items]
      }
  """

  @enforce_keys [:slug]
  defstruct [
    :slug,
    :max_attempts,
    :base_delay,
    :timeout,
    :start_delay,
    step_type: :single,
    depends_on: []
  ]

  @type step_type :: :single | :map

  @type t :: %__MODULE__{
          slug: atom(),
          step_type: step_type(),
          depends_on: [atom()],
          max_attempts: pos_integer() | nil,
          base_delay: pos_integer() | nil,
          timeout: pos_integer() | nil,
          start_delay: pos_integer() | nil
        }

  @doc """
  Converts a step slug (atom) to a string for database storage.

  ## Examples

      iex> PgFlow.Flow.Step.slug_to_string(:fetch_user)
      "fetch_user"

      iex> PgFlow.Flow.Step.slug_to_string(:send_email)
      "send_email"
  """
  @spec slug_to_string(atom()) :: String.t()
  def slug_to_string(slug) when is_atom(slug) do
    Atom.to_string(slug)
  end

  @doc """
  Converts a step tuple from the DSL into a Step struct.

  Accepts tuples in the format:
  - `{name, opts}` - Step with options
  - `name` - Step with just a slug (converted to tuple internally)

  ## Options

  - `:depends_on` - List of step slugs this step depends on (default: `[]`)
  - `:step_type` - Either `:single` or `:map` (default: `:single`)
  - `:max_attempts` - Maximum retry attempts (default: flow default)
  - `:base_delay` - Base delay in milliseconds for exponential backoff (default: flow default)
  - `:timeout` - Timeout in milliseconds (default: flow default)
  - `:start_delay` - Delay in milliseconds before starting (default: `nil`)

  ## Examples

      iex> PgFlow.Flow.Step.from_tuple(:fetch_user)
      %PgFlow.Flow.Step{slug: :fetch_user, step_type: :single, depends_on: []}

      iex> PgFlow.Flow.Step.from_tuple({:send_email, depends_on: [:fetch_user]})
      %PgFlow.Flow.Step{
        slug: :send_email,
        step_type: :single,
        depends_on: [:fetch_user]
      }

      iex> PgFlow.Flow.Step.from_tuple({:process_items, step_type: :map, depends_on: [:fetch_items]})
      %PgFlow.Flow.Step{
        slug: :process_items,
        step_type: :map,
        depends_on: [:fetch_items]
      }
  """
  @spec from_tuple(atom() | {atom(), keyword()}) :: t()
  def from_tuple(slug) when is_atom(slug) do
    from_tuple({slug, []})
  end

  def from_tuple({slug, opts}) when is_atom(slug) and is_list(opts) do
    %__MODULE__{
      slug: slug,
      step_type: Keyword.get(opts, :step_type, :single),
      depends_on: Keyword.get(opts, :depends_on, []),
      max_attempts: Keyword.get(opts, :max_attempts),
      base_delay: Keyword.get(opts, :base_delay),
      timeout: Keyword.get(opts, :timeout),
      start_delay: Keyword.get(opts, :start_delay)
    }
  end
end
