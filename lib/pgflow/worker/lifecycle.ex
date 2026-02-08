defmodule PgFlow.Worker.Lifecycle do
  @moduledoc """
  Worker lifecycle state machine.

  Manages worker state transitions following the same pattern as the
  TypeScript/Deno pgflow WorkerState. Validates that only allowed
  transitions occur.

  ## States

    * `:created` - Worker has been created but not yet started
    * `:starting` - Worker is starting but not yet processing messages
    * `:running` - Worker is actively processing messages
    * `:stopping` - Worker stopped processing, releasing resources
    * `:stopped` - Worker has stopped and released resources (terminal state)

  ## State Transitions

  ```
  created -> starting -> running -> stopping -> stopped
  ```

  ## Usage

      lifecycle = Lifecycle.new()
      {:ok, lifecycle} = Lifecycle.transition(lifecycle, :starting)
      {:ok, lifecycle} = Lifecycle.transition(lifecycle, :running)

      Lifecycle.running?(lifecycle)
      #=> true

  """

  @type state :: :created | :starting | :running | :stopping | :stopped

  @type t :: %__MODULE__{
          state: state()
        }

  @enforce_keys [:state]
  defstruct [:state]

  # Valid state transitions
  @transitions %{
    created: [:starting],
    starting: [:running],
    running: [:stopping],
    stopping: [:stopped],
    stopped: []
  }

  @doc """
  Creates a new lifecycle in the `:created` state.

  ## Examples

      lifecycle = Lifecycle.new()
      lifecycle.state
      #=> :created

  """
  @spec new() :: t()
  def new do
    %__MODULE__{state: :created}
  end

  @doc """
  Returns the current state.

  ## Examples

      Lifecycle.current(lifecycle)
      #=> :running

  """
  @spec current(t()) :: state()
  def current(%__MODULE__{state: state}), do: state

  @doc """
  Transitions to a new state if the transition is valid.

  Returns `{:ok, lifecycle}` if the transition is valid,
  or `{:error, reason}` if the transition is not allowed.

  ## Examples

      {:ok, lifecycle} = Lifecycle.transition(lifecycle, :starting)
      {:error, {:invalid_transition, :created, :running}} = Lifecycle.transition(lifecycle, :running)

  """
  @spec transition(t(), state()) :: {:ok, t()} | {:error, {:invalid_transition, state(), state()}}
  def transition(%__MODULE__{state: current} = lifecycle, new_state) do
    # Same state is a no-op
    if current == new_state do
      {:ok, lifecycle}
    else
      allowed = Map.get(@transitions, current, [])

      if new_state in allowed do
        {:ok, %{lifecycle | state: new_state}}
      else
        {:error, {:invalid_transition, current, new_state}}
      end
    end
  end

  @doc """
  Transitions to a new state, raising on invalid transition.

  ## Examples

      lifecycle = Lifecycle.transition!(lifecycle, :starting)

  """
  @spec transition!(t(), state()) :: t()
  def transition!(lifecycle, new_state) do
    case transition(lifecycle, new_state) do
      {:ok, new_lifecycle} ->
        new_lifecycle

      {:error, {:invalid_transition, from, to}} ->
        raise ArgumentError, "Cannot transition from #{from} to #{to}"
    end
  end

  @doc "Returns true if in `:running` state."
  @spec running?(t()) :: boolean()
  def running?(%__MODULE__{state: :running}), do: true
  def running?(_), do: false

  @doc "Returns true if in `:stopped` state."
  @spec stopped?(t()) :: boolean()
  def stopped?(%__MODULE__{state: :stopped}), do: true
  def stopped?(_), do: false

  @doc """
  Returns true if the worker can accept new work.

  A worker can accept work only when in the `:running` state.
  """
  @spec can_accept_work?(t()) :: boolean()
  def can_accept_work?(%__MODULE__{state: :running}), do: true
  def can_accept_work?(_), do: false
end
