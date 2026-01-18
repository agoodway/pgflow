defmodule PgFlow.FlowCase do
  @moduledoc """
  This module defines the setup for tests that require flow execution.

  Provides helpers for testing PgFlow workflows including starting flows,
  waiting for completion, and asserting on step outputs.
  """

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.{FlowRegistry, Queries}
  alias PgFlow.Schema.{Run, StepState}

  using do
    quote do
      alias PgFlow.TestRepo

      import Ecto
      import Ecto.Query
      import PgFlow.FlowCase
    end
  end

  setup tags do
    pid = Sandbox.start_owner!(PgFlow.TestRepo, shared: not tags[:async])
    on_exit(fn -> Sandbox.stop_owner(pid) end)

    # Register test flows
    if flows = tags[:flows] do
      Enum.each(flows, &FlowRegistry.register/1)
      on_exit(fn -> Enum.each(flows, &FlowRegistry.unregister/1) end)
    end

    :ok
  end

  @doc """
  Starts a flow and returns the run_id.
  """
  def start_flow(flow_module, input) do
    Queries.start_flow(PgFlow.TestRepo, flow_module.__pgflow_slug__(), input)
  end

  @doc """
  Starts a flow and waits for it to complete or fail.

  ## Options

    * `:timeout` - Maximum time to wait in milliseconds (default: 5000)
    * `:poll_interval` - How often to check status (default: 50)

  """
  def run_flow(flow_module, input, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)
    poll_interval = Keyword.get(opts, :poll_interval, 50)

    with {:ok, run_id} <- start_flow(flow_module, input) do
      wait_for_completion(run_id, timeout, poll_interval)
    end
  end

  @doc """
  Waits for a run to reach a terminal state (completed or failed).
  """
  def wait_for_completion(run_id, timeout \\ 5000, poll_interval \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_for_completion(run_id, deadline, poll_interval)
  end

  defp do_wait_for_completion(run_id, deadline, poll_interval) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :timeout}
    else
      case get_run(run_id) do
        {:ok, %{status: "completed"} = run} ->
          {:ok, run}

        {:ok, %{status: "failed"} = run} ->
          {:error, run}

        {:ok, _run} ->
          Process.sleep(poll_interval)
          do_wait_for_completion(run_id, deadline, poll_interval)

        error ->
          error
      end
    end
  end

  @doc """
  Gets a run by ID.
  """
  def get_run(run_id) do
    import Ecto.Query

    case PgFlow.TestRepo.get(Run, run_id) do
      nil -> {:error, :not_found}
      run -> {:ok, run}
    end
  end

  @doc """
  Gets a run with all step states preloaded.
  """
  def get_run_with_states(run_id) do
    import Ecto.Query

    query =
      from(r in Run,
        where: r.run_id == ^run_id,
        preload: [:step_states]
      )

    case PgFlow.TestRepo.one(query) do
      nil -> {:error, :not_found}
      run -> {:ok, run}
    end
  end

  @doc """
  Gets the output of a specific step in a run.
  """
  def get_step_output(run_id, step_slug) do
    import Ecto.Query

    step_slug_str = to_string(step_slug)

    query =
      from(s in StepState,
        where: s.run_id == ^run_id and s.step_slug == ^step_slug_str,
        select: s.output
      )

    case PgFlow.TestRepo.one(query) do
      nil -> {:error, :not_found}
      output -> {:ok, output}
    end
  end

  @doc """
  Asserts that a step produced the expected output.
  """
  def assert_step_output(run_id, step_slug, expected) do
    {:ok, actual} = get_step_output(run_id, step_slug)

    assert actual == expected,
           "Expected step #{step_slug} output to be #{inspect(expected)}, got #{inspect(actual)}"
  end

  @doc """
  Waits for a specific step to reach a terminal state.
  """
  def wait_for_step(run_id, step_slug, timeout \\ 5000, poll_interval \\ 50) do
    import Ecto.Query

    step_slug_str = to_string(step_slug)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_wait_for_step(run_id, step_slug_str, deadline, poll_interval)
  end

  defp do_wait_for_step(run_id, step_slug_str, deadline, poll_interval) do
    import Ecto.Query

    if System.monotonic_time(:millisecond) > deadline do
      {:error, :timeout}
    else
      query =
        from(s in StepState,
          where: s.run_id == ^run_id and s.step_slug == ^step_slug_str
        )

      case PgFlow.TestRepo.one(query) do
        nil ->
          Process.sleep(poll_interval)
          do_wait_for_step(run_id, step_slug_str, deadline, poll_interval)

        %{status: status} = state when status in ["completed", "failed"] ->
          {:ok, state}

        _state ->
          Process.sleep(poll_interval)
          do_wait_for_step(run_id, step_slug_str, deadline, poll_interval)
      end
    end
  end
end
