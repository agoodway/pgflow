defmodule PgflowDemo.ScenarioCase do
  @moduledoc """
  Shared helpers for real-database scenario execution tests.
  """

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Runs
  alias PgFlow.Schema.Run
  alias PgflowDemo.Repo

  using do
    quote do
      import PgflowDemo.ScenarioCase

      alias PgFlow.Runs
      alias PgflowDemo.Repo
    end
  end

  setup tags do
    skip? = tags[:skip_sandbox] == true or tags[:integration_sandbox] == true

    unless skip? do
      PgflowDemo.DataCase.setup_sandbox(tags)
    end

    :ok
  end

  @doc """
  Uses SQL Sandbox auto mode so PgFlow workers can poll without ownership errors.
  The suite owns auto mode until the application is stopped in test_helper.
  """
  @spec setup_integration_sandbox() :: :ok
  def setup_integration_sandbox do
    Sandbox.mode(PgflowDemo.Repo, :auto)

    :ok
  end

  @doc """
  Gracefully stops demo PgFlow workers to reduce teardown ownership noise.
  """
  @spec stop_pgflow_workers() :: :ok
  def stop_pgflow_workers do
    for module <- pgflow_worker_modules() do
      try do
        _ = PgFlow.stop_worker(module)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @spec pgflow_worker_modules() :: [module()]
  def pgflow_worker_modules do
    [
      PgflowDemo.Flows.ArticleFlow,
      PgflowDemo.Flows.OnboardingFlow,
      PgflowDemo.Flows.ParallelFlow,
      PgflowDemo.Flows.MapFlow,
      PgflowDemo.Flows.RootMapFlow,
      PgflowDemo.Flows.RetryFlow,
      PgflowDemo.Flows.JsonFlow,
      PgflowDemo.Flows.TimeoutFlow,
      PgflowDemo.Flows.DelayedFlow,
      PgflowDemo.Flows.RecoveryFlow,
      PgflowDemo.Flows.QueueIdentityFlow,
      PgflowDemo.Flows.ScheduledFlow,
      PgflowDemo.Flows.Policies.IfMetFlow,
      PgflowDemo.Flows.Policies.IfNotFlow,
      PgflowDemo.Flows.Policies.WhenUnmetSkipFlow,
      PgflowDemo.Flows.Policies.WhenUnmetSkipCascadeFlow,
      PgflowDemo.Flows.Policies.WhenUnmetFailFlow,
      PgflowDemo.Flows.Exhaustion.FailFlow,
      PgflowDemo.Flows.Exhaustion.SkipFlow,
      PgflowDemo.Flows.Exhaustion.SkipCascadeFlow,
      PgflowDemo.Jobs.ArticleFlowCleanup,
      PgflowDemo.Jobs.RecordJob
    ]
  end

  @doc """
  Polls persisted run state until `predicate` returns true or timeout.

  Raises on timeout and includes the last persisted run status in the message.
  """
  @spec await_run(String.t(), (Run.t() -> boolean()), keyword()) :: Run.t()
  def await_run(run_id, predicate, opts \\ []) when is_function(predicate, 1) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 30_000)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, 100)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    await_run_loop(run_id, predicate, deadline, poll_interval_ms, timeout_ms)
  end

  defp await_run_loop(run_id, predicate, deadline, poll_interval_ms, timeout_ms) do
    run = load_run!(run_id)

    if predicate.(run) do
      run
    else
      if System.monotonic_time(:millisecond) >= deadline do
        raise "scenario reconciliation timed out after #{timeout_ms}ms; last status=#{inspect(run.status)}"
      end

      Process.sleep(poll_interval_ms)
      await_run_loop(run_id, predicate, deadline, poll_interval_ms, timeout_ms)
    end
  end

  @doc """
  Waits until the run reaches a terminal persisted status.
  """
  @spec await_terminal_run(String.t(), keyword()) :: Run.t()
  def await_terminal_run(run_id, opts \\ []) do
    await_run(
      run_id,
      fn run -> run.status in ["completed", "failed"] end,
      opts
    )
  end

  @spec load_run!(String.t()) :: Run.t()
  def load_run!(run_id) do
    {:ok, run} = Runs.get_with_states(Repo, run_id)
    run
  end
end
