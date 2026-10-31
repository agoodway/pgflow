defmodule PgFlow.Test.WorkerSupervisorSetup do
  @moduledoc false

  import ExUnit.Assertions
  import ExUnit.Callbacks
  import PgFlow.Test.FlowFixtures, only: [compile_flow: 1]

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Test.WaitHelpers
  alias PgFlow.TestRepo
  alias PgFlow.WorkerSupervisor

  @doc """
  Starts a task supervisor and `WorkerSupervisor` with the given `worker_options`
  map, compiles `flow_module`, and registers cleanup. Call from a test `setup`.
  """
  def start_worker_supervisor!(flow_module, worker_options) do
    repo_key = {PgFlow, :repo}
    config_key = {PgFlow, :config}
    missing = make_ref()
    previous_repo = :persistent_term.get(repo_key, missing)
    previous_config = :persistent_term.get(config_key, missing)

    :ok = Sandbox.checkout(TestRepo)
    :ok = Sandbox.mode(TestRepo, {:shared, self()})
    TestRepo.query!("SET LOCAL app.settings.jwt_secret = 'production-secret'")
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    worker_config = [
      repo: TestRepo,
      max_concurrency: 8,
      batch_size: 8,
      signal_strategy: :polling,
      min_poll_interval: 50,
      max_poll_interval: 5_000,
      notify_fallback_interval: 30_000,
      heartbeat_interval: 10_000,
      worker_options: worker_options
    ]

    start_supervised!({Task.Supervisor, name: PgFlow.TaskSupervisor})
    start_supervised!({WorkerSupervisor, worker_config})

    compile_flow(flow_module)

    # ExUnit stops the supervised WorkerSupervisor, and its workers, before
    # on_exit callbacks run.
    on_exit(fn ->
      restore_term(repo_key, previous_repo, missing)
      restore_term(config_key, previous_config, missing)

      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  @doc """
  Kills the registered worker `pid` for `flow_module`, asserts it exits, and
  returns the replacement the supervisor starts.
  """
  @spec kill_and_await_restart!(module(), pid()) :: pid()
  def kill_and_await_restart!(flow_module, pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 5_000
    WaitHelpers.wait_until(fn -> is_pid(WorkerSupervisor.find_worker(flow_module)) end)
    WorkerSupervisor.find_worker(flow_module)
  end

  defp restore_term(key, missing, missing), do: :persistent_term.erase(key)
  defp restore_term(key, previous, _missing), do: :persistent_term.put(key, previous)
end
