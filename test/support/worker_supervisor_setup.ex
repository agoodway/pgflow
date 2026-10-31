defmodule PgFlow.Test.WorkerSupervisorSetup do
  @moduledoc false

  import ExUnit.Callbacks

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.TestRepo
  alias PgFlow.WorkerSupervisor

  @doc """
  Compiles a flow definition into the database and returns its slug.
  """
  def compile_flow(flow_module) do
    definition = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    max_attempts = definition.opts[:max_attempts] || 3
    base_delay = definition.opts[:base_delay] || 1
    timeout = definition.opts[:timeout] || 30

    TestRepo.query!(
      "SELECT pgflow.create_flow($1, $2, $3, $4)",
      [flow_slug, max_attempts, base_delay, timeout]
    )

    for step <- definition.steps do
      step_slug = Atom.to_string(step.slug)
      deps = Enum.map(step.depends_on, &Atom.to_string/1)
      step_type = Atom.to_string(step.step_type)

      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)",
        [
          flow_slug,
          step_slug,
          deps,
          step.max_attempts,
          step.base_delay,
          step.timeout,
          step.start_delay,
          step_type
        ]
      )
    end

    flow_slug
  end

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

    on_exit(fn ->
      case WorkerSupervisor.find_worker(flow_module) do
        nil -> :ok
        _ -> WorkerSupervisor.stop_worker(flow_module)
      end

      restore_term(repo_key, previous_repo, missing)
      restore_term(config_key, previous_config, missing)

      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  defp restore_term(key, missing, missing), do: :persistent_term.erase(key)
  defp restore_term(key, previous, _missing), do: :persistent_term.put(key, previous)
end
