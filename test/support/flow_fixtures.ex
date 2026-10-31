defmodule PgFlow.Test.FlowFixtures do
  @moduledoc false

  alias PgFlow.TestRepo

  @doc """
  Compiles a flow definition into the database and returns its slug.
  """
  @spec compile_flow(module()) :: String.t()
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
  Starts a run of `flow_slug` with `input` and returns its run id as a string.
  """
  @spec start_flow_run(String.t(), map()) :: String.t()
  defdelegate start_flow_run(flow_slug, input), to: PgFlow.IntegrationCase

  @doc """
  Builds a `PgFlow.Worker.Server` config map for `flow_module`.

  `opts` overrides `:max_concurrency` and `:batch_size` (default `10`),
  `:signal_strategy` (`:polling`), `:min_poll_interval` (`50`),
  `:max_poll_interval` (`5_000`), `:notify_fallback_interval` (`30_000`), and
  `:heartbeat_interval` (`10_000`).
  """
  @spec worker_config(module(), pid() | atom(), keyword()) :: map()
  def worker_config(flow_module, task_supervisor, opts \\ []) do
    %{
      flow_module: flow_module,
      repo: TestRepo,
      task_supervisor: task_supervisor,
      max_concurrency: Keyword.get(opts, :max_concurrency, 10),
      batch_size: Keyword.get(opts, :batch_size, 10),
      signal_strategy: Keyword.get(opts, :signal_strategy, :polling),
      min_poll_interval: Keyword.get(opts, :min_poll_interval, 50),
      max_poll_interval: Keyword.get(opts, :max_poll_interval, 5_000),
      notify_fallback_interval: Keyword.get(opts, :notify_fallback_interval, 30_000),
      heartbeat_interval: Keyword.get(opts, :heartbeat_interval, 10_000)
    }
  end
end
