defmodule Mix.Tasks.Pgflow.CheckSchema do
  @shortdoc "Verifies pgflow database schema exists and is compatible"

  @moduledoc """
  Verifies that the pgflow database schema exists and is compatible with
  this version of the Elixir implementation.

  ## Usage

      mix pgflow.check_schema [options]

  ## Options

    * `--repo` - The Ecto repo to check. Default: reads from config
    * `--notify-trigger` - `deferred` (default) or `pgmq_default`, matching
      the app's `PgFlow.Config` `:notify_trigger` setting

  ## Examples

      mix pgflow.check_schema
      mix pgflow.check_schema --repo MyApp.Repo
      mix pgflow.check_schema --notify-trigger pgmq_default

  ## What It Checks

    1. The `pgflow` schema and required tables exist
    2. Installed core and helpers versions meet the bundled minimums
    3. Required function signatures: five-argument `start_tasks/5`,
       `ensure_flow_compiled/4`, and no obsolete three-argument claim
    4. Queue identity constraints on `step_tasks` and `steps`
    5. Eight-field `step_task_record` layout including `attempts_count`
    6. Task status enum includes `skipped` and `cancelled`
    7. pgmq extension is available

  When those checks pass, it also prints warnings, without failing, for
  advisory findings:

    * pgmq notify triggers on flow queues (`lower(flow_slug)`) that are not
      `DEFERRABLE INITIALLY DEFERRED`. Workers using
      `signal_strategy: :notify` recreate them deferred on boot; an immediate
      trigger makes producers hold the queue's notify throttle row lock until
      their transaction commits. `pgmq.enable_notify_insert` and pgmq
      upgrades that rebuild notify triggers (`ALTER EXTENSION pgmq UPDATE`,
      for example to 1.13.1) recreate them immediate. Pass
      `--notify-trigger pgmq_default` when the app configures
      `notify_trigger: :pgmq_default` to skip this warning.
    * Notify triggers on flow queues with no `pgmq.notify_insert_throttle`
      row. pgmq keeps that table UNLOGGED, so crash recovery and Neon compute
      restarts empty it, and a trigger without its row sends no NOTIFY until
      a worker boot or `PgFlow.Signal.Notify` restores the row.
    * Notify triggers on partitioned flow queue tables when the installed
      pgmq (before 1.13.1) cannot resolve a partition to its queue, so those
      queues never send NOTIFY.

  See `docs/UPSTREAM_COMPATIBILITY.md` for the full contract.

  """

  use Mix.Task

  alias Mix.Tasks.Pgflow.Helpers
  alias PgFlow.SchemaCheck

  @impl Mix.Task
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string, notify_trigger: :string])

    Mix.Task.run("app.config")

    repo = Helpers.resolve_repo(Keyword.get(opts, :repo))

    Mix.shell().info("Checking pgflow schema in #{inspect(repo)}...")

    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    Helpers.start_repo(repo)

    notify_trigger = get_notify_trigger(opts)

    case SchemaCheck.run(repo) do
      :ok ->
        print_warnings(SchemaCheck.warnings(repo, notify_trigger: notify_trigger))
        Mix.shell().info("\n✓ All checks passed! pgflow schema is compatible.")

      {:error, errors} ->
        Mix.shell().error("\n✗ Schema check failed:")

        Enum.each(errors, fn message ->
          Mix.shell().error("  - #{message}")
        end)

        Mix.raise("pgflow schema is not compatible")
    end
  end

  defp print_warnings([]), do: :ok

  defp print_warnings(warnings) do
    Mix.shell().info("\nWarnings:")
    Enum.each(warnings, &Mix.shell().info("  - #{&1}"))
  end

  defp get_notify_trigger(opts) do
    case Keyword.get(opts, :notify_trigger, "deferred") do
      "deferred" ->
        :deferred

      "pgmq_default" ->
        :pgmq_default

      other ->
        Mix.raise("Unknown --notify-trigger #{inspect(other)}; expected deferred or pgmq_default")
    end
  end
end
