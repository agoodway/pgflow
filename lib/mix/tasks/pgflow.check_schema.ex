defmodule Mix.Tasks.Pgflow.CheckSchema do
  @shortdoc "Verifies pgflow database schema exists and is compatible"

  @moduledoc """
  Verifies that the pgflow database schema exists and is compatible with
  this version of the Elixir implementation.

  ## Usage

      mix pgflow.check_schema [options]

  ## Options

    * `--repo` - The Ecto repo to check. Default: reads from config

  ## Examples

      mix pgflow.check_schema
      mix pgflow.check_schema --repo MyApp.Repo

  ## What It Checks

    1. The `pgflow` schema and required tables exist
    2. Installed core and helpers versions meet the bundled minimums
    3. Required function signatures: five-argument `start_tasks/5`,
       `ensure_flow_compiled/4`, and no obsolete three-argument claim
    4. Queue identity constraints on `step_tasks` and `steps`
    5. Eight-field `step_task_record` layout including `attempts_count`
    6. Task status enum includes `skipped` and `cancelled`
    7. pgmq extension is available

  See `docs/UPSTREAM_COMPATIBILITY.md` for the full contract.

  """

  use Mix.Task

  alias Mix.Tasks.Pgflow.Helpers
  alias PgFlow.SchemaCheck

  @impl Mix.Task
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string])

    Mix.Task.run("app.config")

    repo = Helpers.configured_repo(opts)

    Mix.shell().info("Checking pgflow schema in #{inspect(repo)}...")

    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    Helpers.start_repo(repo)

    case SchemaCheck.run(repo) do
      :ok ->
        Mix.shell().info("\n✓ All checks passed! pgflow schema is compatible.")

      {:error, errors} ->
        Mix.shell().error("\n✗ Schema check failed:")

        Enum.each(errors, fn message ->
          Mix.shell().error("  - #{message}")
        end)

        Mix.raise("pgflow schema is not compatible")
    end
  end
end
