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
    3. Required function signatures: four-argument `start_tasks/4`,
       `ensure_flow_compiled/2`, and no obsolete three-argument claim
    4. Queue identity constraints on `step_tasks` and `steps`
    5. Eight-field `step_task_record` layout including `attempts_count`
    6. Task status enum includes `skipped` and `cancelled`
    7. pgmq extension is available

  See `docs/UPSTREAM_COMPATIBILITY.md` for the full contract.

  """

  use Mix.Task

  alias PgFlow.SchemaCheck

  @impl Mix.Task
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string])

    Mix.Task.run("app.config")

    repo = get_repo(opts)

    Mix.shell().info("Checking pgflow schema in #{inspect(repo)}...")

    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    start_repo(repo)

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

  defp get_repo(opts) do
    case Keyword.get(opts, :repo) do
      nil ->
        case Application.get_env(:pgflow, :ecto_repos, []) do
          [repo | _] -> repo
          [] -> Mix.raise("No repo configured. Use --repo or configure :pgflow, :ecto_repos")
        end

      repo_string ->
        Module.concat([repo_string])
    end
  end

  defp start_repo(repo) do
    case repo.start_link() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      {:error, error} -> raise "Failed to start repo: #{inspect(error)}"
    end
  end
end
