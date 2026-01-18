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

    1. The `pgflow` schema exists
    2. Required tables exist: flows, steps, deps, runs, step_states, step_tasks, workers
    3. Required functions exist: start_flow, complete_task, fail_task, etc.
    4. pgmq extension is available

  """

  use Mix.Task

  @required_tables ~w(flows steps deps runs step_states step_tasks workers)
  @required_functions ~w(start_flow complete_task fail_task read_with_poll start_tasks)

  @impl Mix.Task
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string])

    # Start the application to get config
    Mix.Task.run("app.config")

    repo = get_repo(opts)

    Mix.shell().info("Checking pgflow schema in #{inspect(repo)}...")

    # Start the repo
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = repo.start_link()

    results = [
      check_schema_exists(repo),
      check_tables_exist(repo),
      check_functions_exist(repo),
      check_pgmq_extension(repo)
    ]

    errors = Enum.filter(results, &match?({:error, _}, &1))

    if Enum.empty?(errors) do
      Mix.shell().info("\n✓ All checks passed! pgflow schema is compatible.")
    else
      Mix.shell().error("\n✗ Schema check failed:")

      Enum.each(errors, fn {:error, message} ->
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

  defp check_schema_exists(repo) do
    query = """
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name = 'pgflow'
    """

    case repo.query(query) do
      {:ok, %{num_rows: 1}} ->
        Mix.shell().info("  ✓ pgflow schema exists")
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, "pgflow schema does not exist"}

      {:error, error} ->
        {:error, "Failed to check schema: #{inspect(error)}"}
    end
  end

  defp check_tables_exist(repo) do
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'pgflow'
    """

    case repo.query(query) do
      {:ok, %{rows: rows}} ->
        existing_tables = Enum.map(rows, fn [name] -> name end)
        missing = @required_tables -- existing_tables

        if Enum.empty?(missing) do
          Mix.shell().info("  ✓ All required tables exist")
          :ok
        else
          {:error, "Missing tables: #{Enum.join(missing, ", ")}"}
        end

      {:error, error} ->
        {:error, "Failed to check tables: #{inspect(error)}"}
    end
  end

  defp check_functions_exist(repo) do
    query = """
    SELECT routine_name
    FROM information_schema.routines
    WHERE routine_schema = 'pgflow'
      AND routine_type = 'FUNCTION'
    """

    case repo.query(query) do
      {:ok, %{rows: rows}} ->
        existing_functions = Enum.map(rows, fn [name] -> name end)
        missing = @required_functions -- existing_functions

        if Enum.empty?(missing) do
          Mix.shell().info("  ✓ All required functions exist")
          :ok
        else
          {:error, "Missing functions: #{Enum.join(missing, ", ")}"}
        end

      {:error, error} ->
        {:error, "Failed to check functions: #{inspect(error)}"}
    end
  end

  defp check_pgmq_extension(repo) do
    query = """
    SELECT extname
    FROM pg_extension
    WHERE extname = 'pgmq'
    """

    case repo.query(query) do
      {:ok, %{num_rows: 1}} ->
        Mix.shell().info("  ✓ pgmq extension is installed")
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, "pgmq extension is not installed"}

      {:error, error} ->
        {:error, "Failed to check pgmq extension: #{inspect(error)}"}
    end
  end
end
