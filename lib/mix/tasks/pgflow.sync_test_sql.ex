defmodule Mix.Tasks.Pgflow.SyncTestSql do
  @shortdoc "Downloads pgflow SQL from GitHub for test database setup"

  @moduledoc """
  Downloads the pgflow SQL files from the official GitHub repository to set up
  the test database exactly like the TypeScript/Deno reference implementation.

  Downloads and combines:
  1. supabase-baseline-schema.sql - Supabase schemas (realtime, vault, etc.)
  2. All migration SQL files from pkgs/core/supabase/migrations
  3. seed.sql - Test helper functions (pgflow_tests schema)

  ## Usage

      mix pgflow.sync_test_sql [options]

  ## Options

    * `--branch` - Git branch to download from. Default: `main`
    * `--force` - Re-download even if files exist

  ## Output

  Writes to `test/support/db/pgflow.sql` - a single combined SQL file
  that can be loaded by Docker's entrypoint.
  """

  use Mix.Task

  @github_raw "https://raw.githubusercontent.com/pgflow-dev/pgflow"
  @github_api "https://api.github.com/repos/pgflow-dev/pgflow/contents/pkgs/core/supabase/migrations"
  @output_dir "test/support/db"

  @impl Mix.Task
  def run(args) do
    {:ok, _} = Application.ensure_all_started(:inets)
    {:ok, _} = Application.ensure_all_started(:ssl)

    {opts, _, _} = OptionParser.parse(args, switches: [branch: :string, force: :boolean])

    branch = Keyword.get(opts, :branch, "main")
    force? = Keyword.get(opts, :force, false)

    output_dir = Path.join(File.cwd!(), @output_dir)
    File.mkdir_p!(output_dir)

    output_path = Path.join(output_dir, "pgflow.sql")

    if not force? and File.exists?(output_path) do
      Mix.shell().info("pgflow.sql already exists. Use --force to re-download.")
      :ok
    else
      Mix.shell().info("Syncing test SQL from GitHub (branch: #{branch})...")

      content = build_combined_sql(branch)

      File.write!(output_path, content)
      Mix.shell().info("\nWritten to: #{output_path}")
    end
  end

  defp build_combined_sql(branch) do
    [
      "-- Combined pgflow SQL for testing",
      "-- Generated on #{DateTime.utc_now()}",
      "-- Branch: #{branch}",
      "",
      download_baseline_schema(branch),
      download_migrations(branch),
      download_seed(branch),
      "-- Configure local JWT secret for is_local() detection",
      "ALTER DATABASE pgflow_test SET app.settings.jwt_secret = 'super-secret-jwt-token-with-at-least-32-characters-long';",
      ""
    ]
    |> Enum.join("\n")
  end

  defp download_baseline_schema(branch) do
    Mix.shell().info("\nDownloading supabase-baseline-schema.sql...")

    url = "#{@github_raw}/#{branch}/pkgs/core/atlas/supabase-baseline-schema.sql"

    case fetch_content(url) do
      {:ok, content} ->
        Mix.shell().info("  -> Downloaded baseline schema")

        """
        -- Supabase baseline schema (realtime, vault, etc.)
        #{content}
        """

      {:error, reason} ->
        Mix.raise("Failed to download baseline schema: #{reason}")
    end
  end

  defp download_migrations(branch) do
    Mix.shell().info("\nFetching migration list...")

    api_url = "#{@github_api}?ref=#{branch}"

    files =
      case fetch_json(api_url) do
        {:ok, files} when is_list(files) ->
          files
          |> Enum.filter(&String.ends_with?(&1["name"] || "", ".sql"))
          |> Enum.sort_by(& &1["name"])

        {:error, reason} ->
          Mix.raise("Failed to fetch migration list: #{reason}")
      end

    if Enum.empty?(files) do
      Mix.raise("No SQL migration files found")
    end

    Mix.shell().info("Found #{length(files)} migration files")

    Enum.map_join(files, "\n", fn file ->
      filename = file["name"]
      raw_url = "#{@github_raw}/#{branch}/pkgs/core/supabase/migrations/#{filename}"

      Mix.shell().info("  Downloading: #{filename}")

      case fetch_content(raw_url) do
        {:ok, sql} ->
          """
          -- Migration: #{filename}
          #{sql}
          """

        {:error, reason} ->
          Mix.raise("Failed to download #{filename}: #{reason}")
      end
    end)
  end

  defp download_seed(branch) do
    Mix.shell().info("\nDownloading seed.sql...")

    url = "#{@github_raw}/#{branch}/pkgs/core/supabase/seed.sql"

    case fetch_content(url) do
      {:ok, content} ->
        Mix.shell().info("  -> Downloaded seed.sql")

        """
        -- Test helper functions (pgflow_tests schema)
        #{content}
        """

      {:error, reason} ->
        Mix.raise("Failed to download seed.sql: #{reason}")
    end
  end

  defp fetch_json(url) do
    headers = [
      {~c"User-Agent", ~c"pgflow-elixir"},
      {~c"Accept", ~c"application/vnd.github.v3+json"}
    ]

    case :httpc.request(:get, {String.to_charlist(url), headers}, [], []) do
      {:ok, {{_, 200, _}, _, body}} ->
        {:ok, Jason.decode!(to_string(body))}

      {:ok, {{_, status, _}, _, body}} ->
        {:error, "HTTP #{status}: #{body}"}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  defp fetch_content(url) do
    headers = [{~c"User-Agent", ~c"pgflow-elixir"}]

    case :httpc.request(:get, {String.to_charlist(url), headers}, [], []) do
      {:ok, {{_, 200, _}, _, body}} ->
        {:ok, to_string(body)}

      {:ok, {{_, status, _}, _, body}} ->
        {:error, "HTTP #{status}: #{body}"}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end
end
