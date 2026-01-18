defmodule Mix.Tasks.Pgflow.CopyMigrations do
  @shortdoc "Downloads pgflow SQL migrations from GitHub and generates Ecto wrappers"

  @moduledoc """
  Downloads pgflow SQL migrations from the official repository and generates
  Ecto migration wrappers that execute the SQL files.

  ## Usage

      mix pgflow.copy_migrations [options]

  ## Options

    * `--branch` - Git branch to download from. Default: `main`

    * `--install` - Also copy migrations to your app's `priv/repo/migrations` directory

  ## Examples

      # Download migrations to priv/pgflow/migrations
      mix pgflow.copy_migrations

      # Download from a specific branch
      mix pgflow.copy_migrations --branch develop

      # Also install to your app's migrations
      mix pgflow.copy_migrations --install

  ## Generated Files

  SQL files are downloaded to:
      priv/pgflow/migrations/sql/

  Ecto wrapper migrations are generated in:
      priv/pgflow/migrations/ecto/

  Each wrapper looks like:

      defmodule PgFlow.Migrations.V20240101120000CreatePgflowSchema do
        use Ecto.Migration

        def up do
          path = Application.app_dir(:pgflow, "priv/pgflow/migrations/sql/20240101120000_create_pgflow_schema.sql")
          execute(File.read!(path))
        end

        def down do
          # pgflow migrations are not reversible
          raise "pgflow migrations cannot be automatically rolled back"
        end
      end

  """

  use Mix.Task

  @github_api "https://api.github.com/repos/pgflow-dev/pgflow/contents/pkgs/core/supabase/migrations"
  @github_raw "https://raw.githubusercontent.com/pgflow-dev/pgflow"

  @impl Mix.Task
  def run(args) do
    # Ensure httpc is available
    {:ok, _} = Application.ensure_all_started(:inets)
    {:ok, _} = Application.ensure_all_started(:ssl)

    {opts, _, _} = OptionParser.parse(args, switches: [branch: :string, install: :boolean])

    branch = Keyword.get(opts, :branch, "main")
    install? = Keyword.get(opts, :install, false)

    # Create target directories
    sql_dir = "priv/pgflow/migrations/sql"
    ecto_dir = "priv/pgflow/migrations/ecto"

    File.mkdir_p!(sql_dir)
    File.mkdir_p!(ecto_dir)

    Mix.shell().info("Fetching migration list from GitHub (branch: #{branch})...")

    # Get list of migration files from GitHub API
    api_url = "#{@github_api}?ref=#{branch}"

    files =
      case fetch_json(api_url) do
        {:ok, files} when is_list(files) ->
          files
          |> Enum.filter(fn file ->
            String.ends_with?(file["name"] || "", ".sql")
          end)
          |> Enum.sort_by(& &1["name"])

        {:error, reason} ->
          Mix.raise("Failed to fetch migration list: #{reason}")
      end

    if Enum.empty?(files) do
      Mix.raise("No SQL files found in repository")
    end

    Mix.shell().info("Found #{length(files)} migration files")

    # Download and process each SQL file
    migrations =
      Enum.map(files, fn file ->
        filename = file["name"]
        raw_url = "#{@github_raw}/#{branch}/pkgs/core/supabase/migrations/#{filename}"

        Mix.shell().info("  Downloading: #{filename}")

        case fetch_content(raw_url) do
          {:ok, content} ->
            # Save SQL file
            target_path = Path.join(sql_dir, filename)
            File.write!(target_path, content)

            # Generate Ecto wrapper
            {timestamp, name} = parse_migration_filename(filename)
            module_name = camelize_migration_name(name)
            ecto_content = generate_ecto_migration(timestamp, module_name, filename)

            ecto_filename = "#{timestamp}_#{name}.exs"
            ecto_path = Path.join(ecto_dir, ecto_filename)
            File.write!(ecto_path, ecto_content)

            {timestamp, name, ecto_filename}

          {:error, reason} ->
            Mix.raise("Failed to download #{filename}: #{reason}")
        end
      end)

    Mix.shell().info("\nGenerated #{length(migrations)} Ecto migration wrappers")

    # Optionally install to app's migrations
    if install? do
      install_migrations(migrations, ecto_dir)
    end

    Mix.shell().info("\nDone! Migrations are ready in priv/pgflow/migrations/")

    if not install? do
      Mix.shell().info("""

      To install migrations to your app, either:
        1. Run: mix pgflow.copy_migrations --install
        2. Or manually copy from priv/pgflow/migrations/ecto/ to priv/repo/migrations/
      """)
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

  defp parse_migration_filename(filename) do
    case Regex.run(~r/^(\d+)_(.+)\.sql$/, filename) do
      [_, timestamp, name] ->
        {timestamp, name}

      nil ->
        Mix.raise("Invalid migration filename format: #{filename}")
    end
  end

  defp camelize_migration_name(name) do
    name
    |> String.split("_")
    |> Enum.map_join(&String.capitalize/1)
  end

  defp generate_ecto_migration(timestamp, module_name, sql_filename) do
    """
    defmodule PgFlow.Migrations.V#{timestamp}#{module_name} do
      @moduledoc \"\"\"
      Ecto migration wrapper for pgflow SQL migration.

      This migration executes the SQL file: #{sql_filename}
      \"\"\"
      use Ecto.Migration

      @sql_file Application.app_dir(:pgflow, "priv/pgflow/migrations/sql/#{sql_filename}")

      def up do
        execute(File.read!(@sql_file))
      end

      def down do
        # pgflow migrations are forward-only
        # Rolling back requires manual intervention
        raise "pgflow migrations cannot be automatically rolled back"
      end
    end
    """
  end

  defp install_migrations(migrations, ecto_dir) do
    target_dir = "priv/repo/migrations"
    File.mkdir_p!(target_dir)

    Mix.shell().info("\nInstalling migrations to #{target_dir}...")

    Enum.each(migrations, fn {timestamp, name, ecto_filename} ->
      source = Path.join(ecto_dir, ecto_filename)
      target = Path.join(target_dir, "#{timestamp}_pgflow_#{name}.exs")

      content = File.read!(source)
      File.write!(target, content)
      Mix.shell().info("  Installed: #{Path.basename(target)}")
    end)
  end
end
