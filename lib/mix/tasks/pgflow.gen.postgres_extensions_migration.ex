defmodule Mix.Tasks.Pgflow.Gen.PostgresExtensionsMigration do
  @shortdoc "Generate a migration that installs Postgres extensions required by pgflow"

  @moduledoc """
  Generates a single Ecto migration that runs `CREATE EXTENSION` for the
  Postgres extensions pgflow relies on.

  Defaults to: `citext`, `pg_trgm`, `pgcrypto`, `pg_cron`.

  Pairs with `mix pgflow.setup` (installs the pgflow schema) and
  `mix pgflow.gen.pgmq_migration` (installs pgmq via SQL-only method,
  which is the portable path for pgmq since most Postgres distributions
  don't ship it as a native extension).

  ## Usage

      mix pgflow.gen.postgres_extensions_migration
      mix pgflow.gen.postgres_extensions_migration --repo MyApp.OtherRepo
      mix pgflow.gen.postgres_extensions_migration --no-cron

  ## Options

    * `--repo` - Ecto repo module to generate the migration for. Defaults
      to the first entry in `config :my_app, ecto_repos: [...]`.

    * `--no-cron` - Skip `pg_cron`. Use on managed Postgres hosts that
      don't permit it. Default: `pg_cron` is included (pgflow schedules
      cron-based flows via pg_cron).

  ## pgmq note

  `pgmq` is NOT included in this task. On most Postgres distributions
  pgmq isn't available as a native extension — use
  `mix pgflow.gen.pgmq_migration` to install it via SQL-only method.
  On environments where pgmq IS pre-built (Supabase,
  atlas-postgres-pgflow), hand-edit the generated migration to add
  `execute("CREATE EXTENSION IF NOT EXISTS pgmq")`.

  ## Migration order

  This migration should run BEFORE `mix pgflow.gen.pgmq_migration`'s
  output and before `mix pgflow.setup`'s output:

    1. `install_extensions` (this task)
    2. `install_pgmq` (from `mix pgflow.gen.pgmq_migration`, if needed)
    3. `setup_pgflow` (from `mix pgflow.setup`)
  """

  use Mix.Task

  @impl Mix.Task
  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args, switches: [repo: :string, cron: :boolean])

    repo = resolve_repo(opts[:repo])
    cron? = Keyword.get(opts, :cron, true)

    migrations_dir = Path.join(priv_path(repo), "migrations")
    File.mkdir_p!(migrations_dir)

    timestamp = timestamp()
    filename = "#{timestamp}_install_extensions.exs"
    full_path = Path.join(migrations_dir, filename)

    File.write!(full_path, migration_content(repo, cron?))

    Mix.shell().info("""
    Created migration: #{full_path}

    Run `mix ecto.migrate` to apply, or generate the remaining pgflow
    migrations first (see `mix help pgflow.setup`).
    """)
  end

  defp resolve_repo(nil) do
    app = Mix.Project.config()[:app]

    case Application.get_env(app, :ecto_repos, []) do
      [repo] ->
        repo

      [repo | _] = repos ->
        Mix.shell().info(
          "Multiple repos configured (#{inspect(repos)}); using #{inspect(repo)}. " <>
            "Pass `--repo` to choose a specific one."
        )

        repo

      [] ->
        Mix.raise("No Ecto repos configured. Add `:ecto_repos` to your app config.")
    end
  end

  defp resolve_repo(repo_string) do
    Module.concat([repo_string])
  end

  defp priv_path(repo) do
    case repo.config()[:priv] do
      nil -> "priv/repo"
      priv when is_binary(priv) -> priv
    end
  rescue
    _ -> "priv/repo"
  end

  defp timestamp do
    {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()

    :io_lib.format("~4..0B~2..0B~2..0B~2..0B~2..0B~2..0B", [y, m, d, hh, mm, ss])
    |> IO.iodata_to_binary()
  end

  # Base extensions — always installed. pgflow's query layer depends on
  # citext (case-insensitive identifiers), pg_trgm (fuzzy search on slugs
  # and step names in the dashboard), and pgcrypto (gen_random_uuid used
  # in several core tables).
  @base_extensions ~w(citext pg_trgm pgcrypto)

  defp migration_content(repo, cron?) do
    extensions = if cron?, do: @base_extensions ++ ["pg_cron"], else: @base_extensions

    up_body =
      Enum.map_join(extensions, "\n    ", &~s|execute("CREATE EXTENSION IF NOT EXISTS #{&1}")|)

    down_body =
      extensions
      |> Enum.reverse()
      |> Enum.map_join("\n    ", &~s|execute("DROP EXTENSION IF EXISTS #{&1}")|)

    gen_command =
      "mix pgflow.gen.postgres_extensions_migration" <> if(cron?, do: "", else: " --no-cron")

    cron_note =
      if cron? do
        "  - pg_cron    — scheduled flow/job support"
      else
        "  (pg_cron skipped — regenerate without `--no-cron` if your host supports it)"
      end

    cron_setup_note =
      if cron? do
        "  pg_cron requires TWO server-level settings in `postgresql.conf`:\n" <>
          "    - `shared_preload_libraries = 'pg_cron'`  (add + restart Postgres)\n" <>
          "    - `cron.database_name = '<your_app_db>'` (pg_cron's metadata lives\n" <>
          "      in exactly one DB; defaults to `postgres`. If your app DB isn't\n" <>
          "      `postgres`, set this or install pg_cron against `postgres`\n" <>
          "      instead — cron functions will only exist in the configured DB).\n" <>
          "\n" <>
          "  Supported on Neon, AWS RDS (PG 12.5+), Aurora (PG 12.6+), and\n" <>
          "  Supabase — each with its own setup path (parameter groups, API\n" <>
          "  calls, etc.). On hosts that don't support pg_cron, regenerate\n" <>
          "  with `mix pgflow.gen.postgres_extensions_migration --no-cron`."
      else
        ""
      end

    """
    defmodule #{inspect(repo)}.Migrations.InstallExtensions do
      @moduledoc \"\"\"
      Installs Postgres extensions required by pgflow.

      Generated by: `#{gen_command}`

      Extensions:
        - citext     — case-insensitive text columns
        - pg_trgm    — trigram indexes (dashboard slug search)
        - pgcrypto   — `gen_random_uuid()` for primary keys
      #{cron_note}

      #{cron_setup_note}

      `@disable_ddl_transaction true` is set because `CREATE EXTENSION` can't
      run inside a migration transaction on some Postgres distributions.
      `@disable_migration_lock true` is set for the same reason.

      On hosts with native pgmq (Supabase, atlas-postgres-pgflow), hand-add
      `execute("CREATE EXTENSION IF NOT EXISTS pgmq")` here instead of
      running `mix pgflow.gen.pgmq_migration`.
      \"\"\"
      use Ecto.Migration

      @disable_ddl_transaction true
      @disable_migration_lock true

      def up do
        #{up_body}
      end

      def down do
        #{down_body}
      end
    end
    """
  end
end
