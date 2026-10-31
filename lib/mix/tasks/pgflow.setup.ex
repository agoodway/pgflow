defmodule Mix.Tasks.Pgflow.Setup do
  @shortdoc "Generates one Ecto migration that installs pgflow's core schema + helper functions"

  @moduledoc """
  Generates an Ecto migration in the consumer app that installs the pgflow
  schema by calling `PgFlow.Migration.up/0` and `PgFlow.HelpersMigration.up/0`
  in the correct order. For historical SQL compatibility, it can optionally
  call `PgFlowDashboard.Migration.up/0` as well.

  This mirrors `mix tango.setup` and `mix good_analytics.setup`. The output
  is a single wrapper migration, not per-statement migrations — SQL is
  vendored inside pgflow.

  ## Usage

      mix pgflow.setup
      mix pgflow.setup --repo MyApp.OtherRepo
      mix pgflow.setup --no-helpers
      mix pgflow.setup --dashboard
      mix pgflow.setup --upgrade

  ## Options

    * `--repo` - Ecto repo module to install against. Defaults to the first
      entry in `config :my_app, ecto_repos: [...]`.

    * `--no-helpers` - Skip `PgFlow.HelpersMigration` (Elixir-binding SQL
      helpers: worker registration, flow input/output queries). Default:
      helpers are installed.

    * `--dashboard` - Also install the historical `PgFlowDashboard.Migration`
      views and functions for compatibility with external SQL consumers.
      Default: skipped. The core-backed PgFlow LiveView dashboard does not
      require this option.

    * `--upgrade` - Generate a **new** wrapper migration that upgrades an
      existing pgflow install by calling `PgFlow.Migration.up/0` and
      `PgFlow.HelpersMigration.up/0` in one Ecto transaction. Use this when a
      dependency bump adds core/helpers versions. An already-applied setup
      wrapper does **not** rerun just because the pgflow library version
      changed — Ecto records the wrapper once and skips it on later
      `mix ecto.migrate` runs. The generated `down/0` is forward-only.

    * `--migrations-path` / `-p` - Directory for the generated migration.
      Defaults to `<repo priv>/migrations`.

  ## Prerequisites

  Before `mix ecto.migrate` picks up the generated migration, the consumer
  must have:

    * pgmq installed (run `mix pgflow.gen.pgmq_migration` to generate a
      migration that installs pgmq via SQL-only method — required unless
      your Postgres already provides pgmq, e.g. Supabase).
    * `pg_cron` extension registered (`CREATE EXTENSION pg_cron` in an
      earlier migration).
    * `citext`, `pg_trgm`, `pgcrypto` extensions registered.

  The typical migration order is:

    1. `install_extensions` (citext, pg_trgm, pgcrypto, pg_cron)
    2. `install_pgmq` (from `mix pgflow.gen.pgmq_migration`)
    3. `setup_pgflow` (this task)
  """

  use Mix.Task

  alias Mix.Tasks.Pgflow.Helpers

  @impl Mix.Task
  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        switches: [
          repo: :string,
          helpers: :boolean,
          dashboard: :boolean,
          upgrade: :boolean,
          migrations_path: :string
        ],
        aliases: [p: :migrations_path]
      )

    repo = Helpers.resolve_repo(opts[:repo])
    helpers? = Keyword.get(opts, :helpers, true)
    dashboard? = Keyword.get(opts, :dashboard, false)
    upgrade? = Keyword.get(opts, :upgrade, false)

    migrations_dir =
      Keyword.get_lazy(opts, :migrations_path, fn ->
        Path.join(Helpers.priv_path(repo), "migrations")
      end)

    File.mkdir_p!(migrations_dir)

    timestamp = Helpers.generate_timestamp()

    {filename, content} =
      if upgrade? do
        {upgrade_filename(timestamp), upgrade_migration_content(repo, helpers?, dashboard?)}
      else
        {"#{timestamp}_setup_pgflow.exs", migration_content(repo, helpers?, dashboard?)}
      end

    full_path = Path.join(migrations_dir, filename)

    File.write!(full_path, content)

    Mix.shell().info("""
    Created migration: #{full_path}

    Run `mix ecto.migrate` to apply.

    Ensure pgmq is installed before this migration runs (via
    `mix pgflow.gen.pgmq_migration`) unless your Postgres already ships pgmq.
    """)
  end

  # Pins core and helpers to the versions bundled at generation time so a
  # consumer replaying its migration history installs the same schema each time.
  defp pinned_up_steps(helpers?, dashboard?) do
    [
      "PgFlow.Migration.up(version: #{PgFlow.Migration.current_version()})",
      if(helpers?,
        do: "PgFlow.HelpersMigration.up(version: #{PgFlow.HelpersMigration.current_version()})"
      ),
      if(dashboard?, do: "PgFlowDashboard.Migration.up()")
    ]
    |> Enum.reject(&is_nil/1)
  end

  defp migration_content(repo, helpers?, dashboard?) do
    up_steps = pinned_up_steps(helpers?, dashboard?)

    down_steps =
      [
        if(dashboard?, do: "PgFlowDashboard.Migration.down()"),
        if(helpers?, do: "PgFlow.HelpersMigration.down()"),
        "PgFlow.Migration.down()"
      ]
      |> Enum.reject(&is_nil/1)

    up_body = up_steps |> Enum.map_join("\n    ", & &1)
    down_body = down_steps |> Enum.map_join("\n    ", & &1)

    gen_command = setup_command(helpers?, dashboard?, false)

    sources = installed_sources_note(helpers?, dashboard?)

    """
    defmodule #{inspect(repo)}.Migrations.SetupPgflow do
      @moduledoc \"\"\"
      Installs pgflow's core schema + Elixir helper functions.

      Generated by: `#{gen_command}`

      SQL sources (vendored inside pgflow):
      #{sources}

      Versions are pinned so replaying migration history on an empty database
      installs the schema as it was when this wrapper was generated.

      `PgFlow.Migration.up/1` runs via EctoEvolver with statement-level
      splitting — the V01 core bundle contains 138 statements and
      Postgrex rejects multi-statement queries (error `42601`).
      `PgFlow.HelpersMigration.up/1` adds Elixir-binding SQL functions
      (`flow_exists`, `register_worker`, `get_flow_input`, ...) that the
      library's query layer calls via RPC.

      Idempotent: rerun is a no-op via EctoEvolver's tracking comment on
      `pgflow.pgflow_version`.

      Prerequisites (applied as earlier migrations):
        1. Postgres extensions — `mix pgflow.gen.postgres_extensions_migration`
        2. pgmq schema + functions — `mix pgflow.gen.pgmq_migration` (or a
           native `CREATE EXTENSION pgmq` on Supabase/atlas-postgres-pgflow)
      \"\"\"
      use Ecto.Migration

      def up do
        #{up_body}
      end

      def down do
        #{down_body}
      end
    end
    """
  end

  @forward_only_down_message "This PgFlow upstream upgrade is forward-only; restore the coordinated backup to revert"

  defp upgrade_migration_content(repo, helpers?, dashboard?) do
    up_steps = pinned_up_steps(helpers?, dashboard?)

    up_body = up_steps |> Enum.map_join("\n    ", & &1)
    gen_command = setup_command(helpers?, dashboard?, true)

    """
    defmodule #{inspect(repo)}.Migrations.#{upgrade_module_name()} do
      @moduledoc \"\"\"
      Upgrades pgflow's core schema and helper functions in one Ecto transaction.

      Generated by: `#{gen_command}`

      Stop-and-drain before migrating:
        1. Before the window, upgrade to pgflow 0.6.0, run `mix pgflow.audit_slugs`,
           generate this wrapper, and back up the database.
        2. Pause producers that call start_flow, enqueue, cron, or TS startFlow.
        3. Drain running handlers; queued tasks and PGMQ messages are kept.
        4. Stop every worker: Elixir nodes, TS workers, recovery and prune jobs, and definition writers.
           This step must stop every worker before `mix ecto.migrate` runs.
        5. Migrate with `mix ecto.migrate` in one transaction (core, then helpers); a failure changes nothing.
        6. Deploy matching worker code, then resume maintenance, workers, and producers.

      `up/0` must run core then helpers inside the same migration transaction.
      Do not split these calls across separate migrations or rerun an older
      setup wrapper that Ecto has already recorded — generate a **new**
      timestamped wrapper whenever a pgflow release adds core or helpers
      versions.

      EctoEvolver applies only the pgflow versions that are newer than what the
      database already tracks. Run `mix ecto.migrate` before deploying a worker
      release that depends on the upgraded SQL.
      \"\"\"
      use Ecto.Migration

      def up do
        #{up_body}
      end

      def down do
        raise "#{@forward_only_down_message}"
      end
    end
    """
  end

  defp upgrade_filename(timestamp) do
    core = padded_version("v", PgFlow.Migration.current_version())
    helpers = padded_version("v", PgFlow.HelpersMigration.current_version())
    "#{timestamp}_upgrade_pgflow_core_#{core}_helpers_#{helpers}.exs"
  end

  defp upgrade_module_name do
    core = padded_version("V", PgFlow.Migration.current_version())
    helpers = padded_version("V", PgFlow.HelpersMigration.current_version())
    "UpgradePgflowCore#{core}Helpers#{helpers}"
  end

  defp padded_version(prefix, version) when is_integer(version) do
    prefix <> String.pad_leading(Integer.to_string(version), 2, "0")
  end

  defp setup_command(helpers?, dashboard?, upgrade?) do
    "mix pgflow.setup" <>
      if(upgrade?, do: " --upgrade", else: "") <>
      if(helpers?, do: "", else: " --no-helpers") <>
      if(dashboard?, do: " --dashboard", else: "")
  end

  defp installed_sources_note(helpers?, dashboard?) do
    [
      {true,
       "  - priv/pgflow_core/sql/versions/v01/v01_up.sql       (core: flows, runs, steps, workers, step_tasks, step_states, deps)"},
      {helpers?,
       "  - priv/pgflow_helpers/sql/versions/v01/v01_up.sql    (Elixir-binding RPC helpers)"},
      {dashboard?,
       "  - priv/pgflow_dashboard/sql/versions/v01/v01_up.sql  (historical dashboard views + functions)"}
    ]
    |> Enum.filter(fn {include?, _} -> include? end)
    |> Enum.map_join("\n  ", fn {_, line} -> line end)
  end
end
