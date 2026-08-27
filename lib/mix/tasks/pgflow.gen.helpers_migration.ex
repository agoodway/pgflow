defmodule Mix.Tasks.Pgflow.Gen.HelpersMigration do
  @shortdoc "Generates initial or version-aware PgFlow helpers migrations"

  @moduledoc """
  Generates an Ecto migration that installs PgFlow's extension SQL functions.

  ## Usage

      mix pgflow.gen.helpers_migration
      mix pgflow.gen.helpers_migration --from-version 4
      mix pgflow.gen.helpers_migration --migrations-path priv/repo/migrations

  ## Options

    * `--from-version` - Installed helper version to restore on rollback.
      When omitted, generates the initial-install migration. When supplied, it
      must be at least V01 and lower than the current helper version.
    * `--migrations-path` - Path to the migrations directory.
      Defaults to `priv/repo/migrations`.

  ## Generated Functions

  This migration creates PostgreSQL functions in the `pgflow` schema:

  ### Read Functions
    * `get_flow_input(uuid)` - Get flow run input data
    * `flow_exists(text)` - Check if flow exists
    * `get_step_output(uuid, text)` - Get step output

  ### Write Functions
    * `register_worker(uuid, text, text)` - Register or heartbeat a worker
    * `mark_worker_stopped(uuid)` - Mark worker as stopped
    * `recover_stalled_tasks(double precision)` - Recover stalled tasks

  ## Requirements

  The pgflow schema must already exist. Run pgflow migrations first if needed.

  With no `--from-version`, this task generates the initial-install helpers
  migration. Existing installations must generate and apply a version-aware
  helpers upgrade migration whenever PgFlow release notes increase the helpers
  version. For V05, run:

      mix pgflow.gen.helpers_migration --from-version 4

  Apply that migration before starting the new worker release. V05 rollback
  refuses active waits/signals, so operators must drain them before rollback.
  Validate its deferred `valid_status` constraint later in a separately
  committed operator migration, not inside the V05 transaction:

      ALTER TABLE pgflow.step_tasks VALIDATE CONSTRAINT valid_status;

  ## Example

      # Generate the migration
      $ mix pgflow.gen.helpers_migration

      # Run the migration
      $ mix ecto.migrate

  """

  use Mix.Task

  alias Mix.Tasks.Pgflow.Helpers
  alias PgFlow.HelpersMigration

  @switches [from_version: :integer, migrations_path: :string]
  @aliases [p: :migrations_path]

  @impl Mix.Task
  def run(args) do
    {opts, positional, invalid} =
      OptionParser.parse(args, strict: @switches, aliases: @aliases)

    validate_arguments!(positional, invalid)

    case Keyword.fetch(opts, :from_version) do
      :error -> generate_initial_migration(opts)
      {:ok, from_version} -> generate_upgrade_migration(opts, from_version)
    end
  end

  defp generate_initial_migration(opts) do
    Helpers.write_migration(
      migrations_path_args(opts),
      "add_pgflow_helpers",
      &generate_migration_content/1,
      &message/1
    )
  end

  defp generate_upgrade_migration(opts, from_version) do
    current_version = HelpersMigration.current_version()
    validate_from_version!(from_version, current_version)

    rendered_version = render_version(current_version)

    Helpers.write_migration(
      migrations_path_args(opts),
      "upgrade_pgflow_helpers_to_#{String.downcase(rendered_version)}",
      &generate_upgrade_migration_content(&1, current_version, from_version, rendered_version)
    )
  end

  defp migrations_path_args(opts) do
    case Keyword.fetch(opts, :migrations_path) do
      {:ok, path} -> ["--migrations-path", path]
      :error -> []
    end
  end

  defp validate_arguments!([], []), do: :ok

  defp validate_arguments!(positional, invalid) do
    Mix.raise(
      "Invalid arguments for pgflow.gen.helpers_migration: " <>
        inspect(positional ++ invalid)
    )
  end

  defp validate_from_version!(from_version, current_version)
       when is_integer(from_version) and from_version >= 1 and from_version < current_version,
       do: :ok

  defp validate_from_version!(from_version, _current_version)
       when is_integer(from_version) and from_version < 1 do
    Mix.raise("source helper version must be at least V01")
  end

  defp validate_from_version!(_from_version, current_version) do
    Mix.raise("source helper version must be lower than #{render_version(current_version)}")
  end

  defp render_version(version) do
    "V" <> (version |> Integer.to_string() |> String.pad_leading(2, "0"))
  end

  defp message(filepath) do
    """
    Generated migration: #{filepath}

    Run the migration with:
        mix ecto.migrate

    This will create PostgreSQL functions in the pgflow schema for:
      - Worker registration and lifecycle
      - Flow input/output queries
      - Stalled task recovery

    Note: The pgflow schema must already exist. Run pgflow migrations first if needed.
    For upgrades, rerun this task with the --from-version value named in the
    PgFlow release notes; do not reapply the initial-install migration.
    """
  end

  defp generate_migration_content(app_module) do
    """
    defmodule #{app_module}.Repo.Migrations.AddPgflowHelpers do
      @moduledoc \"\"\"
      Installs PgFlow extension SQL functions in the pgflow schema.

      Generated by: mix pgflow.gen.helpers_migration
      \"\"\"
      use Ecto.Migration

      def up do
        PgFlow.HelpersMigration.up()
      end

      def down do
        PgFlow.HelpersMigration.down()
      end
    end
    """
  end

  defp generate_upgrade_migration_content(
         app_module,
         current_version,
         from_version,
         rendered_version
       ) do
    """
    defmodule #{app_module}.Repo.Migrations.UpgradePgflowHelpersTo#{rendered_version} do
      use Ecto.Migration

      def up, do: PgFlow.HelpersMigration.up(version: #{current_version})
      def down, do: PgFlow.HelpersMigration.down(version: #{from_version})
    end
    """
  end
end
