defmodule PgFlow.ExtensionsMigration do
  @moduledoc """
  Manages database migrations for PgFlow's extension SQL functions.

  These functions wrap custom queries (worker registration, stalled task recovery, etc.)
  as PostgreSQL functions in the `pgflow` schema, called via RPC from `PgFlow.Queries`.

  ## Usage in Ecto Migrations

      defmodule MyApp.Repo.Migrations.AddPgflowExtensions do
        use Ecto.Migration

        def up, do: PgFlow.ExtensionsMigration.up()
        def down, do: PgFlow.ExtensionsMigration.down()
      end

  """

  use PgEvolver,
    otp_app: :pgflow,
    default_prefix: "pgflow",
    versions: [PgFlow.Migrations.Versions.V01],
    tracking_object: {:view, "extensions_version"}
end
