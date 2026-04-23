defmodule PgFlow.HelpersMigration do
  @moduledoc """
  Manages database migrations for PgFlow's extension SQL functions.

  These functions wrap custom queries (worker registration, stalled task recovery, etc.)
  as PostgreSQL functions in the `pgflow` schema, called via RPC from `PgFlow.Queries.*` modules.

  ## Usage in Ecto Migrations

      defmodule MyApp.Repo.Migrations.AddPgflowExtensions do
        use Ecto.Migration

        def up, do: PgFlow.HelpersMigration.up()
        def down, do: PgFlow.HelpersMigration.down()
      end

  """

  # EctoEvolver generates pattern matches for :materialized_view and :table,
  # but our tracking_object is {:view, ...} so only :view is reachable.
  @dialyzer :no_match

  use EctoEvolver,
    otp_app: :pgflow,
    default_prefix: "pgflow",
    versions: [PgFlow.Migrations.Versions.V01],
    tracking_object: {:view, "extensions_version"}
end
