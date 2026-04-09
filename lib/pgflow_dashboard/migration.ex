defmodule PgFlowDashboard.Migration do
  @moduledoc """
  Manages PgFlowDashboard database migrations with version tracking.

  This module provides versioned migrations for PgFlowDashboard, allowing
  incremental upgrades and rollbacks.

  ## Usage in Ecto Migrations

      defmodule MyApp.Repo.Migrations.AddPgflowDashboard do
        use Ecto.Migration

        def up, do: PgFlowDashboard.Migration.up()
        def down, do: PgFlowDashboard.Migration.down()
      end

  ## Options

    * `:prefix` - The schema prefix to use. Defaults to "pgflow_dashboard".
    * `:version` - Target version for up/down migrations. Defaults to current/0.

  ## Version Tracking

  Versions are tracked via PostgreSQL comments on the `runs_with_progress` view:

      COMMENT ON VIEW schema.runs_with_progress IS 'PgFlowDashboard version=1';

  """

  # EctoEvolver generates pattern matches for :materialized_view and :table,
  # but our tracking_object is {:view, ...} so only :view is reachable.
  @dialyzer :no_match

  use EctoEvolver,
    otp_app: :pgflow,
    default_prefix: "pgflow_dashboard",
    versions: [PgFlowDashboard.Migrations.Versions.V01],
    tracking_object: {:view, "runs_with_progress"}
end
