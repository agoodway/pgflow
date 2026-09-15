defmodule PgFlow.Migrations.Versions.V05 do
  @moduledoc """
  PgFlow extensions migration version 5.

  Reconciles the helper chain with core V02 queue identity:

    * requires core V02 (`queue_name` snapshots)
    * installs the recorded eight-column four-argument `start_tasks/4`
    * drops the obsolete three-argument helper overload from V03
    * rebases `recover_stalled_tasks/1` and `prune_data_older_than/2` on task
      queue snapshots and persisted step routes

  See `priv/pgflow_helpers/sql/versions/v05/v05_up.sql` for details.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "05",
    sql_path: "pgflow_helpers/sql/versions"
end
