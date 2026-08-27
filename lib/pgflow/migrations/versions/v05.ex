defmodule PgFlow.Migrations.Versions.V05 do
  @moduledoc """
  Adds awaiting-signals: `waiting` task status, `task_signals` store, and atomic await/signal/expiry.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "05",
    sql_path: "pgflow_helpers/sql/versions"
end
