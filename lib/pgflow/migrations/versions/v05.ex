defmodule PgFlow.Migrations.Versions.V05 do
  @moduledoc """
  Adds awaiting-signals: `waiting` task status, `task_signals` store, park/signal/consume/expire.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "05",
    sql_path: "pgflow_helpers/sql/versions"
end
