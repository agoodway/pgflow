defmodule PgFlow.Migrations.Versions.V06 do
  @moduledoc """
  PgFlow extensions migration version 6.

  Escalates permanently stalled tasks through `fail_task` and publishes the
  `pgflow.step_progress` SQL read model.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "06",
    sql_path: "pgflow_helpers/sql/versions"
end
