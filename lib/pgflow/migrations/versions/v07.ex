defmodule PgFlow.Migrations.Versions.V07 do
  @moduledoc """
  PgFlow extensions migration version 7.

  Replaces the four-argument claim with the recorded five-argument
  eight-column `start_tasks` from core V03, and stamps `pgflow_version`
  from `register_worker/4`. Requires core V03.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "07",
    sql_path: "pgflow_helpers/sql/versions"
end
