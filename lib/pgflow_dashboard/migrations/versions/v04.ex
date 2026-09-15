defmodule PgFlowDashboard.Migrations.Versions.V04 do
  @moduledoc """
  PgFlowDashboard migration version 4.

  Resolves canonical worker queue names back to their logical flow identity,
  including mixed-case slugs, without changing the worker view's columns.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "04",
    sql_path: "pgflow_dashboard/sql/versions"
end
