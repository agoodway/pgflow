defmodule PgFlowDashboard.Migrations.Versions.V02 do
  @moduledoc "Adds skipped_steps counts and skip_reason on step lists."

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "02",
    sql_path: "pgflow_dashboard/sql/versions"
end
