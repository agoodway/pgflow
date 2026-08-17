defmodule PgFlowDashboard.Migrations.Versions.V03 do
  @moduledoc """
  PgFlowDashboard migration version 3.

  Bounds a run's `duration_ms` by `failed_at` so a failed run's displayed
  duration stops growing. v02 fixed the step-level formula but left the
  run-level one in `runs_with_progress` measuring against `NOW()` — a failed
  run never gets a `completed_at`, so its duration climbed forever. See
  `priv/pgflow_dashboard/sql/versions/v03/v03_up.sql` for the full rationale.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "03",
    sql_path: "pgflow_dashboard/sql/versions"
end
