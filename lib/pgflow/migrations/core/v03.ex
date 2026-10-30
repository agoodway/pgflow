defmodule PgFlow.Migrations.Core.V03 do
  @moduledoc """
  Upstream core delta for pgflow 0.17.2 (`baa9e43f`).

  Private step queues, tightened slug rules, the 5-argument `start_tasks`,
  the 4-argument `ensure_flow_compiled`, `flows.queue_mode`, and
  `workers.pgflow_version`. The `pgflow_telemetry` schema is not installed.
  See `priv/pgflow_core/sql/versions/v03/v03_manifest.json`.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "03",
    sql_path: "pgflow_core/sql/versions"
end
