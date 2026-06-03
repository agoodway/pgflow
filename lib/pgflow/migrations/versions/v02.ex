defmodule PgFlow.Migrations.Versions.V02 do
  @moduledoc """
  PgFlow extensions migration version 2.

  Replaces `recover_stalled_tasks(double precision)` with a step-aware
  implementation: it deadlines on each task's effective timeout
  (`coalesce(step.opt_timeout, flow.opt_timeout)`) plus a buffer, caps requeues
  at 3 (then archives the message and sets `permanently_stalled_at`), takes
  `FOR UPDATE SKIP LOCKED`, and skips tasks whose run has failed. See
  `priv/pgflow_helpers/sql/versions/v02/v02_up.sql` for the rationale and how it
  differs from upstream pgflow's `requeue_stalled_tasks()`.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "02",
    sql_path: "pgflow_helpers/sql/versions"
end
