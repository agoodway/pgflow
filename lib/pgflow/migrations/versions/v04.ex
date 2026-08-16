defmodule PgFlow.Migrations.Versions.V04 do
  @moduledoc """
  PgFlow extensions migration version 4.

  Narrows `recover_stalled_tasks/1` to tasks `pgflow.start_tasks` could actually
  dispatch again: the run must be `started` and the task's `step_states` row must
  be `started`.

  Without the step-state guard, a map step that skips via `when_exhausted`
  strands its siblings in `started` — `fail_task` archives their messages but
  never terminalizes the task rows — and the sweep requeued those orphans on an
  already-completed run. See
  `priv/pgflow_helpers/sql/versions/v04/v04_up.sql` for the full rationale.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "04",
    sql_path: "pgflow_helpers/sql/versions"
end
