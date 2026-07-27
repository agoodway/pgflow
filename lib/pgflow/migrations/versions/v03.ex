defmodule PgFlow.Migrations.Versions.V03 do
  @moduledoc """
  PgFlow extensions migration version 3.

  Adds `attempts_count` to the `step_task_record` composite type and replaces
  `start_tasks` with the upstream body plus that column, so a dispatched task
  carries the attempt it is on and `PgFlow.Context.attempt` can stop reporting a
  hardcoded `1`.

  Deploy the worker before applying this: `PgFlow.Worker.TaskRow` reads both the
  seven- and eight-column row, but a worker that predates it matches seven
  positionally and would fail every dispatch against the wider record. See
  `priv/pgflow_helpers/sql/versions/v03/v03_up.sql` for why the returned value is
  the stored count plus one.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "03",
    sql_path: "pgflow_helpers/sql/versions"
end
