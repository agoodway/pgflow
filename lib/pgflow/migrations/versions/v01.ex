defmodule PgFlow.Migrations.Versions.V01 do
  @moduledoc """
  PgFlow extensions migration version 1.

  Creates PostgreSQL functions in the `pgflow` schema for:

  ## Read Functions
  - `get_flow_input(uuid)` - Get flow run input data
  - `flow_exists(text)` - Check if flow exists
  - `get_step_output(uuid, text)` - Get step output

  ## Write Functions
  - `register_worker(uuid, text, text)` - Register or heartbeat a worker
  - `mark_worker_stopped(uuid)` - Mark worker as stopped
  - `recover_stalled_tasks(double precision)` - Recover stalled tasks and reset visibility
  """

  use PgEvolver.Version,
    otp_app: :pgflow,
    version: "01",
    sql_path: "pgflow_extensions/sql/versions"
end
