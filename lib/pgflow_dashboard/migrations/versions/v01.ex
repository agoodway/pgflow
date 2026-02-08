defmodule PgFlowDashboard.Migrations.Versions.V01 do
  @moduledoc """
  PgFlowDashboard migration version 1.

  Creates the initial dashboard schema with:

  ## Views
  - `runs_with_progress` - Runs with step progress calculations
  - `workers_with_load` - Workers with current task load
  - `flow_stats` - Per-flow statistics
  - `step_states_with_tasks` - Step states with task details

  ## Materialized Views
  - `overview_metrics_cached` - Cached dashboard overview metrics
  - `flow_stats_cached` - Cached flow statistics

  ## Query Functions (Portable SQL API)
  - `get_overview_metrics()` - Dashboard overview metrics
  - `refresh_cached_views()` - Refresh materialized views
  - `list_runs(timestamptz, text, text, int, uuid)` - List runs with filtering/pagination
  - `count_runs(timestamptz, text, text)` - Count runs matching filters
  - `get_run(uuid)` - Get single run details
  - `get_adjacent_run(uuid, text)` - Navigate to next/prev run
  - `list_step_states(uuid)` - List step states for a run
  - `list_step_tasks(uuid, text)` - List tasks for a step
  - `count_workers(text, text)` - Count workers matching filters
  - `list_workers(text, text, int, uuid)` - List workers with filtering/pagination
  - `get_worker(uuid)` - Get single worker details
  - `list_worker_tasks(uuid, int)` - List tasks processed by worker
  - `get_adjacent_worker(uuid, text)` - Navigate to next/prev worker
  - `count_flows()` - Count all flows
  - `list_flows(int, text)` - List all flows with optional pagination
  - `get_flow(text)` - Get single flow details
  - `count_jobs()` - Count all jobs
  - `list_jobs(int, text)` - List all jobs with optional pagination
  - `get_job(text)` - Get single job details
  - `count_crons()` - Count all crons
  - `list_crons(int, text)` - List all crons with optional pagination
  - `get_cron(text)` - Get single cron details
  - `list_flow_steps(text)` - List steps for a flow
  - `get_run_history_grid(text, int)` - Activity grid data for flow
  """

  use PgEvolver.Version,
    otp_app: :pgflow,
    version: "01",
    sql_path: "pgflow_dashboard/sql/versions"
end
