defmodule PgFlowDashboard.Queries do
  @moduledoc """
  Centralized database queries for the PgFlow Dashboard.

  All queries delegate to PostgreSQL functions in the `pgflow_dashboard` schema,
  providing a portable SQL API that can be used from any language or SQL client.

  This module serves as a facade, delegating to domain-specific modules:

    * `PgFlowDashboard.Queries.Metrics` - Overview metrics with caching
    * `PgFlowDashboard.Queries.Runs` - Run and step state queries
    * `PgFlowDashboard.Queries.Workers` - Worker queries
    * `PgFlowDashboard.Queries.Flows` - Flow and history grid queries

  """

  # ===================
  # Overview Metrics
  # ===================

  defdelegate get_overview_metrics(repo, opts \\ []), to: PgFlowDashboard.Queries.Metrics

  # ===================
  # Runs
  # ===================

  defdelegate list_runs(repo, opts \\ []), to: PgFlowDashboard.Queries.Runs
  defdelegate count_runs(repo, opts \\ []), to: PgFlowDashboard.Queries.Runs
  defdelegate get_run(repo, run_id), to: PgFlowDashboard.Queries.Runs
  defdelegate get_adjacent_run(repo, run_id, direction), to: PgFlowDashboard.Queries.Runs
  defdelegate list_step_states(repo, run_id), to: PgFlowDashboard.Queries.Runs
  defdelegate list_step_tasks(repo, run_id, step_slug), to: PgFlowDashboard.Queries.Runs

  # ===================
  # Workers
  # ===================

  defdelegate count_workers(repo, opts \\ []), to: PgFlowDashboard.Queries.Workers
  defdelegate list_workers(repo, opts \\ []), to: PgFlowDashboard.Queries.Workers
  defdelegate get_worker(repo, worker_id), to: PgFlowDashboard.Queries.Workers
  defdelegate list_worker_tasks(repo, worker_id, opts \\ []), to: PgFlowDashboard.Queries.Workers
  defdelegate get_adjacent_worker(repo, worker_id, direction), to: PgFlowDashboard.Queries.Workers

  # ===================
  # Flows
  # ===================

  defdelegate list_flows(repo), to: PgFlowDashboard.Queries.Flows
  defdelegate get_flow_with_graph(repo, flow_slug), to: PgFlowDashboard.Queries.Flows
  defdelegate get_run_history_grid(repo, flow_slug, opts \\ []), to: PgFlowDashboard.Queries.Flows

  # ===================
  # Jobs
  # ===================

  defdelegate list_jobs(repo), to: PgFlowDashboard.Queries.Jobs
  defdelegate get_job(repo, flow_slug), to: PgFlowDashboard.Queries.Jobs

  defdelegate get_job_run_history_grid(repo, flow_slug, opts \\ []),
    to: PgFlowDashboard.Queries.Jobs,
    as: :get_run_history_grid

  # ===================
  # Crons
  # ===================

  defdelegate list_crons(repo), to: PgFlowDashboard.Queries.Crons
  defdelegate get_cron(repo, flow_slug), to: PgFlowDashboard.Queries.Crons

  defdelegate get_cron_run_history_grid(repo, flow_slug, opts \\ []),
    to: PgFlowDashboard.Queries.Crons,
    as: :get_run_history_grid
end
