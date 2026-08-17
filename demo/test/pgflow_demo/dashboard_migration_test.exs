defmodule PgflowDemo.DashboardMigrationTest do
  use PgflowDemo.DataCase, async: true

  test "installs the current dashboard schema version" do
    %{rows: [[comment]]} =
      Repo.query!(
        "SELECT obj_description('pgflow_dashboard.runs_with_progress'::regclass, 'pg_class')"
      )

    assert comment == "PgFlowDashboard version=3"
  end

  test "exposes skipped step counts to dashboard run queries" do
    %{rows: rows} =
      Repo.query!("""
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'pgflow_dashboard'
        AND table_name = 'runs_view'
      """)

    assert ["skipped_steps"] in rows
  end
end
