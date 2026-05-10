defmodule PgflowDemo.Repo.Migrations.SetupPgflowDashboard do
  @moduledoc """
  Installs the PgFlowDashboard schema, views, and functions used by the
  dashboard LiveViews (overview, runs, flows, jobs, crons, workers).
  """
  use Ecto.Migration

  def up, do: PgFlowDashboard.Migration.up()
  def down, do: PgFlowDashboard.Migration.down()
end
