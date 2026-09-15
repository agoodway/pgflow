defmodule PgflowDemo.Repo.Migrations.UpgradePgflowUpstreamAlignment do
  @moduledoc """
  Advances existing demo installations to the synchronized upstream core and
  helpers versions without rewriting historical setup or compile migrations.
  """
  use Ecto.Migration

  def up do
    PgFlow.Migration.up()
    PgFlow.HelpersMigration.up()
    PgFlowDashboard.Migration.up()
  end

  def down do
    raise "Coordinated PgFlow upgrade is forward-only; use the documented restore procedure"
  end
end
