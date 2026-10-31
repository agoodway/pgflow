defmodule PgflowDemo.Repo.Migrations.UpgradePgflowUpstreamAlignment do
  @moduledoc """
  Advances existing demo installations to core V02 and helpers V05 without
  rewriting historical setup or compile migrations. The core version is pinned
  so a later pgflow release cannot change this replayed step.
  """
  use Ecto.Migration

  def up do
    PgFlow.Migration.up(version: 2)
    PgFlow.HelpersMigration.up(version: 5)
    PgFlowDashboard.Migration.up()
  end

  def down do
    raise "Coordinated PgFlow upgrade is forward-only; use the documented restore procedure"
  end
end
