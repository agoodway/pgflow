defmodule PgflowDemo.Repo.Migrations.UpgradePgflowHelpersToV05 do
  use Ecto.Migration

  def up, do: PgFlow.HelpersMigration.up(version: 5)
  def down, do: PgFlow.HelpersMigration.down(version: 4)
end
