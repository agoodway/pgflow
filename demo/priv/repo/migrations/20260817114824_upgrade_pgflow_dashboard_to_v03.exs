defmodule PgflowDemo.Repo.Migrations.UpgradePgflowDashboardToV03 do
  @moduledoc """
  Advances existing demo installations from the original PgFlowDashboard
  schema to the library's current version.

  The initial dashboard Ecto migration is already recorded as applied in
  existing databases, so adding EctoEvolver versions to the library does not
  invoke them until a new Ecto migration calls `PgFlowDashboard.Migration.up/0`.
  """

  use Ecto.Migration

  def up, do: PgFlowDashboard.Migration.up()
  def down, do: PgFlowDashboard.Migration.down(version: 1)
end
