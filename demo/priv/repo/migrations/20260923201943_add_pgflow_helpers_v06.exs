defmodule PgflowDemo.Repo.Migrations.AddPgflowHelpersV06 do
  @moduledoc """
  Adds PgFlow helpers V06 without replaying the core/dashboard upgrade.
  """
  use Ecto.Migration

  def up, do: PgFlow.HelpersMigration.up(version: 6)

  def down do
    raise "PgFlow helpers V06 upgrade is forward-only; use the documented restore procedure"
  end
end
