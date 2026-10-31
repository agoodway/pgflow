defmodule PgflowDemo.Repo.Migrations.UpgradePgflowCoreV03HelpersV07 do
  @moduledoc """
  Advances the demo from core V02 / helpers V06 to core V03 / helpers V07.

  Versions are pinned so a later pgflow release cannot change this replayed
  step. Run `mix pgflow.audit_slugs` before migrating. The procedure is
  `docs/UPGRADING_UPSTREAM_2026_10.md`.
  """

  use Ecto.Migration

  def up do
    PgFlow.Migration.up(version: 3)
    PgFlow.HelpersMigration.up(version: 7)
  end

  def down do
    raise "PgFlow core V03 / helpers V07 upgrade is forward-only; use the documented restore procedure"
  end
end
