defmodule PgFlow.Upstream.CoreUpgradeMigration do
  @moduledoc false
  use Ecto.Migration

  @doc "Upgrades the fixture to the upstream-aligned core."
  def up, do: PgFlow.Migration.up(version: 2)
  @doc "Exercises the forward-only core rollback guard."
  def down, do: PgFlow.Migration.down(version: 1)
end

defmodule PgFlow.Upstream.HelpersUpgradeMigration do
  @moduledoc false
  use Ecto.Migration

  @doc "Upgrades the fixture to queue-aware helpers."
  def up, do: PgFlow.HelpersMigration.up(version: 5)
  @doc "Exercises the helpers rollback guard."
  def down, do: PgFlow.HelpersMigration.down(version: 4)
end

defmodule PgFlow.Upstream.HelpersFullChainMigration do
  @moduledoc false
  use Ecto.Migration

  @doc "Installs the full helpers migration chain."
  def up, do: PgFlow.HelpersMigration.up()
  @doc "Uninstalls the helpers chain from the fixture."
  def down, do: PgFlow.HelpersMigration.down()
end

defmodule PgFlow.Upstream.CoreOnlyMigration do
  @moduledoc false
  use Ecto.Migration

  @doc "Installs the full core migration chain."
  def up, do: PgFlow.Migration.up()
  @doc "Exercises full core teardown from the fixture."
  def down, do: PgFlow.Migration.down()
end
