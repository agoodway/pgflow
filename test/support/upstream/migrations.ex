defmodule PgFlow.Upstream.CoreUpgradeMigration do
  @moduledoc false
  use Ecto.Migration

  @doc "Upgrades the fixture to the upstream-aligned core."
  def up, do: PgFlow.Migration.up(version: 2)
  @doc "Exercises the forward-only core rollback guard."
  def down, do: PgFlow.Migration.down(version: 1)
end

defmodule PgFlow.Upstream.CoreV03Migration do
  @moduledoc false
  use Ecto.Migration

  @doc "Upgrades the fixture through core V03."
  def up, do: PgFlow.Migration.up(version: 3)
  @doc "Exercises the core V03 forward-only rollback guard."
  def down, do: PgFlow.Migration.down(version: 2)
end

defmodule PgFlow.Upstream.HelpersUpgradeMigration do
  @moduledoc false
  use Ecto.Migration

  @doc "Upgrades the fixture to queue-aware helpers."
  def up, do: PgFlow.HelpersMigration.up(version: 5)
  @doc "Exercises the helpers rollback guard."
  def down, do: PgFlow.HelpersMigration.down(version: 4)
end

defmodule PgFlow.Upstream.HelpersV06Migration do
  @moduledoc false
  use Ecto.Migration

  @doc "Installs permanent stall escalation and the progress view."
  def up, do: PgFlow.HelpersMigration.up(version: 6)
  @doc "Removes the version 6 helper additions."
  def down, do: PgFlow.HelpersMigration.down(version: 5)
end

defmodule PgFlow.Upstream.HelpersV07Migration do
  @moduledoc false
  use Ecto.Migration

  @doc "Installs the 0.17.2 claim and worker-version helper."
  def up, do: PgFlow.HelpersMigration.up(version: 7)
  @doc "Exercises the helpers V07 forward-only rollback guard."
  def down, do: PgFlow.HelpersMigration.down(version: 6)
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
