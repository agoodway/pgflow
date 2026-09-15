defmodule PgflowDemo.UpstreamUpgradeFixture.BaselineSetupPgflow do
  @moduledoc false
  use Ecto.Migration

  @doc false
  @spec up() :: :ok
  def up do
    PgFlow.Migration.up(version: 1)
    PgFlow.HelpersMigration.up(version: 4)
  end

  @doc false
  @spec down() :: :ok
  def down do
    PgFlow.HelpersMigration.down(version: 0)
    PgFlow.Migration.down(version: 0)
  end
end

defmodule PgflowDemo.UpstreamUpgradeFixture.BaselineDashboard do
  @moduledoc false
  use Ecto.Migration

  @doc false
  @spec up() :: :ok
  def up, do: PgFlowDashboard.Migration.up(version: 3)

  @doc false
  @spec down() :: :ok
  def down, do: PgFlowDashboard.Migration.down()
end

defmodule PgflowDemo.UpstreamUpgradeFixture.BaselineExtensions do
  @moduledoc false
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  @doc false
  @spec up() :: :ok
  def up do
    execute("CREATE EXTENSION IF NOT EXISTS citext")
    execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
  end

  @doc false
  @spec down() :: :ok
  def down do
    execute("DROP EXTENSION IF EXISTS pgcrypto")
    execute("DROP EXTENSION IF EXISTS pg_trgm")
    execute("DROP EXTENSION IF EXISTS citext")
  end
end

defmodule PgflowDemo.UpstreamUpgradeFixture.BaselineArticleFlowCleanup do
  @moduledoc false
  use Ecto.Migration

  alias PgflowDemo.Jobs.ArticleFlowCleanup

  @doc false
  @spec up() :: :ok
  def up do
    definition = ArticleFlowCleanup.__pgflow_definition__()

    for sql <- PgFlow.JobCompiler.compile(definition, include_cron?: false) do
      execute(sql)
    end
  end

  @doc false
  @spec down() :: :ok
  def down do
    execute("DELETE FROM pgflow.deps WHERE flow_slug = 'article_flow_cleanup'")
    execute("DELETE FROM pgflow.steps WHERE flow_slug = 'article_flow_cleanup'")
    execute("DELETE FROM pgflow.flows WHERE flow_slug = 'article_flow_cleanup'")
    execute("SELECT pgmq.drop_queue('article_flow_cleanup')")
  end
end
