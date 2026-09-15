defmodule PgflowDemo.UpstreamUpgradeTest do
  use ExUnit.Case, async: false

  alias PgFlow.Upstream.Claim
  alias PgflowDemo.UpstreamUpgradeFixture

  @moduletag :integration

  setup do
    fixture = UpstreamUpgradeFixture.setup!()
    Process.unlink(fixture.repo_pid)
    on_exit(fn -> UpstreamUpgradeFixture.teardown!(fixture) end)
    {:ok, fixture: fixture}
  end

  test "fresh database replays committed migrations through the upgrade wrapper", %{
    fixture: fixture
  } do
    UpstreamUpgradeFixture.with_fixture_repo(fixture, fn ->
      PgflowDemo.Repo.query!("DROP SCHEMA IF EXISTS pgflow CASCADE")
      PgflowDemo.Repo.query!("DROP SCHEMA IF EXISTS pgflow_dashboard CASCADE")
      PgflowDemo.Repo.query!("DROP SCHEMA IF EXISTS pgmq CASCADE")
      PgflowDemo.Repo.query!("TRUNCATE schema_migrations")
    end)

    UpstreamUpgradeFixture.migrate_all!(fixture)

    assert UpstreamUpgradeFixture.core_version(fixture) == 2
    assert UpstreamUpgradeFixture.helpers_version(fixture) == 5
    assert catalog_contract!(fixture)
    assert dashboard_comment(fixture) == "PgFlowDashboard version=4"
  end

  test "populated pre-upgrade demo data survives the upgrade wrapper", %{fixture: fixture} do
    snapshot = fixture.snapshot

    assert snapshot.cleanup_job_type == "job"
    assert snapshot.cron_count == 1
    assert snapshot.dashboard_comment == "PgFlowDashboard version=3"
    assert snapshot.run_count >= 2
    assert snapshot.task_count > 0
    assert snapshot.queued_message_count > 0

    assert UpstreamUpgradeFixture.core_version(fixture) == 1
    assert UpstreamUpgradeFixture.helpers_version(fixture) == 4

    UpstreamUpgradeFixture.migrate_upgrade!(fixture)

    assert UpstreamUpgradeFixture.core_version(fixture) == 2
    assert UpstreamUpgradeFixture.helpers_version(fixture) == 5
    assert catalog_contract!(fixture)
    assert Claim.verify!() == :ok

    assert UpstreamUpgradeFixture.scalar!(fixture, "SELECT count(*) FROM pgflow.runs") ==
             snapshot.run_count

    assert UpstreamUpgradeFixture.scalar!(fixture, "SELECT count(*) FROM pgflow.step_tasks") ==
             snapshot.task_count

    assert UpstreamUpgradeFixture.scalar!(
             fixture,
             "SELECT count(*) FROM pgflow.step_tasks WHERE message_id IS NOT NULL"
           ) == snapshot.queued_message_count

    stored_output =
      UpstreamUpgradeFixture.scalar!(
        fixture,
        "SELECT output FROM pgflow.step_states WHERE run_id = $1 AND step_slug = 'fetch_article'",
        [snapshot.article_run_id]
      )

    assert normalize_json(stored_output) == snapshot.article_output

    assert UpstreamUpgradeFixture.scalar!(
             fixture,
             "SELECT flow_type FROM pgflow.flows WHERE flow_slug = 'article_flow_cleanup'"
           ) == "job"

    if snapshot.cron_count > 0 do
      assert UpstreamUpgradeFixture.scalar!(
               fixture,
               "SELECT count(*) FROM cron.job WHERE jobname = 'pgflow:article_flow_cleanup'"
             ) == snapshot.cron_count
    end

    assert dashboard_comment(fixture) == "PgFlowDashboard version=4"
  end

  test "upgrade wrapper is forward-only", %{fixture: fixture} do
    UpstreamUpgradeFixture.migrate_upgrade!(fixture)

    migration = UpstreamUpgradeFixture.upgrade_migration_module()
    version = UpstreamUpgradeFixture.upgrade_migration_version!()

    assert_raise RuntimeError, ~r/forward-only/, fn ->
      UpstreamUpgradeFixture.with_fixture_repo(fixture, fn ->
        Ecto.Migrator.down(PgflowDemo.Repo, version, migration, log: false)
      end)
    end

    assert UpstreamUpgradeFixture.core_version(fixture) == 2
    assert UpstreamUpgradeFixture.helpers_version(fixture) == 5
  end

  test "application FlowStarter is ready after demo test database migration" do
    assert :ok = PgFlow.FlowStarter.await_ready(30_000)

    assert Enum.all?(PgFlow.FlowStarter.status().modules, fn state ->
             state.status == :succeeded
           end)
  end

  defp catalog_contract!(fixture) do
    assert start_tasks_signatures(fixture) == [[true, true]]

    assert record_fields(fixture) ==
             ~w(flow_slug run_id step_slug input msg_id task_index flow_input attempts_count)

    assert queue_columns(fixture) == [["step_tasks", "queue_name"], ["steps", "queue_name"]]

    assert UpstreamUpgradeFixture.scalar!(
             fixture,
             "SELECT to_regprocedure('pgflow.ensure_flow_compiled(text,jsonb)') IS NOT NULL"
           )

    true
  end

  defp dashboard_comment(fixture) do
    UpstreamUpgradeFixture.scalar!(
      fixture,
      "SELECT obj_description('pgflow_dashboard.runs_with_progress'::regclass, 'pg_class')"
    )
  end

  defp start_tasks_signatures(fixture) do
    UpstreamUpgradeFixture.query_rows!(fixture, """
    SELECT
      to_regprocedure('pgflow.start_tasks(text,bigint[],uuid,text)') IS NOT NULL,
      to_regprocedure('pgflow.start_tasks(text,bigint[],uuid)') IS NULL
    """)
  end

  defp record_fields(fixture) do
    UpstreamUpgradeFixture.query_rows!(fixture, """
    SELECT attname
    FROM pg_attribute
    WHERE attrelid = (
      SELECT typrelid FROM pg_type WHERE oid = 'pgflow.step_task_record'::regtype
    )
      AND attnum > 0
      AND NOT attisdropped
    ORDER BY attnum
    """)
    |> List.flatten()
  end

  defp normalize_json(value) when is_binary(value), do: Jason.decode!(value)
  defp normalize_json(value), do: value

  defp queue_columns(fixture) do
    UpstreamUpgradeFixture.query_rows!(fixture, """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'pgflow'
      AND (table_name, column_name) IN (('steps', 'queue_name'), ('step_tasks', 'queue_name'))
    ORDER BY table_name
    """)
  end
end
