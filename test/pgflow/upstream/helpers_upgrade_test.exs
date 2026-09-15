defmodule PgFlow.Upstream.HelpersUpgradeTest do
  use ExUnit.Case, async: false

  alias EctoEvolver.Adapters.Postgres, as: EvolverPostgres
  alias PgFlow.Queries.Flows
  alias PgFlow.Test.UpstreamFixture
  alias PgFlow.Upstream.Claim

  @moduletag :integration
  @moduletag :migration

  test "fresh database installs latest core and helpers" do
    fixture = fresh_fixture!()

    in_fixture(fixture, fn ->
      install_latest!()

      assert core_version() == 2
      assert helpers_version() == 5
      assert catalog_contract!()
    end)
  end

  test "baseline fixture upgrades core V01 helpers V04 to latest" do
    fixture = baseline_fixture!()

    in_fixture(fixture, fn ->
      task_count = scalar!("SELECT count(*) FROM pgflow.step_tasks")
      migrate_core_v02!()
      migrate_helpers_v05!()

      assert core_version() == 2
      assert helpers_version() == 5
      assert scalar!("SELECT count(*) FROM pgflow.step_tasks") == task_count
      assert catalog_contract!()
    end)
  end

  test "core V02 without helpers replays the full helper chain to V05" do
    fixture = fresh_fixture!()

    in_fixture(fixture, fn ->
      PgFlow.TestRepo.query!("CREATE EXTENSION IF NOT EXISTS pgmq")

      Ecto.Migrator.up(PgFlow.TestRepo, 9_000_000_201, PgFlow.Upstream.CoreOnlyMigration,
        log: false
      )

      assert core_version() == 2
      assert helpers_version() == 0

      Ecto.Migrator.up(
        PgFlow.TestRepo,
        9_000_000_202,
        PgFlow.Upstream.HelpersFullChainMigration,
        log: false
      )

      assert helpers_version() == 5
      assert start_tasks_signatures() == [[true, true]]
      assert catalog_contract!()
    end)
  end

  test "fresh and upgraded databases expose the same final catalog contract" do
    fresh = fresh_fixture!()
    baseline = baseline_fixture!()

    fresh_catalog =
      in_fixture(fresh, fn ->
        install_latest!()
        catalog_snapshot!()
      end)

    upgraded_catalog =
      in_fixture(baseline, fn ->
        migrate_core_v02!()
        migrate_helpers_v05!()
        catalog_snapshot!()
      end)

    assert Map.drop(fresh_catalog, [:core_oid, :helpers_oid]) ==
             Map.drop(upgraded_catalog, [:core_oid, :helpers_oid])
  end

  test "every intermediate V05 DDL statement validates on its own" do
    fixture = baseline_fixture!()

    in_fixture(fixture, fn ->
      migrate_core_v02!()

      v05_up =
        :pgflow
        |> Application.app_dir("priv/pgflow_helpers/sql/versions/v05/v05_up.sql")
        |> File.read!()
        |> String.replace("$SCHEMA$", "pgflow")

      v05_up
      |> String.split("--SPLIT--")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.each(fn statement ->
        assert {:ok, _} = PgFlow.TestRepo.query(statement <> "\n", [])
      end)

      migrate_helpers_v05!()
      assert helpers_version() == 5
    end)
  end

  test "recorded eight-column claim matches the V02 fragment checksum" do
    assert Claim.verify!() == :ok

    v05_up =
      :pgflow
      |> Application.app_dir("priv/pgflow_helpers/sql/versions/v05/v05_up.sql")
      |> File.read!()

    assert v05_up =~ Claim.eight_column_start_tasks_sql()
  end

  test "recovery archives exhausted tasks even when nothing is requeued" do
    fixture = baseline_fixture!()

    in_fixture(fixture, fn ->
      migrate_core_v02!()
      migrate_helpers_v05!()

      [run_id, queue_name, message_id] =
        PgFlow.TestRepo.query!("""
        SELECT run_id, queue_name, message_id
        FROM pgflow.step_tasks
        WHERE status = 'started' AND message_id IS NOT NULL
        LIMIT 1
        """).rows
        |> List.first()

      PgFlow.TestRepo.query!(
        """
        UPDATE pgflow.step_tasks
        SET requeued_count = 3,
            queued_at = now() - interval '3 hours',
            started_at = now() - interval '2 hours'
        WHERE run_id = $1
        """,
        [run_id]
      )

      assert message_in_queue?(queue_name, message_id)

      assert {:ok, 0} =
               Flows.recover_stalled_tasks(PgFlow.TestRepo, 60)

      refute message_in_queue?(queue_name, message_id)

      [[permanently_stalled_at]] =
        PgFlow.TestRepo.query!(
          "SELECT permanently_stalled_at IS NOT NULL FROM pgflow.step_tasks WHERE run_id = $1",
          [run_id]
        ).rows

      assert permanently_stalled_at
    end)
  end

  test "V05 downgrade fails with an actionable forward-only error" do
    fixture = baseline_fixture!()

    in_fixture(fixture, fn ->
      migrate_core_v02!()
      migrate_helpers_v05!()

      assert_raise Postgrex.Error, ~r/forward-only.*queue-aware claim/, fn ->
        Ecto.Migrator.down(
          PgFlow.TestRepo,
          9_000_000_105,
          PgFlow.Upstream.HelpersUpgradeMigration,
          log: false
        )
      end

      assert helpers_version() == 5
      assert start_tasks_signatures() == [[true, true]]
    end)
  end

  defp fresh_fixture! do
    fixture = UpstreamFixture.setup!([])
    Process.unlink(fixture.repo_pid)
    on_exit(fn -> UpstreamFixture.teardown!(fixture) end)

    in_fixture(fixture, fn ->
      PgFlow.TestRepo.query!("DROP SCHEMA IF EXISTS pgflow CASCADE")
    end)

    fixture
  end

  defp baseline_fixture! do
    fixture = UpstreamFixture.setup!()
    Process.unlink(fixture.repo_pid)
    on_exit(fn -> UpstreamFixture.teardown!(fixture) end)
    fixture
  end

  defp install_latest! do
    PgFlow.TestRepo.query!("CREATE EXTENSION IF NOT EXISTS pgmq")

    Ecto.Migrator.up(PgFlow.TestRepo, 9_000_000_200, PgFlow.Upstream.CoreOnlyMigration,
      log: false
    )

    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_202,
      PgFlow.Upstream.HelpersFullChainMigration,
      log: false
    )
  end

  defp migrate_core_v02! do
    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_102,
      PgFlow.Upstream.CoreUpgradeMigration,
      log: false
    )
  end

  defp migrate_helpers_v05! do
    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_105,
      PgFlow.Upstream.HelpersUpgradeMigration,
      log: false
    )
  end

  defp in_fixture(fixture, fun) do
    previous = PgFlow.TestRepo.put_dynamic_repo(fixture.repo_name)

    try do
      fun.()
    after
      PgFlow.TestRepo.put_dynamic_repo(previous)
    end
  end

  defp core_version do
    EvolverPostgres.get_version(PgFlow.TestRepo, "pgflow", {:view, "pgflow_version"})
  end

  defp helpers_version do
    EvolverPostgres.get_version(PgFlow.TestRepo, "pgflow", {:view, "extensions_version"})
  end

  defp scalar!(sql, params \\ []) do
    %{rows: [[value]]} = PgFlow.TestRepo.query!(sql, params)
    value
  end

  defp catalog_contract! do
    assert start_tasks_signatures() == [[true, true]]

    assert record_fields() ==
             ~w(flow_slug run_id step_slug input msg_id task_index flow_input attempts_count)

    assert queue_columns() == [["step_tasks", "queue_name"], ["steps", "queue_name"]]

    assert scalar!(
             "SELECT to_regprocedure('pgflow.ensure_flow_compiled(text,jsonb)') IS NOT NULL"
           )

    true
  end

  defp catalog_snapshot! do
    %{
      core_version: core_version(),
      helpers_version: helpers_version(),
      start_tasks: start_tasks_signatures(),
      record_fields: record_fields(),
      queue_columns: queue_columns(),
      compile_signature:
        scalar!("SELECT to_regprocedure('pgflow.ensure_flow_compiled(text,jsonb)') IS NOT NULL"),
      recover_signature:
        scalar!(
          "SELECT to_regprocedure('pgflow.recover_stalled_tasks(double precision)') IS NOT NULL"
        ),
      prune_signature:
        scalar!(
          "SELECT to_regprocedure('pgflow.prune_data_older_than(interval,text[])') IS NOT NULL"
        )
    }
  end

  defp queue_columns do
    PgFlow.TestRepo.query!("""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'pgflow'
      AND (table_name, column_name) IN (('steps', 'queue_name'), ('step_tasks', 'queue_name'))
    ORDER BY table_name
    """).rows
  end

  defp record_fields do
    PgFlow.TestRepo.query!("""
    SELECT attname
    FROM pg_attribute
    WHERE attrelid = (
      SELECT typrelid FROM pg_type WHERE oid = 'pgflow.step_task_record'::regtype
    )
      AND attnum > 0
      AND NOT attisdropped
    ORDER BY attnum
    """).rows
    |> List.flatten()
  end

  defp start_tasks_signatures do
    PgFlow.TestRepo.query!("""
    SELECT
      to_regprocedure('pgflow.start_tasks(text,bigint[],uuid,text)') IS NOT NULL,
      to_regprocedure('pgflow.start_tasks(text,bigint[],uuid)') IS NULL
    """).rows
  end

  defp message_in_queue?(queue_name, message_id) do
    %{rows: [[exists?]]} =
      PgFlow.TestRepo.query!(
        """
        SELECT EXISTS(
          SELECT 1
          FROM pgmq.read($1, 0, 100)
          WHERE msg_id = $2
        )
        """,
        [queue_name, message_id]
      )

    exists?
  end
end
