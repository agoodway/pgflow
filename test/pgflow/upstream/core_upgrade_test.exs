defmodule PgFlow.Upstream.CoreUpgradeTest do
  use ExUnit.Case, async: false

  alias EctoEvolver.Adapters.Postgres, as: EvolverPostgres
  alias PgFlow.Test.UpstreamFixture

  @moduletag :integration
  @moduletag :migration

  test "installs V01 and V02 on a fresh core-only schema" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      PgFlow.TestRepo.query!("DROP SCHEMA pgflow CASCADE")
      migrate_v02!()

      assert core_version() == 2
      assert record_fields() == ~w(flow_slug run_id step_slug input msg_id task_index flow_input)
      assert queue_columns() == [["step_tasks", "queue_name"], ["steps", "queue_name"]]
      assert start_tasks_signatures() == [[true, true]]
      assert PgFlow.TestRepo.query!("SHOW lock_timeout").rows == [["0"]]
    end)
  end

  test "upgrades core V01 with helpers V04 and preserves seeded data" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      task_count = scalar!("SELECT count(*) FROM pgflow.step_tasks")

      message_count =
        scalar!("SELECT count(*) FROM pgflow.step_tasks WHERE message_id IS NOT NULL")

      migrate_v02!()

      assert core_version() == 2
      assert scalar!("SELECT count(*) FROM pgflow.step_tasks") == task_count

      assert scalar!("SELECT count(*) FROM pgflow.step_tasks WHERE message_id IS NOT NULL") ==
               message_count

      assert scalar!("SELECT count(*) FROM pgflow.flows WHERE flow_type = 'job'") == 1

      assert scalar!("SELECT count(*) FROM pgflow.step_tasks WHERE queue_name = lower(flow_slug)") ==
               task_count

      assert record_fields() ==
               ~w(flow_slug run_id step_slug input msg_id task_index flow_input attempts_count)

      assert start_tasks_signatures() == [[true, true]]
    end)
  end

  test "conflicting normalized slugs roll back the complete V02 transaction" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      PgFlow.TestRepo.query!(
        "INSERT INTO pgflow.flows (flow_slug) VALUES ('collision'), ('COLLISION')"
      )

      assert_raise Postgrex.Error, ~r/idx_flows_normalized_slug|duplicate key/, fn ->
        migrate_v02!()
      end

      assert core_version() == 1
      assert queue_columns() == []

      assert scalar!("SELECT count(*) FROM pgflow.flows WHERE lower(flow_slug) = 'collision'") ==
               2
    end)
  end

  test "duplicate queue and message identities roll back the complete V02 transaction" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      PgFlow.TestRepo.query!("DROP INDEX pgflow.idx_step_tasks_message_id")

      [run_id, flow_slug] =
        PgFlow.TestRepo.query!(
          "SELECT run_id, flow_slug FROM pgflow.step_tasks WHERE message_id IS NOT NULL LIMIT 1"
        ).rows
        |> List.first()

      PgFlow.TestRepo.query!(
        """
        INSERT INTO pgflow.step_tasks (flow_slug, run_id, step_slug, task_index, message_id)
        VALUES ($1, $2, 'fixture_step', 100, (
          SELECT message_id
          FROM pgflow.step_tasks
          WHERE run_id = $2 AND message_id IS NOT NULL
          LIMIT 1
        ))
        """,
        [flow_slug, run_id]
      )

      PgFlow.TestRepo.query!(
        "CREATE INDEX idx_step_tasks_message_id ON pgflow.step_tasks (message_id)"
      )

      assert_raise Postgrex.Error, ~r/idx_step_tasks_queue_message|duplicate key/, fn ->
        migrate_v02!()
      end

      assert core_version() == 1
      assert queue_columns() == []
      assert scalar!("SELECT count(*) FROM pgflow.step_tasks WHERE run_id = $1", [run_id]) >= 2
    end)
  end

  test "V02 downgrade fails with an actionable forward-only error" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_v02!()

      assert_raise Postgrex.Error, ~r/forward-only.*statuses.*queue identities/is, fn ->
        Ecto.Migrator.down(
          PgFlow.TestRepo,
          9_000_000_102,
          PgFlow.Upstream.CoreUpgradeMigration,
          log: false
        )
      end

      assert core_version() == 2
    end)
  end

  defp fixture! do
    fixture = UpstreamFixture.setup!()
    Process.unlink(fixture.repo_pid)
    on_exit(fn -> UpstreamFixture.teardown!(fixture) end)
    fixture
  end

  defp in_fixture(fixture, fun) do
    previous = PgFlow.TestRepo.put_dynamic_repo(fixture.repo_name)

    try do
      fun.()
    after
      PgFlow.TestRepo.put_dynamic_repo(previous)
    end
  end

  defp migrate_v02! do
    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_102,
      PgFlow.Upstream.CoreUpgradeMigration,
      log: false
    )
  end

  defp core_version do
    EvolverPostgres.get_version(PgFlow.TestRepo, "pgflow", {:view, "pgflow_version"})
  end

  defp scalar!(sql, params \\ []) do
    %{rows: [[value]]} = PgFlow.TestRepo.query!(sql, params)
    value
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
end
