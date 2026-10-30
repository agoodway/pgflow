defmodule PgFlow.Upstream.CoreV03UpgradeTest do
  use ExUnit.Case, async: false

  alias EctoEvolver.Adapters.Postgres, as: EvolverPostgres
  alias PgFlow.Test.UpstreamFixture

  @moduletag :integration
  @moduletag :migration

  test "installs V01 through V03 on a fresh core-only schema" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      PgFlow.TestRepo.query!("DROP SCHEMA pgflow CASCADE")
      migrate_v03!()

      assert core_version() == 3
      assert column_default("flows", "queue_mode") =~ "flow"

      assert function_arguments("start_tasks") == [
               "flow_slug text, msg_ids bigint[], worker_id uuid, queue_name text, step_slug text"
             ]

      assert function_arguments("ensure_flow_compiled") == [
               "flow_slug text, shape jsonb, queue_mode text, route_map jsonb"
             ]

      assert column_default("workers", "pgflow_version") == nil
      refute indexdef("idx_runs_started_at") =~ "DESC"
      assert PgFlow.TestRepo.query!("SHOW lock_timeout").rows == [["0"]]
    end)
  end

  test "upgrades V02 with helpers V06 and preserves seeded rows" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_v02!()
      migrate_helpers_v05!()
      migrate_helpers_v06!()
      seed_flow!()

      counts = table_counts()

      queue_name =
        scalar!("SELECT queue_name FROM pgflow.step_tasks WHERE flow_slug = 'seed_flow'")

      migrate_v03!()

      assert table_counts() == counts

      assert scalar!("SELECT queue_name FROM pgflow.step_tasks WHERE flow_slug = 'seed_flow'") ==
               queue_name

      assert List.last(record_fields()) == "attempts_count"

      assert scalar!("SELECT pgflow.is_valid_slug('a__b')") == false
    end)
  end

  test "slug pre-flight rolls the V03 transaction back" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_v02!()
      PgFlow.TestRepo.query!("ALTER TABLE pgflow.flows DROP CONSTRAINT slug_is_valid")
      PgFlow.TestRepo.query!("INSERT INTO pgflow.flows (flow_slug) VALUES ('bad__flow')")

      error =
        assert_raise Postgrex.Error, fn ->
          migrate_v03!()
        end

      assert error.postgres.code == :check_violation
      assert error.postgres.message =~ "bad__flow"
      assert core_version() == 2
      assert column_exists?("flows", "queue_mode") == false
    end)
  end

  test "case-insensitive step slugs fail the V03 pre-flight" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_v02!()

      PgFlow.TestRepo.query!("""
      INSERT INTO pgflow.flows (flow_slug) VALUES ('case_flow')
      """)

      PgFlow.TestRepo.query!("""
      INSERT INTO pgflow.steps (flow_slug, step_slug, step_index, queue_name)
      VALUES ('case_flow', 'fetch', 0, 'case_flow'), ('case_flow', 'Fetch', 1, 'case_flow')
      """)

      error =
        assert_raise Postgrex.Error, fn ->
          migrate_v03!()
        end

      assert error.postgres.code == :check_violation
      assert error.postgres.message =~ "step_collision"
      assert error.postgres.message =~ "Fetch,fetch"
      assert core_version() == 2
    end)
  end

  test "V03 downgrade fails with the forward-only error" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      PgFlow.TestRepo.query!("DROP SCHEMA pgflow CASCADE")
      migrate_v03!()

      assert_raise Postgrex.Error, ~r/PgFlow core V03 is forward-only/, fn ->
        Ecto.Migrator.down(
          PgFlow.TestRepo,
          9_000_000_203,
          PgFlow.Upstream.CoreV03Migration,
          log: false
        )
      end

      assert core_version() == 3
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

  defp migrate_helpers_v05! do
    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_105,
      PgFlow.Upstream.HelpersUpgradeMigration,
      log: false
    )
  end

  defp migrate_helpers_v06! do
    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_106,
      PgFlow.Upstream.HelpersV06Migration,
      log: false
    )
  end

  defp migrate_v03! do
    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_203,
      PgFlow.Upstream.CoreV03Migration,
      log: false
    )
  end

  defp seed_flow! do
    PgFlow.TestRepo.query!("INSERT INTO pgflow.flows (flow_slug) VALUES ('seed_flow')")

    PgFlow.TestRepo.query!("""
    INSERT INTO pgflow.steps (flow_slug, step_slug, step_index, queue_name)
    VALUES ('seed_flow', 'fetch', 0, 'seed_flow'), ('seed_flow', 'store', 1, 'seed_flow')
    """)

    %{rows: [[run_id]]} =
      PgFlow.TestRepo.query!(
        "INSERT INTO pgflow.runs (flow_slug, status, input, remaining_steps) VALUES ('seed_flow', 'started', '{}', 2) RETURNING run_id"
      )

    PgFlow.TestRepo.query!(
      """
      INSERT INTO pgflow.step_states (flow_slug, run_id, step_slug, status)
      VALUES ($1, $2, 'fetch', 'created'), ($1, $2, 'store', 'created')
      """,
      ["seed_flow", run_id]
    )

    PgFlow.TestRepo.query!(
      """
      INSERT INTO pgflow.step_tasks
        (flow_slug, run_id, step_slug, task_index, message_id, status, queue_name)
      VALUES ($1, $2, 'fetch', 0, 1, 'queued', 'seed_flow')
      """,
      ["seed_flow", run_id]
    )
  end

  defp table_counts do
    ~w(flows steps runs step_tasks)
    |> Map.new(fn table ->
      {table, scalar!("SELECT count(*) FROM pgflow.#{table}")}
    end)
  end

  defp core_version do
    EvolverPostgres.get_version(PgFlow.TestRepo, "pgflow", {:view, "pgflow_version"})
  end

  defp scalar!(sql, params \\ []) do
    %{rows: [[value]]} = PgFlow.TestRepo.query!(sql, params)
    value
  end

  defp column_default(table, column) do
    scalar!(
      """
      SELECT column_default
      FROM information_schema.columns
      WHERE table_schema = 'pgflow' AND table_name = $1 AND column_name = $2
      """,
      [table, column]
    )
  end

  defp column_exists?(table, column) do
    scalar!(
      """
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'pgflow' AND table_name = $1 AND column_name = $2
      )
      """,
      [table, column]
    )
  end

  defp function_arguments(name) do
    PgFlow.TestRepo.query!(
      """
      SELECT pg_get_function_identity_arguments(p.oid)
      FROM pg_proc AS p
      JOIN pg_namespace AS n ON n.oid = p.pronamespace
      WHERE n.nspname = 'pgflow' AND p.proname = $1
      ORDER BY 1
      """,
      [name]
    ).rows
    |> List.flatten()
  end

  defp indexdef(name) do
    scalar!("SELECT pg_get_indexdef(to_regclass($1))", ["pgflow." <> name])
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
end
