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

  test "in-flight run completes after V03 upgrade" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_v02!()
      migrate_helpers_v05!()
      migrate_helpers_v06!()

      old = seed_in_flight!()
      before_checksum = state_checksum()
      before_messages = queue_messages()

      migrate_v03!()
      migrate_helpers_v07!()

      assert core_version() == 3
      assert state_checksum() == before_checksum
      assert queue_messages() == before_messages

      assert scalar!("""
             SELECT count(*) FROM pgflow.step_tasks AS t
             WHERE t.flow_slug = 'upg_main'
               AND t.status IN ('queued', 'started')
               AND NOT EXISTS (
                 SELECT 1 FROM pgmq.q_upg_main AS q WHERE q.msg_id = t.message_id
               )
             """) == 0

      assert run_status(old.done) == "completed"
      assert run_status(old.failed) == "failed"

      assert scalar!("SELECT count(*) FROM pgflow.workers WHERE worker_id = $1::text::uuid", [
               old.worker
             ]) == 1

      # Run 1: b was claimed under the old signature; finishing it releases c.
      assert task_status(old.started, "b") == "started"
      assert complete_task!(old.started, "b", %{"b" => 1}) == "completed"
      assert task_status(old.started, "c") == "queued"
      assert claim!(old.started, "c", old.worker) == 1
      assert complete_task!(old.started, "c", %{"c" => 1}) == "completed"
      assert run_status(old.started) == "completed"

      # Run 2: both root tasks were queued before the upgrade and are claimed
      # through the five-argument signature.
      for step <- ["a", "b"] do
        assert task_status(old.queued, step) == "queued"
        assert claim!(old.queued, step, old.worker) == 1
        assert complete_task!(old.queued, step, %{step => 1}) == "completed"
      end

      assert claim!(old.queued, "c", old.worker) == 1
      assert complete_task!(old.queued, "c", %{"c" => 2}) == "completed"
      assert run_status(old.queued) == "completed"
    end)
  end

  test "a failed V03 down migration leaves seeded data intact" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_v02!()
      migrate_helpers_v05!()
      migrate_helpers_v06!()
      seed_in_flight!()
      migrate_v03!()

      checksum = state_checksum()
      messages = queue_messages()

      assert_raise Postgrex.Error, ~r/PgFlow core V03 is forward-only/, fn ->
        Ecto.Migrator.down(
          PgFlow.TestRepo,
          9_000_000_203,
          PgFlow.Upstream.CoreV03Migration,
          log: false
        )
      end

      assert core_version() == 3
      assert state_checksum() == checksum
      assert queue_messages() == messages
      assert column_exists?("flows", "queue_mode")
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

  defp migrate_helpers_v07! do
    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_207,
      PgFlow.Upstream.HelpersV07Migration,
      log: false
    )
  end

  # Seeds, on a V02/V06 schema: a run with `b` claimed (old signature) and `c`
  # still blocked, a run with every root task queued, a completed run, and a
  # run whose only task failed.
  defp seed_in_flight! do
    worker = scalar!("SELECT gen_random_uuid()::text")

    PgFlow.TestRepo.query!(
      "SELECT pgflow.register_worker($1::text::uuid, 'upg_main', 'elixir:Up')",
      [worker]
    )

    PgFlow.TestRepo.query!("SELECT * FROM pgflow.create_flow('upg_main')")
    add_step!("upg_main", "a", [])
    add_step!("upg_main", "b", [])
    add_step!("upg_main", "c", ["a", "b"])

    PgFlow.TestRepo.query!("SELECT * FROM pgflow.create_flow('upg_done')")
    add_step!("upg_done", "only", [])

    PgFlow.TestRepo.query!("SELECT * FROM pgflow.create_flow('upg_fail', 1, 1, 60)")
    add_step!("upg_fail", "only", [])

    started = start_run!("upg_main")
    queued = start_run!("upg_main")
    done = start_run!("upg_done")
    failed = start_run!("upg_fail")

    old_claim!(started, "upg_main", "a", worker)
    old_claim!(started, "upg_main", "b", worker)
    complete_task!(started, "a", %{"a" => 1})

    old_claim!(done, "upg_done", "only", worker)
    complete_task!(done, "only", %{"ok" => true})

    old_claim!(failed, "upg_fail", "only", worker)

    PgFlow.TestRepo.query!("SELECT * FROM pgflow.fail_task($1::text::uuid, 'only', 0, 'boom')", [
      failed
    ])

    assert run_status(started) == "started"
    assert run_status(done) == "completed"
    assert run_status(failed) == "failed"
    assert task_status(started, "b") == "started"
    assert task_status(queued, "a") == "queued"

    %{worker: worker, started: started, queued: queued, done: done, failed: failed}
  end

  defp add_step!(flow, step, deps) do
    PgFlow.TestRepo.query!(
      "SELECT * FROM pgflow.add_step($1, $2, $3::text[], null, null, null, null, 'single')",
      [flow, step, deps]
    )
  end

  defp start_run!(flow) do
    scalar!("SELECT (pgflow.start_flow($1, '{}'::jsonb)).run_id::text", [flow])
  end

  defp old_claim!(run_id, flow, step, worker) do
    msg_id = message_id!(run_id, step)

    %{num_rows: 1} =
      PgFlow.TestRepo.query!(
        "SELECT * FROM pgflow.start_tasks($1, ARRAY[$2]::bigint[], $3::text::uuid, $4)",
        [flow, msg_id, worker, flow]
      )
  end

  defp claim!(run_id, step, worker) do
    msg_id = message_id!(run_id, step)

    PgFlow.TestRepo.query!(
      """
      SELECT * FROM pgflow.start_tasks('upg_main', ARRAY[$1]::bigint[], $2::text::uuid, 'upg_main', NULL::text)
      """,
      [msg_id, worker]
    ).num_rows
  end

  defp complete_task!(run_id, step, output) do
    PgFlow.TestRepo.query!(
      "SELECT * FROM pgflow.complete_task($1::text::uuid, $2, 0, $3::text::jsonb)",
      [run_id, step, Jason.encode!(output)]
    )

    task_status(run_id, step)
  end

  defp message_id!(run_id, step) do
    scalar!(
      "SELECT message_id FROM pgflow.step_tasks WHERE run_id = $1::text::uuid AND step_slug = $2",
      [run_id, step]
    )
  end

  defp task_status(run_id, step) do
    scalar!(
      "SELECT status FROM pgflow.step_tasks WHERE run_id = $1::text::uuid AND step_slug = $2",
      [run_id, step]
    )
  end

  defp run_status(run_id) do
    scalar!("SELECT status FROM pgflow.runs WHERE run_id = $1::text::uuid", [run_id])
  end

  # Content checksums over columns that exist before and after the upgrade.
  defp state_checksum do
    PgFlow.TestRepo.query!("""
    SELECT
      (SELECT md5(coalesce(string_agg(concat_ws('|', run_id, flow_slug, status, input, output, remaining_steps), ';' ORDER BY run_id), ''))
         FROM pgflow.runs),
      (SELECT md5(coalesce(string_agg(concat_ws('|', run_id, step_slug, status, remaining_deps, remaining_tasks), ';' ORDER BY run_id, step_slug), ''))
         FROM pgflow.step_states),
      (SELECT md5(coalesce(string_agg(concat_ws('|', run_id, step_slug, task_index, status, attempts_count, message_id, queue_name, output, error_message), ';' ORDER BY run_id, step_slug, task_index), ''))
         FROM pgflow.step_tasks)
    """).rows
  end

  defp queue_messages do
    Enum.map(~w(upg_main upg_done upg_fail), fn queue ->
      PgFlow.TestRepo.query!(
        "SELECT msg_id, read_ct, vt, message FROM pgmq.q_#{queue} ORDER BY msg_id"
      ).rows
    end)
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
