defmodule PgFlow.Upstream.HelpersV07UpgradeTest do
  use ExUnit.Case, async: false

  alias EctoEvolver.Adapters.Postgres, as: EvolverPostgres
  alias PgFlow.Test.UpstreamFixture

  @moduletag :integration
  @moduletag :migration

  test "installs the five-argument claim and four-argument register_worker" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_core_v03!()
      migrate_helpers_v06!()
      migrate_helpers_v07!()

      assert function_arguments("start_tasks") == [
               "flow_slug text, msg_ids bigint[], worker_id uuid, queue_name text, step_slug text"
             ]

      assert function_arguments("register_worker") == [
               "p_worker_id uuid, p_queue_name text, p_function_name text, p_pgflow_version text"
             ]

      assert helpers_version() == 7
    end)
  end

  test "refuses to install on core V02 and leaves the helper version unchanged" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_core_v02!()
      migrate_helpers_v06!()

      assert_raise Postgrex.Error, ~r/PgFlow helpers V07 requires core V03/, fn ->
        migrate_helpers_v07!()
      end

      assert helpers_version() == 6
      refute column_exists?("flows", "queue_mode")
    end)
  end

  test "register_worker stamps pgflow_version and keeps started_at" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_core_v03!()
      migrate_helpers_v07!()

      %{rows: [[worker_id]]} = PgFlow.TestRepo.query!("SELECT gen_random_uuid()")

      PgFlow.TestRepo.query!(
        "SELECT pgflow.register_worker($1, 'q', 'elixir:Mod', 'elixir-0.7.0')",
        [worker_id]
      )

      [started_at, first_heartbeat, version] = worker_row!(worker_id)

      PgFlow.TestRepo.query!(
        "SELECT pgflow.register_worker($1, 'q', 'elixir:Mod', 'elixir-0.7.0')",
        [worker_id]
      )

      [second_started, second_heartbeat, second_version] = worker_row!(worker_id)

      assert version == "elixir-0.7.0"
      assert second_version == "elixir-0.7.0"
      assert second_started == started_at
      assert DateTime.compare(second_heartbeat, first_heartbeat) != :lt
    end)
  end

  test "flow-mode start_tasks returns attempts_count 1" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate_core_v03!()
      migrate_helpers_v07!()

      [flow_slug, message_id, queue_name] =
        PgFlow.TestRepo.query!("""
        SELECT flow_slug, message_id, queue_name
        FROM pgflow.step_tasks
        WHERE status = 'queued' AND message_id IS NOT NULL
        LIMIT 1
        """).rows
        |> List.first()

      %{rows: [[worker_id]]} = PgFlow.TestRepo.query!("SELECT gen_random_uuid()")

      PgFlow.TestRepo.query!(
        "SELECT pgflow.register_worker($1, $2, 'elixir:Mod', 'elixir-0.7.0')",
        [worker_id, queue_name]
      )

      assert [[1]] ==
               PgFlow.TestRepo.query!(
                 """
                 SELECT attempts_count
                 FROM pgflow.start_tasks($1, ARRAY[$2]::bigint[], $3, $4, NULL::text)
                 """,
                 [flow_slug, message_id, worker_id, queue_name]
               ).rows
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

  defp migrate_core_v02! do
    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_102,
      PgFlow.Upstream.CoreUpgradeMigration,
      log: false
    )
  end

  defp migrate_core_v03! do
    Ecto.Migrator.up(
      PgFlow.TestRepo,
      9_000_000_203,
      PgFlow.Upstream.CoreV03Migration,
      log: false
    )
  end

  defp migrate_helpers_v06! do
    Ecto.Migrator.up(PgFlow.TestRepo, 9_000_000_205, PgFlow.Upstream.HelpersUpgradeMigration,
      log: false
    )

    Ecto.Migrator.up(PgFlow.TestRepo, 9_000_000_206, PgFlow.Upstream.HelpersV06Migration,
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

  defp helpers_version do
    EvolverPostgres.get_version(PgFlow.TestRepo, "pgflow", {:view, "extensions_version"})
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

  defp column_exists?(table, column) do
    %{rows: [[exists?]]} =
      PgFlow.TestRepo.query!(
        """
        SELECT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'pgflow' AND table_name = $1 AND column_name = $2
        )
        """,
        [table, column]
      )

    exists?
  end

  defp worker_row!(worker_id) do
    %{rows: [row]} =
      PgFlow.TestRepo.query!(
        """
        SELECT started_at, last_heartbeat_at, pgflow_version
        FROM pgflow.workers
        WHERE worker_id = $1
        """,
        [worker_id]
      )

    row
  end
end
