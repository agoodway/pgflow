defmodule PgFlow.Upstream.HelpersV07UpgradeTest do
  use ExUnit.Case, async: false

  import PgFlow.Test.UpstreamUpgrade

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.SchemaCheck

  @moduletag :integration
  @moduletag :migration

  test "installs the five-argument claim and four-argument register_worker" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate!(:core_v03)
      migrate!([:helpers_v05, :helpers_v06])
      migrate!(:helpers_v07)

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
      migrate!(:core_v02)
      migrate!([:helpers_v05, :helpers_v06])

      assert_raise Postgrex.Error, ~r/PgFlow helpers V07 requires core V03/, fn ->
        migrate!(:helpers_v07)
      end

      assert helpers_version() == 6
      refute column_exists?("flows", "queue_mode")
    end)
  end

  test "register_worker stamps pgflow_version and keeps started_at" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate!(:core_v03)
      migrate!(:helpers_v07)

      %{rows: [[worker_id]]} = PgFlow.TestRepo.query!("SELECT gen_random_uuid()")

      PgFlow.TestRepo.query!(
        "SELECT pgflow.register_worker($1, 'q', 'elixir:Mod', 'elixir-0.6.0')",
        [worker_id]
      )

      [started_at, first_heartbeat, version] = worker_row!(worker_id)

      PgFlow.TestRepo.query!(
        "SELECT pgflow.register_worker($1, 'q', 'elixir:Mod', 'elixir-0.6.0')",
        [worker_id]
      )

      [second_started, second_heartbeat, second_version] = worker_row!(worker_id)

      assert version == "elixir-0.6.0"
      assert second_version == "elixir-0.6.0"
      assert second_started == started_at
      assert DateTime.compare(second_heartbeat, first_heartbeat) != :lt
    end)
  end

  test "flow-mode start_tasks returns attempts_count 1" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate!(:core_v03)
      migrate!(:helpers_v07)

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
        "SELECT pgflow.register_worker($1, $2, 'elixir:Mod', 'elixir-0.6.0')",
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

  test "installs delete_run and the run-first prune lock order" do
    fixture = fixture!()

    in_fixture(fixture, fn ->
      migrate!(:core_v03)
      migrate!(:helpers_v07)

      assert function_arguments("delete_run") == ["p_run_id uuid"]

      prune_source =
        scalar!(
          "SELECT prosrc FROM pg_proc WHERE oid = 'pgflow.prune_data_older_than(interval,text[])'::regprocedure"
        )

      assert prune_source =~ "ORDER BY r.run_id\n    FOR UPDATE"
      assert prune_source =~ "ORDER BY st.run_id, st.step_slug, st.task_index\n    FOR UPDATE"

      run_id = scalar!("SELECT run_id FROM pgflow.runs ORDER BY run_id LIMIT 1")
      PgFlow.TestRepo.query!("SELECT pgflow.delete_run($1)", [run_id])

      assert scalar!(
               """
               SELECT (SELECT count(*) FROM pgflow.runs WHERE run_id = $1)
                    + (SELECT count(*) FROM pgflow.step_states WHERE run_id = $1)
                    + (SELECT count(*) FROM pgflow.step_tasks WHERE run_id = $1)
               """,
               [run_id]
             ) == 0
    end)
  end

  test "schema check passes fresh V07 and flags a pre-release V07 without delete_run" do
    # The ordinary test database carries a fresh V07 install. The sandbox
    # transaction undoes the drop that simulates a pre-release V07 database.
    :ok = Sandbox.checkout(PgFlow.TestRepo)
    on_exit(fn -> Sandbox.mode(PgFlow.TestRepo, :manual) end)

    assert :ok = SchemaCheck.run(PgFlow.TestRepo)

    PgFlow.TestRepo.query!("DROP FUNCTION pgflow.delete_run(uuid)")

    assert helpers_version() == 7
    assert {:error, errors} = SchemaCheck.run(PgFlow.TestRepo)

    assert [message] = Enum.filter(errors, &String.contains?(&1, "pgflow.delete_run(uuid)"))
    assert message =~ "re-apply the helpers V07"
    assert :ok = SchemaCheck.runtime_checks(PgFlow.TestRepo)
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
