defmodule PgFlow.RunLifecycleTest do
  use PgFlow.IntegrationCase

  alias PgFlow.Runs
  alias PgFlow.Schema.Run

  describe "count_queue_messages/4" do
    test "counts one or many runs across live and archived messages without double-counting IDs" do
      create_flow("count_messages")
      add_step("count_messages", "work", type: "map")
      first_run_id = start_flow_run("count_messages", ["first-live", "first-archive"])
      second_run_id = start_flow_run("count_messages", ["second-a", "second-b"])
      unrelated_run_id = start_flow_run("count_messages", ["unrelated"])
      [archived_message_id | _] = task_message_ids(first_run_id)

      TestRepo.query!("SELECT pgmq.archive($1::text, $2::bigint)", [
        "count_messages",
        archived_message_id
      ])

      assert {:ok, 2} = Runs.count_queue_messages(TestRepo, "count_messages", first_run_id)

      assert {:ok, 1} =
               Runs.count_queue_messages(TestRepo, "count_messages", first_run_id,
                 location: :live
               )

      assert {:ok, 1} =
               Runs.count_queue_messages(TestRepo, "count_messages", first_run_id,
                 location: :archive
               )

      assert {:ok, 4} =
               Runs.count_queue_messages(TestRepo, "count_messages", [
                 first_run_id,
                 second_run_id,
                 first_run_id
               ])

      assert {:ok, 1} =
               Runs.count_queue_messages(TestRepo, "count_messages", unrelated_run_id)
    end

    test "counts live and archive tables in one combined query for a consistent snapshot" do
      create_flow("count_single_snapshot")
      add_step("count_single_snapshot", "work", type: "map")
      run_id = start_flow_run("count_single_snapshot", ["live", "archive"])
      [archived_message_id | _] = task_message_ids(run_id)

      TestRepo.query!("SELECT pgmq.archive($1::text, $2::bigint)", [
        "count_single_snapshot",
        archived_message_id
      ])

      handler_id = "count-single-snapshot-#{System.unique_integer([:positive])}"
      event = TestRepo.config()[:telemetry_prefix] ++ [:query]

      :telemetry.attach(
        handler_id,
        event,
        fn _event, _measurements, metadata, test_pid ->
          send(test_pid, {:count_query, metadata.query})
        end,
        self()
      )

      try do
        assert {:ok, 2} = Runs.count_queue_messages(TestRepo, "count_single_snapshot", run_id)
      after
        :telemetry.detach(handler_id)
      end

      payload_count_queries =
        []
        |> collect_count_queries()
        |> Enum.filter(&String.contains?(&1, "message->>'run_id'"))

      assert [combined_query] = payload_count_queries
      assert combined_query =~ "UNION ALL"
      assert combined_query =~ ~s("pgmq".q_count_single_snapshot)
      assert combined_query =~ ~s("pgmq".a_count_single_snapshot)
    end

    test "counts durable orphaned messages after relational lifecycle rows disappear" do
      create_flow("count_orphaned_messages")
      add_step("count_orphaned_messages", "work")
      run_id = start_flow_run("count_orphaned_messages", %{})

      with_foreign_key_checks_disabled(fn ->
        TestRepo.query!("DELETE FROM pgflow.step_tasks WHERE run_id = $1", [
          Ecto.UUID.dump!(run_id)
        ])

        TestRepo.query!("DELETE FROM pgflow.step_states WHERE run_id = $1", [
          Ecto.UUID.dump!(run_id)
        ])

        TestRepo.query!("DELETE FROM pgflow.runs WHERE run_id = $1", [Ecto.UUID.dump!(run_id)])
      end)

      assert {:ok, 1} = Runs.count_queue_messages(TestRepo, "count_orphaned_messages", run_id)
    end

    test "uses PGMQ canonical queue names and treats missing queues and empty IDs as empty" do
      create_flow("MixedCaseCount")
      add_step("MixedCaseCount", "work", type: "map")
      run_id = start_flow_run("MixedCaseCount", ["a", "b"])

      assert {:ok, 2} = Runs.count_queue_messages(TestRepo, "MixedCaseCount", run_id)
      assert {:ok, 0} = Runs.count_queue_messages(TestRepo, "MixedCaseCount", [])

      # An empty run-ID list short-circuits before the flow-slug DB validation,
      # so it returns {:ok, 0} even for a flow slug that doesn't exist.
      assert {:ok, 0} = Runs.count_queue_messages(TestRepo, "no_such_flow_slug", [])

      TestRepo.query!("SELECT pgmq.drop_queue($1::text)", ["MixedCaseCount"])

      assert {:ok, 0} = Runs.count_queue_messages(TestRepo, "MixedCaseCount", run_id)
    end

    test "rejects invalid IDs, flow slugs, and locations" do
      run_id = Ecto.UUID.generate()

      assert {:error, :invalid_id} =
               Runs.count_queue_messages(TestRepo, "count_messages", "not-a-uuid")

      assert {:error, :invalid_id} =
               Runs.count_queue_messages(TestRepo, "count_messages", [run_id, "not-a-uuid"])

      assert {:error, :invalid_flow_slug} =
               Runs.count_queue_messages(TestRepo, "invalid-slug", run_id)

      assert {:error, :invalid_flow_slug} = Runs.count_queue_messages(TestRepo, 123, run_id)

      assert {:error, :invalid_location} =
               Runs.count_queue_messages(TestRepo, "count_messages", run_id, location: :somewhere)
    end

    test "returns tagged database errors" do
      create_flow("count_database_error")
      add_step("count_database_error", "work")
      run_id = start_flow_run("count_database_error", %{})
      TestRepo.query!("SELECT pgmq.drop_queue($1::text)", ["count_database_error"])
      TestRepo.query!("CREATE TABLE pgmq.q_count_database_error (message integer)")

      try do
        assert {:error, %Postgrex.Error{}} =
                 Runs.count_queue_messages(TestRepo, "count_database_error", run_id,
                   location: :live
                 )
      after
        TestRepo.query!("DROP TABLE IF EXISTS pgmq.q_count_database_error")
      end
    end
  end

  describe "make_available/2" do
    test "exposes every delayed single and map task for only the requested run" do
      create_flow("available_single")
      add_step_with_retry_options("available_single", "work", start_delay: 3_600)
      single_run_id = start_flow_run("available_single", %{"run" => "target"})
      other_run_id = start_flow_run("available_single", %{"run" => "other"})

      create_flow("available_map")
      add_step_with_retry_options("available_map", "work", start_delay: 3_600, type: "map")
      map_run_id = start_flow_run("available_map", ["a", "b", "c"])

      assert queue_visibility("available_single", single_run_id) == {0, 1}
      assert queue_visibility("available_single", other_run_id) == {0, 1}
      assert queue_visibility("available_map", map_run_id) == {0, 3}

      assert :ok = Runs.make_available(TestRepo, single_run_id)
      assert :ok = Runs.make_available(TestRepo, map_run_id)

      assert queue_visibility("available_single", single_run_id) == {1, 1}
      assert queue_visibility("available_single", other_run_id) == {0, 1}
      assert queue_visibility("available_map", map_run_id) == {3, 3}
    end

    test "rejects invalid IDs and treats absent runs or queues as no-ops" do
      assert {:error, :invalid_id} = Runs.make_available(TestRepo, "not-a-uuid")
      assert :ok = Runs.make_available(TestRepo, Ecto.UUID.generate())

      create_flow("available_missing_queue")
      add_step_with_retry_options("available_missing_queue", "work", start_delay: 3_600)
      run_id = start_flow_run("available_missing_queue", %{})
      TestRepo.query!("SELECT pgmq.drop_queue($1::text)", ["available_missing_queue"])

      assert :ok = Runs.make_available(TestRepo, run_id)
      assert {:ok, %Run{run_id: ^run_id}} = Runs.get(TestRepo, run_id)
    end

    test "uses PGMQ's canonical queue name for a mixed-case flow" do
      create_flow("MixedCaseAvailable")

      add_step_with_retry_options("MixedCaseAvailable", "work",
        start_delay: 3_600,
        type: "map"
      )

      run_id = start_flow_run("MixedCaseAvailable", ["a", "b"])
      other_run_id = start_flow_run("MixedCaseAvailable", ["other"])

      assert queue_visibility("MixedCaseAvailable", run_id) == {0, 2}
      assert queue_visibility("MixedCaseAvailable", other_run_id) == {0, 1}

      assert :ok = Runs.make_available(TestRepo, run_id)

      assert queue_visibility("MixedCaseAvailable", run_id) == {2, 2}
      assert queue_visibility("MixedCaseAvailable", other_run_id) == {0, 1}
    end
  end

  describe "delete/2" do
    test "removes the exact run from live and archived queues and relational tables" do
      create_flow("delete_run")
      add_step("delete_run", "work", type: "map")
      run_id = start_flow_run("delete_run", ["target-live", "target-archive"])
      other_run_id = start_flow_run("delete_run", ["other-a", "other-b"])
      [archived_message_id | _] = task_message_ids(run_id)
      [other_archived_message_id | _] = task_message_ids(other_run_id)

      Enum.each([archived_message_id, other_archived_message_id], fn message_id ->
        TestRepo.query!("SELECT pgmq.archive($1::text, $2::bigint)", ["delete_run", message_id])
      end)

      assert queue_count("q", "delete_run", run_id) == 1
      assert queue_count("a", "delete_run", run_id) == 1
      assert queue_count("q", "delete_run", other_run_id) == 1
      assert queue_count("a", "delete_run", other_run_id) == 1
      assert relational_counts(run_id) == {1, 2, 1}

      assert :ok = Runs.delete(TestRepo, run_id)

      assert queue_count("q", "delete_run", run_id) == 0
      assert queue_count("a", "delete_run", run_id) == 0
      assert queue_count("q", "delete_run", other_run_id) == 1
      assert queue_count("a", "delete_run", other_run_id) == 1
      assert relational_counts(run_id) == {0, 0, 0}
      assert {:ok, %Run{run_id: ^other_run_id}} = Runs.get(TestRepo, other_run_id)

      assert :ok = Runs.delete(TestRepo, run_id)
      assert queue_count("q", "delete_run", other_run_id) == 1
      assert queue_count("a", "delete_run", other_run_id) == 1
    end

    test "rejects invalid IDs and invalid persisted flow slugs" do
      assert {:error, :invalid_id} = Runs.delete(TestRepo, "not-a-uuid")

      run_id = Ecto.UUID.generate()

      with_foreign_key_checks_disabled(fn ->
        TestRepo.query!(
          "INSERT INTO pgflow.runs (run_id, flow_slug, input) VALUES ($1, $2, '{}'::jsonb)",
          [Ecto.UUID.dump!(run_id), "invalid-slug"]
        )
      end)

      assert {:error, :invalid_flow_slug} = Runs.make_available(TestRepo, run_id)
      assert {:error, :invalid_flow_slug} = Runs.delete(TestRepo, run_id)
      assert relational_counts(run_id) == {1, 0, 0}

      with_foreign_key_checks_disabled(fn ->
        TestRepo.query!("DELETE FROM pgflow.runs WHERE run_id = $1", [Ecto.UUID.dump!(run_id)])
      end)
    end

    test "treats absent runs and missing queues as successful no-ops" do
      assert :ok = Runs.delete(TestRepo, Ecto.UUID.generate())

      create_flow("delete_missing_queue")
      add_step("delete_missing_queue", "work")
      run_id = start_flow_run("delete_missing_queue", %{})
      TestRepo.query!("SELECT pgmq.drop_queue($1::text)", ["delete_missing_queue"])

      assert :ok = Runs.delete(TestRepo, run_id)
      assert relational_counts(run_id) == {0, 0, 0}
    end

    test "uses canonical mixed-case queue tables and preserves unrelated live and archive data" do
      create_flow("MixedCaseDelete")
      add_step("MixedCaseDelete", "work", type: "map")
      run_id = start_flow_run("MixedCaseDelete", ["target-live", "target-archive"])
      other_run_id = start_flow_run("MixedCaseDelete", ["other-live", "other-archive"])
      [archived_message_id | _] = task_message_ids(run_id)
      [other_archived_message_id | _] = task_message_ids(other_run_id)

      Enum.each([archived_message_id, other_archived_message_id], fn message_id ->
        TestRepo.query!("SELECT pgmq.archive($1::text, $2::bigint)", [
          "MixedCaseDelete",
          message_id
        ])
      end)

      assert queue_count("q", "MixedCaseDelete", run_id) == 1
      assert queue_count("a", "MixedCaseDelete", run_id) == 1
      assert queue_count("q", "MixedCaseDelete", other_run_id) == 1
      assert queue_count("a", "MixedCaseDelete", other_run_id) == 1

      assert :ok = Runs.delete(TestRepo, run_id)

      assert queue_count("q", "MixedCaseDelete", run_id) == 0
      assert queue_count("a", "MixedCaseDelete", run_id) == 0
      assert queue_count("q", "MixedCaseDelete", other_run_id) == 1
      assert queue_count("a", "MixedCaseDelete", other_run_id) == 1
      assert relational_counts(run_id) == {0, 0, 0}
      assert relational_counts(other_run_id) == {1, 2, 1}
    end

    test "rolls back queue and lifecycle deletion when the final run delete fails" do
      create_flow("delete_rollback")
      add_step("delete_rollback", "work", type: "map")
      run_id = start_flow_run("delete_rollback", ["live", "archive"])
      [archived_message_id | _] = task_message_ids(run_id)

      TestRepo.query!("SELECT pgmq.archive($1::text, $2::bigint)", [
        "delete_rollback",
        archived_message_id
      ])

      blocker_table = "run_delete_blocker_#{System.unique_integer([:positive])}"

      TestRepo.query!("""
      CREATE TABLE #{blocker_table} (
        run_id uuid PRIMARY KEY REFERENCES pgflow.runs(run_id) ON DELETE RESTRICT
      )
      """)

      try do
        TestRepo.query!("INSERT INTO #{blocker_table} (run_id) VALUES ($1)", [
          Ecto.UUID.dump!(run_id)
        ])

        assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation}}} =
                 Runs.delete(TestRepo, run_id)

        assert queue_count("q", "delete_rollback", run_id) == 1
        assert queue_count("a", "delete_rollback", run_id) == 1
        assert relational_counts(run_id) == {1, 2, 1}
      after
        TestRepo.query!("DROP TABLE IF EXISTS #{blocker_table}")
      end
    end
  end

  defp queue_visibility(flow_slug, run_id) do
    queue_table = canonical_queue_table("q", flow_slug)

    %{rows: [[visible, total]]} =
      TestRepo.query!(
        """
        SELECT count(*) FILTER (WHERE vt <= clock_timestamp()), count(*)
        FROM #{queue_table}
        WHERE message->>'run_id' = $1
        """,
        [run_id]
      )

    {visible, total}
  end

  defp queue_count(prefix, flow_slug, run_id) do
    queue_table = canonical_queue_table(prefix, flow_slug)

    %{rows: [[count]]} =
      TestRepo.query!(
        "SELECT count(*) FROM #{queue_table} WHERE message->>'run_id' = $1",
        [run_id]
      )

    count
  end

  defp task_message_ids(run_id) do
    %{rows: rows} =
      TestRepo.query!(
        "SELECT message_id FROM pgflow.step_tasks WHERE run_id = $1 ORDER BY task_index",
        [Ecto.UUID.dump!(run_id)]
      )

    Enum.map(rows, fn [message_id] -> message_id end)
  end

  defp relational_counts(run_id) do
    %{rows: [[runs, tasks, states]]} =
      TestRepo.query!(
        """
        SELECT
          (SELECT count(*) FROM pgflow.runs WHERE run_id = $1),
          (SELECT count(*) FROM pgflow.step_tasks WHERE run_id = $1),
          (SELECT count(*) FROM pgflow.step_states WHERE run_id = $1)
        """,
        [Ecto.UUID.dump!(run_id)]
      )

    {runs, tasks, states}
  end

  defp canonical_queue_table(prefix, flow_slug) do
    %{rows: [[quoted_table_name]]} =
      TestRepo.query!(
        "SELECT quote_ident(pgmq.format_table_name($1::text, $2::text))",
        [flow_slug, prefix]
      )

    ~s("pgmq".#{quoted_table_name})
  end

  defp with_foreign_key_checks_disabled(fun) do
    {:ok, result} =
      TestRepo.transaction(fn ->
        TestRepo.query!("SET LOCAL session_replication_role = replica")

        try do
          fun.()
        after
          TestRepo.query!("SET LOCAL session_replication_role = origin")
        end
      end)

    result
  end

  defp collect_count_queries(queries) do
    receive do
      {:count_query, query} -> collect_count_queries([query | queries])
    after
      0 -> Enum.reverse(queries)
    end
  end
end
