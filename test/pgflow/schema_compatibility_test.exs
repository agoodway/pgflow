defmodule PgFlow.SchemaCompatibilityTest do
  use ExUnit.Case, async: false

  alias PgFlow.SchemaCompatibility

  @version_query_result_key {__MODULE__, :version_query_result}
  @objects_query_result_key {__MODULE__, :objects_query_result}

  defmodule FakeRepo do
    def __adapter__, do: Ecto.Adapters.Postgres

    def query(sql, ["pgflow", "extensions_version"]) do
      send(self(), {:version_query, sql})

      next_result(
        {PgFlow.SchemaCompatibilityTest, :version_query_result},
        {:ok, %{rows: [["PgFlow HelpersMigration version=5"]]}}
      )
    end

    def query(sql) do
      send(self(), {:objects_query, sql})

      next_result(
        {PgFlow.SchemaCompatibilityTest, :objects_query_result},
        {:ok, %{rows: [[true, true, true, true]]}}
      )
    end

    defp next_result(key, default) do
      case Process.get(key, default) do
        [result | remaining] ->
          Process.put(key, remaining)
          result

        result ->
          result
      end
    end
  end

  setup do
    Process.delete(@version_query_result_key)
    Process.delete(@objects_query_result_key)
    :ok
  end

  test "accepts V05 when every required await-signals object exists" do
    assert :ok = SchemaCompatibility.check_await_signals(FakeRepo)

    assert_received {:objects_query, sql}
    assert sql =~ "to_regclass('pgflow.task_signals')"

    assert sql =~
             "pgflow.await_task_signal(uuid,text,integer,integer,bigint,bigint,boolean)"

    assert sql =~ "pgflow.signal_task(uuid,text,integer,jsonb)"
    assert sql =~ "pgflow.expire_waiting_tasks(integer)"
  end

  test "reports V04 with version-aware upgrade guidance" do
    Process.put(
      @version_query_result_key,
      {:ok, %{rows: [["PgFlow HelpersMigration version=4"]]}}
    )

    assert {:error, message} = SchemaCompatibility.check_await_signals(FakeRepo)
    assert message =~ "PgFlow helpers V05 are required"
    assert message =~ "database is at V04"
    assert message =~ "generated helpers upgrade migration"
    refute_received {:objects_query, _sql}
  end

  test "fails closed with a clear error when the helper version is missing or unreadable" do
    Process.put(@version_query_result_key, {:ok, %{rows: [[nil]]}})

    assert {:error, message} = SchemaCompatibility.check_await_signals(FakeRepo)
    assert message =~ "helper version is missing or unreadable"
    assert message =~ "V05"
    refute_received {:objects_query, _sql}
  end

  test "classifies a version lookup query failure as transient repo unavailability" do
    Process.put(@version_query_result_key, {:error, :catalog_unavailable})

    assert {:error, {:repo_unavailable, :catalog_unavailable}} =
             SchemaCompatibility.check_await_signals(FakeRepo)

    refute_received {:objects_query, _sql}
  end

  test "rejects a V05 marker when any required object is absent" do
    Process.put(@objects_query_result_key, {:ok, %{rows: [[true, false, true, true]]}})

    assert {:error, "PgFlow helpers report V05 but await-signals objects are missing"} =
             SchemaCompatibility.check_await_signals(FakeRepo)
  end

  test "classifies a required-object query failure as transient repo unavailability" do
    Process.put(@objects_query_result_key, {:error, :connection_closed})

    assert {:error, {:repo_unavailable, :connection_closed}} =
             SchemaCompatibility.check_await_signals(FakeRepo)
  end

  test "startup wait retries only repo unavailability with bounded exponential delays" do
    Process.put(@version_query_result_key, [
      {:error, :starting},
      {:error, :still_starting},
      {:ok, %{rows: [["PgFlow HelpersMigration version=5"]]}}
    ])

    assert :ok =
             SchemaCompatibility.await_await_signals!(FakeRepo,
               initial_delay: 5,
               max_delay: 8,
               max_attempts: 3,
               sleep: fn delay -> send(self(), {:slept, delay}) end
             )

    assert_received {:slept, 5}
    assert_received {:slept, 8}
  end

  test "startup wait fails immediately for a permanently incompatible helper version" do
    Process.put(
      @version_query_result_key,
      {:ok, %{rows: [["PgFlow HelpersMigration version=4"]]}}
    )

    assert_raise RuntimeError, ~r/database is at V04/, fn ->
      SchemaCompatibility.await_await_signals!(FakeRepo,
        sleep: fn delay -> flunk("unexpected compatibility retry after #{delay}ms") end
      )
    end
  end

  test "supervisor startup exhausts transient checks before any runtime child starts" do
    Process.put(@version_query_result_key, [
      {:error, :starting},
      {:error, :still_starting}
    ])

    refute Process.whereis(PgFlow.TaskSupervisor)

    assert_raise RuntimeError, ~r/repository is unavailable after 2 attempts/, fn ->
      PgFlow.Supervisor.start_link([repo: FakeRepo],
        initial_delay: 0,
        max_delay: 0,
        max_attempts: 2,
        sleep: fn delay -> send(self(), {:supervisor_slept, delay}) end
      )
    end

    assert_received {:supervisor_slept, 0}
    refute Process.whereis(PgFlow.TaskSupervisor)
  end

  test "the raising check preserves the compatibility error" do
    Process.put(@version_query_result_key, {:ok, %{rows: []}})

    assert_raise RuntimeError, ~r/helper version is missing or unreadable/, fn ->
      SchemaCompatibility.check_await_signals!(FakeRepo)
    end
  end

  test "the raising check preserves repository unavailability as an infrastructure error" do
    Process.put(@version_query_result_key, {:error, :connection_closed})

    assert_raise RuntimeError, ~r/could not reach the repository.*connection_closed/, fn ->
      SchemaCompatibility.check_await_signals!(FakeRepo)
    end
  end

  test "formats typed repository errors for operator-facing boundaries" do
    assert SchemaCompatibility.error_message({:repo_unavailable, :connection_closed}) ==
             "PgFlow schema compatibility check could not reach the repository: :connection_closed"
  end

  describe "source contracts" do
    test "startup retry defaults are documented for operators" do
      source = File.read!("lib/pgflow/schema_compatibility.ex")

      assert source =~ "eight attempts"
      assert source =~ "100 milliseconds"
      assert source =~ "5 seconds"
    end

    test "rollback preflight is the first executable SQL and precedes every drop" do
      sql = File.read!("priv/pgflow_helpers/sql/versions/v05/v05_down.sql")

      executable_sql =
        sql
        |> String.replace(~r/^\s*--.*$/m, "")
        |> String.trim_leading()

      assert String.starts_with?(executable_sql, "DO $$")

      assert {run_lock_offset, _} =
               :binary.match(executable_sql, "LOCK TABLE pgflow.runs IN EXCLUSIVE MODE")

      assert {step_lock_offset, _} =
               :binary.match(executable_sql, "LOCK TABLE pgflow.step_states IN EXCLUSIVE MODE")

      assert {task_lock_offset, _} =
               :binary.match(executable_sql, "LOCK TABLE pgflow.step_tasks IN EXCLUSIVE MODE")

      assert {signal_lock_offset, _} =
               :binary.match(executable_sql, "LOCK TABLE pgflow.task_signals IN EXCLUSIVE MODE")

      assert run_lock_offset < step_lock_offset
      assert step_lock_offset < task_lock_offset
      assert task_lock_offset < signal_lock_offset

      assert executable_sql =~
               "IF EXISTS (SELECT 1 FROM pgflow.task_signals)\n" <>
                 "     OR EXISTS (SELECT 1 FROM pgflow.step_tasks WHERE status = 'waiting')"

      assert executable_sql =~
               "cannot roll pgflow helpers V05 back to V04 while task signals or waiting tasks exist; resolve or cancel them first"

      assert {preflight_offset, _} = :binary.match(executable_sql, "DO $$")
      assert {first_drop_offset, _} = :binary.match(executable_sql, "DROP ")
      assert preflight_offset < first_drop_offset
      assert signal_lock_offset < first_drop_offset
    end

    test "rollback drops every current V05 signature and object before restoring valid_status" do
      sql = File.read!("priv/pgflow_helpers/sql/versions/v05/v05_down.sql")

      assert sql =~ "DROP TRIGGER IF EXISTS cleanup_terminal_step_signals"
      assert sql =~ "DROP FUNCTION IF EXISTS $SCHEMA$.cleanup_terminal_step_signals()"
      assert sql =~ "DROP TRIGGER IF EXISTS cleanup_terminal_run_signals"
      assert sql =~ "DROP FUNCTION IF EXISTS $SCHEMA$.cleanup_terminal_run_signals()"

      assert sql =~
               "DROP FUNCTION IF EXISTS $SCHEMA$.park_waiting_task(uuid, text, integer, timestamptz)"

      assert sql =~
               "DROP FUNCTION IF EXISTS $SCHEMA$.await_task_signal(uuid, text, integer, integer, bigint, bigint, boolean)"

      assert sql =~ "DROP FUNCTION IF EXISTS $SCHEMA$.signal_task(uuid, text, integer, jsonb)"
      assert sql =~ "DROP FUNCTION IF EXISTS $SCHEMA$.consume_task_signal(uuid, text, integer)"
      assert sql =~ "DROP FUNCTION IF EXISTS $SCHEMA$.expire_waiting_tasks(integer)"
      assert sql =~ "DROP INDEX IF EXISTS $SCHEMA$.task_signals_unresolved_deadline_idx"
      assert sql =~ "DROP TABLE IF EXISTS $SCHEMA$.task_signals"

      assert {table_offset, _} = :binary.match(sql, "DROP TABLE IF EXISTS")
      assert {constraint_offset, _} = :binary.match(sql, "ADD CONSTRAINT valid_status")
      assert table_offset < constraint_offset
    end

    test "V05 widens valid_status without validating historical rows in the same transaction" do
      sql = File.read!("priv/pgflow_helpers/sql/versions/v05/v05_up.sql")

      executable_sql = String.replace(sql, ~r/^\s*--.*$/m, "")

      assert sql =~ ~r/ADD CONSTRAINT valid_status CHECK \(.+\)\s+NOT VALID/s

      assert sql =~
               "ALTER TABLE pgflow.step_tasks VALIDATE CONSTRAINT valid_status;"

      assert sql =~ "later separately committed migration, not this V05 transaction"
      refute executable_sql =~ "VALIDATE CONSTRAINT valid_status"
    end

    test "supervisor waits for compatibility before starting its process" do
      source = File.read!("lib/pgflow/supervisor.ex")

      assert source =~ "SchemaCompatibility.await_await_signals!(repo, compatibility_opts)"

      assert {check_offset, _} =
               :binary.match(
                 source,
                 "SchemaCompatibility.await_await_signals!(repo, compatibility_opts)"
               )

      assert {start_offset, _} =
               :binary.match(
                 source,
                 "Supervisor.start_link(__MODULE__, config, name: __MODULE__)"
               )

      assert check_offset < start_offset
    end

    test "schema Mix task includes V05 objects and uses the shared compatibility check" do
      source = File.read!("lib/mix/tasks/pgflow.check_schema.ex")

      assert source =~ "task_signals"
      assert source =~ "await_task_signal"
      assert source =~ "signal_task"
      assert source =~ "expire_waiting_tasks"
      refute source =~ "park_waiting_task"
      refute source =~ "consume_task_signal"
      assert source =~ "SchemaCompatibility.check_await_signals(repo)"
      assert source =~ "{:error, SchemaCompatibility.error_message(error)}"
    end
  end
end
