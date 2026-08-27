defmodule PgFlow.Migrations.HelpersUpgradeTest do
  @moduledoc """
  Destructive, database-backed coverage for a real V04-to-V05-to-V04 helper upgrade.

  Run only against the isolated PgFlow test database with:

      PGFLOW_REQUIRE_DB=1 mix test --only migration test/pgflow/migrations/helpers_upgrade_test.exs
  """

  use ExUnit.Case, async: false

  @moduletag :migration

  alias Ecto.Adapters.SQL.Sandbox
  alias EctoEvolver.Adapters.Postgres
  alias PgFlow.Queries.{Flows, Workers}
  alias PgFlow.TestRepo

  defmodule CoreUp do
    @moduledoc false
    use Ecto.Migration
    def up, do: PgFlow.Migration.up()
  end

  defmodule UpToV04 do
    @moduledoc false
    use Ecto.Migration
    def up, do: PgFlow.HelpersMigration.up(version: 4)
  end

  defmodule UpToV05 do
    @moduledoc false
    use Ecto.Migration
    def up, do: PgFlow.HelpersMigration.up(version: 5)
  end

  defmodule DownToV04 do
    @moduledoc false
    use Ecto.Migration
    def up, do: PgFlow.HelpersMigration.down(version: 4)
  end

  defmodule RestoreCurrent do
    @moduledoc false
    use Ecto.Migration

    def up do
      PgFlow.Migration.up()
      PgFlow.HelpersMigration.up()
    end
  end

  @core_version 6_000_000_000_001
  @v04_version 6_000_000_000_002
  @v05_version 6_000_000_000_003
  @down_version 6_000_000_000_004
  @restore_version 6_000_000_000_005
  @migration_versions [
    @core_version,
    @v04_version,
    @v05_version,
    @down_version,
    @restore_version
  ]

  setup_all do
    on_exit(fn ->
      Sandbox.mode(TestRepo, :auto)
      reset_schema!()
      Ecto.Migrator.up(TestRepo, @restore_version, RestoreCurrent, log: false)
      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  setup do
    Sandbox.mode(TestRepo, :auto)
    reset_schema!()

    Ecto.Migrator.up(TestRepo, @core_version, CoreUp, log: false)
    Ecto.Migrator.up(TestRepo, @v04_version, UpToV04, log: false)

    on_exit(fn -> Sandbox.mode(TestRepo, :manual) end)

    :ok
  end

  test "V05 creates task_signals and the exact await-signals functions" do
    migrate_to_v05!()

    assert required_v05_objects() == [true, true, true, true]
    assert installed_helpers_version() == 5
  end

  test "a data-bearing V04 upgrade preserves rows and enables V05 awaiting" do
    {flow_slug, run_id, task} = create_started_task!("helpers_data_bearing_upgrade")
    before_upgrade = lifecycle_snapshot(run_id)

    migrate_to_v05!()

    assert installed_helpers_version() == 5
    assert lifecycle_snapshot(run_id) == before_upgrade

    assert %{rows: [["parked", nil]]} =
             TestRepo.query!(
               "SELECT outcome, payload FROM pgflow.await_task_signal($1, 'approval', 0, $2, $3, 60, true)",
               [run_id, task.attempts_count, task.message_id]
             )

    assert %{rows: [["requeued"]]} =
             TestRepo.query!(
               ~s|SELECT outcome FROM pgflow.signal_task($1, 'approval', 0, '{"decision":"approved"}'::jsonb)|,
               [run_id]
             )

    assert %{rows: [[^flow_slug, "queued", %{"decision" => "approved"}]]} =
             TestRepo.query!(
               """
               SELECT st.flow_slug, st.status, ts.payload
               FROM pgflow.step_tasks st
               JOIN pgflow.task_signals ts
                 ON ts.run_id = st.run_id
                AND ts.step_slug = st.step_slug
                AND ts.task_index = st.task_index
               WHERE st.run_id = $1 AND st.step_slug = 'approval' AND st.task_index = 0
               """,
               [run_id]
             )
  end

  test "an empty V05 installation rolls back to V04" do
    migrate_to_v05!()

    assert :ok = Ecto.Migrator.up(TestRepo, @down_version, DownToV04, log: false)
    assert required_v05_objects() == [false, false, false, false]
    assert installed_helpers_version() == 4

    %{rows: [[constraint]]} =
      TestRepo.query!("""
      SELECT pg_get_constraintdef(oid)
      FROM pg_constraint
      WHERE connamespace = 'pgflow'::regnamespace
        AND conrelid = 'pgflow.step_tasks'::regclass
        AND conname = 'valid_status'
      """)

    refute constraint =~ "waiting"
  end

  test "a buffered signal refuses rollback before any V05 object is removed" do
    migrate_to_v05!()
    create_buffered_signal!()

    error =
      assert_raise Postgrex.Error, fn ->
        Ecto.Migrator.up(TestRepo, @down_version, DownToV04, log: false)
      end

    assert Exception.message(error) =~
             "cannot roll pgflow helpers V05 back to V04 while task signals or waiting tasks exist"

    assert required_v05_objects() == [true, true, true, true]
    assert installed_helpers_version() == 5
  end

  test "rollback waits for an in-flight signal writer and then refuses before teardown" do
    migrate_to_v05!()
    {_flow_slug, run_id, _task} = create_started_task!("helpers_signal_rollback_race")
    parent = self()

    writer =
      Task.async(fn -> signal_or_park_in_open_transaction(parent, :signal, run_id, nil) end)

    assert_receive {:await_writer_finished, writer_pid, "buffered"}, 5_000

    rollback = Task.async(fn -> rollback_sql(parent) end)
    assert_receive {:rollback_started, _rollback_pid, rollback_backend_pid}, 5_000
    assert :ok = wait_until(fn -> backend_blocked?(rollback_backend_pid) end)

    send(writer_pid, :commit)
    assert {:ok, "buffered"} = Task.await(writer, 5_000)
    assert {:error, message} = Task.await(rollback, 5_000)
    assert message =~ "cannot roll pgflow helpers V05 back to V04"
    assert required_v05_objects() == [true, true, true, true]
    assert installed_helpers_version() == 5
  end

  test "rollback waits for an in-flight atomic park and then refuses before teardown" do
    migrate_to_v05!()
    {_flow_slug, run_id, task} = create_started_task!("helpers_park_rollback_race")
    parent = self()

    writer = Task.async(fn -> signal_or_park_in_open_transaction(parent, :park, run_id, task) end)
    assert_receive {:await_writer_finished, writer_pid, "parked"}, 5_000

    rollback = Task.async(fn -> rollback_sql(parent) end)
    assert_receive {:rollback_started, _rollback_pid, rollback_backend_pid}, 5_000
    assert :ok = wait_until(fn -> backend_blocked?(rollback_backend_pid) end)

    send(writer_pid, :commit)
    assert {:ok, "parked"} = Task.await(writer, 5_000)
    assert {:error, message} = Task.await(rollback, 5_000)
    assert message =~ "cannot roll pgflow helpers V05 back to V04"
    assert required_v05_objects() == [true, true, true, true]
    assert installed_helpers_version() == 5
  end

  test "rollback waits for an in-flight expiry sweep without deadlock and then refuses safely" do
    migrate_to_v05!()

    {_flow_slug, run_id, task} =
      create_started_task!("helpers_expiry_rollback_race")

    park_expired_task!(run_id, task)
    parent = self()
    expiry = Task.async(fn -> expire_in_open_transaction(parent) end)
    assert_receive {:expiry_finished, expiry_pid, 1}, 5_000

    rollback = Task.async(fn -> rollback_sql(parent) end)
    assert_receive {:rollback_started, _rollback_pid, rollback_backend_pid}, 5_000
    assert :ok = wait_until(fn -> backend_blocked?(rollback_backend_pid) end)

    send(expiry_pid, :commit)
    assert {:ok, 1} = Task.await(expiry, 5_000)
    assert {:error, message} = Task.await(rollback, 5_000)
    assert message =~ "cannot roll pgflow helpers V05 back to V04"

    assert %{rows: [["queued", true]]} =
             TestRepo.query!(
               """
               SELECT st.status, ts.timed_out
               FROM pgflow.step_tasks st
               JOIN pgflow.task_signals ts
                 ON ts.run_id = st.run_id
                AND ts.step_slug = st.step_slug
                AND ts.task_index = st.task_index
               WHERE st.run_id = $1 AND st.step_slug = 'approval' AND st.task_index = 0
               """,
               [run_id]
             )

    assert required_v05_objects() == [true, true, true, true]
    assert installed_helpers_version() == 5
  end

  defp migrate_to_v05! do
    Ecto.Migrator.up(TestRepo, @v05_version, UpToV05, log: false)
  end

  defp installed_helpers_version do
    Postgres.get_version(
      TestRepo,
      "pgflow",
      {:view, "extensions_version"}
    )
  end

  defp required_v05_objects do
    %{rows: [objects]} =
      TestRepo.query!("""
      SELECT
        to_regclass('pgflow.task_signals') IS NOT NULL,
        to_regprocedure(
          'pgflow.await_task_signal(uuid,text,integer,integer,bigint,bigint,boolean)'
        ) IS NOT NULL,
        to_regprocedure('pgflow.signal_task(uuid,text,integer,jsonb)') IS NOT NULL,
        to_regprocedure('pgflow.expire_waiting_tasks(integer)') IS NOT NULL
      """)

    objects
  end

  defp lifecycle_snapshot(run_id) do
    %{rows: [snapshot]} =
      TestRepo.query!(
        """
        SELECT
          f.flow_slug,
          r.status,
          r.input,
          r.remaining_steps,
          ss.status,
          ss.remaining_tasks,
          ss.remaining_deps,
          st.status,
          st.attempts_count,
          st.message_id
        FROM pgflow.flows f
        JOIN pgflow.runs r ON r.flow_slug = f.flow_slug
        JOIN pgflow.step_states ss ON ss.run_id = r.run_id
        JOIN pgflow.step_tasks st
          ON st.run_id = ss.run_id
         AND st.step_slug = ss.step_slug
        WHERE r.run_id = $1
        """,
        [run_id]
      )

    snapshot
  end

  defp create_buffered_signal! do
    TestRepo.query!("SELECT pgflow.create_flow('helpers_rollback_signal')")

    TestRepo.query!("""
    SELECT pgflow.add_step(
      'helpers_rollback_signal',
      'approval',
      ARRAY[]::text[],
      null,
      null,
      null,
      null,
      'single'
    )
    """)

    %{rows: [[run]]} =
      TestRepo.query!("SELECT pgflow.start_flow('helpers_rollback_signal', '{}'::jsonb)")

    {run_id, _flow_slug, _status, _input, _output, _remaining_steps, _started_at, _completed_at,
     _failed_at} = run

    %{rows: [["buffered"]]} =
      TestRepo.query!(
        """
        SELECT outcome
        FROM pgflow.signal_task($1, 'approval', 0, '{"decision":"approved"}'::jsonb)
        """,
        [run_id]
      )
  end

  defp create_started_task!(flow_slug) do
    TestRepo.query!("SELECT pgflow.create_flow($1)", [flow_slug])

    TestRepo.query!(
      """
      SELECT pgflow.add_step($1, 'approval', ARRAY[]::text[], null, null, null, null, 'single')
      """,
      [flow_slug]
    )

    %{rows: [[run]]} = TestRepo.query!("SELECT pgflow.start_flow($1, '{}'::jsonb)", [flow_slug])
    {run_id, _, _, _, _, _, _, _, _} = run
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
    {:ok, messages} = Flows.read(TestRepo, flow_slug, 30, 1)
    message_ids = Enum.map(messages, fn [message_id | _] -> message_id end)
    {:ok, _} = Flows.start_tasks(TestRepo, flow_slug, message_ids, worker_id)

    %{rows: [[attempt, message_id]]} =
      TestRepo.query!(
        """
        SELECT attempts_count, message_id
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = 'approval' AND task_index = 0
        """,
        [run_id]
      )

    {flow_slug, run_id, %{attempts_count: attempt, message_id: message_id}}
  end

  defp signal_or_park_in_open_transaction(parent, operation, run_id, task) do
    connection = independent_connection()

    result =
      Postgrex.transaction(connection, fn connection ->
        sql =
          case operation do
            :signal ->
              "SELECT outcome FROM pgflow.signal_task($1, 'approval', 0, '{\"decision\":\"approved\"}'::jsonb)"

            :park ->
              "SELECT outcome FROM pgflow.await_task_signal($1, 'approval', 0, $2, $3, NULL, true)"
          end

        params =
          case operation do
            :signal -> [run_id]
            :park -> [run_id, task.attempts_count, task.message_id]
          end

        %{rows: [[outcome | _]]} = Postgrex.query!(connection, sql, params)
        send(parent, {:await_writer_finished, self(), outcome})

        receive do
          :commit -> outcome
        end
      end)

    GenServer.stop(connection)
    result
  end

  defp park_expired_task!(run_id, task) do
    %{rows: [["parked", nil]]} =
      TestRepo.query!(
        """
        SELECT outcome, payload
        FROM pgflow.await_task_signal($1, 'approval', 0, $2, $3, 60, true)
        """,
        [run_id, task.attempts_count, task.message_id]
      )

    TestRepo.query!(
      """
      UPDATE pgflow.task_signals
      SET wait_deadline_at = now() - interval '1 second'
      WHERE run_id = $1 AND step_slug = 'approval' AND task_index = 0
      """,
      [run_id]
    )
  end

  defp expire_in_open_transaction(parent) do
    connection = independent_connection()

    result =
      Postgrex.transaction(connection, fn connection ->
        %{rows: [[count]]} =
          Postgrex.query!(connection, "SELECT pgflow.expire_waiting_tasks(100)", [])

        send(parent, {:expiry_finished, self(), count})

        receive do
          :commit -> count
        end
      end)

    GenServer.stop(connection)
    result
  end

  defp rollback_sql(parent) do
    connection = independent_connection()

    try do
      Postgrex.transaction(connection, fn connection ->
        %{rows: [[backend_pid]]} = Postgrex.query!(connection, "SELECT pg_backend_pid()", [])
        send(parent, {:rollback_started, self(), backend_pid})

        Enum.each(v05_down_statements(), fn statement ->
          Postgrex.query!(connection, statement, [])
        end)
      end)

      {:ok, :rolled_back}
    rescue
      error in Postgrex.Error -> {:error, Exception.message(error)}
    after
      GenServer.stop(connection)
    end
  end

  defp v05_down_statements do
    "priv/pgflow_helpers/sql/versions/v05/v05_down.sql"
    |> File.read!()
    |> String.replace("$SCHEMA$", "pgflow")
    |> String.split("--SPLIT--")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp independent_connection do
    opts =
      TestRepo.config()
      |> Keyword.take([:hostname, :port, :username, :password, :database, :socket_dir, :ssl])

    {:ok, connection} = Postgrex.start_link(opts)
    connection
  end

  defp backend_blocked?(backend_pid) do
    %{rows: [[blocked?]]} =
      TestRepo.query!("SELECT cardinality(pg_blocking_pids($1)) > 0", [backend_pid])

    blocked?
  end

  defp wait_until(condition_fn) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    do_wait_until(condition_fn, deadline)
  end

  defp do_wait_until(condition_fn, deadline) do
    cond do
      condition_fn.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(25)
        do_wait_until(condition_fn, deadline)
    end
  end

  defp reset_schema! do
    TestRepo.query!("DROP SCHEMA IF EXISTS pgflow CASCADE")

    TestRepo.query!(
      "DELETE FROM schema_migrations WHERE version = ANY($1::bigint[])",
      [@migration_versions]
    )
  end
end
