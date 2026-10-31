defmodule PgFlow.Queries.PgmqNotifyTriggerTest do
  @moduledoc """
  Verifies that `PgFlow.Queries.Pgmq.enable_notify_insert/4` leaves pgmq's
  INSERT notify trigger `DEFERRABLE INITIALLY DEFERRED`, so a producer's open
  transaction never holds the queue's `pgmq.notify_insert_throttle` row lock
  against other producers; that repeated calls take the no-DDL fast path, which
  also restores a missing throttle row; that the install's lock wait is
  bounded; that pending deferred trigger events are flushed before pgflow
  drops a queue in the same transaction; that `PgFlow.Signal.Notify` restores
  missing throttle rows; and that `PgFlow.SchemaCheck.warnings/2` reports
  immediate triggers, missing throttle rows, and partitioned queues that cannot
  notify.

  The cross-session cases need committed data on separate connections, so the
  module runs with the sandbox in `:auto` mode against unique scratch queues
  that it drops afterwards. When the database's pgmq predates
  `enable_notify_insert` (the test image ships pgmq 1.5.1), the module loads
  pgmq 1.11.0's notify objects from `test/support/db/pgmq_1_11_notify.sql` and
  drops them once the module finishes.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import ExUnit.CaptureLog

  alias Ecto.Adapters.SQL.Sandbox
  alias Mix.Tasks.Pgflow.CheckSchema
  alias PgFlow.Queries.Flows
  alias PgFlow.Queries.Pgmq
  alias PgFlow.SchemaCheck
  alias PgFlow.Signal.Notify
  alias PgFlow.Test.DatabaseHelpers
  alias PgFlow.TestRepo

  @moduletag timeout: 30_000
  @moduletag :integration

  @notify_sql_path "test/support/db/pgmq_1_11_notify.sql"
  @send_sql "SELECT pgmq.send($1::text, '{}'::jsonb)"
  @reject_deferred_trigger "pgflow_test_reject_deferred_notify"

  # notify_queue_listeners() from pgmq main, which reads the queue name from
  # the trigger argument instead of the lowercased table name.
  @tg_argv_listener_sql """
  CREATE OR REPLACE FUNCTION pgmq.notify_queue_listeners()
  RETURNS TRIGGER AS $$
  DECLARE
    v_queue_name  TEXT;
    updated_count INTEGER;
  BEGIN
    v_queue_name := COALESCE(TG_ARGV[0], substring(TG_TABLE_NAME from 3));

    UPDATE pgmq.notify_insert_throttle
    SET last_notified_at = clock_timestamp()
    WHERE queue_name = v_queue_name
      AND (
        throttle_interval_ms = 0
            OR clock_timestamp() - last_notified_at >=
               (throttle_interval_ms OPERATOR(pg_catalog.*) INTERVAL '1 millisecond')
      );

    GET DIAGNOSTICS updated_count = ROW_COUNT;

    IF updated_count > 0 THEN
      PERFORM PG_NOTIFY('pgmq.q_' || v_queue_name || '.' || TG_OP, NULL);
    END IF;

  RETURN NEW;
  END;
  $$ LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
  """

  setup_all do
    installed? = with_conn(&(not notify_api_present?(&1)))

    if installed? do
      on_exit(fn -> with_conn(&drop_notify_objects!/1) end)
      load_notify_objects!()
    end

    :ok
  end

  setup do
    Sandbox.mode(TestRepo, :auto)
    on_exit(fn -> Sandbox.mode(TestRepo, :manual) end)

    queue = "notify_defer_#{System.unique_integer([:positive])}"
    TestRepo.query!("SELECT pgmq.create($1::text)", [queue])
    on_exit(fn -> drop_queue!(queue) end)

    {:ok, queue: queue}
  end

  describe "enable_notify_insert/4" do
    test "leaves the insert trigger deferrable and initially deferred", %{queue: queue} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      assert [%{deferrable: true, initially_deferred: true, args: [^queue]}] = triggers(queue)

      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 250)
      assert [%{deferrable: true, initially_deferred: true, args: [^queue]}] = triggers(queue)
      assert throttle_interval(queue) == 250
    end

    test "a repeated call takes the fast path and reconciles a changed throttle", %{queue: queue} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      [%{oid: oid}] = triggers(queue)

      # Any DDL on the queue table would wait on this lock and time out.
      locker = hold_table_lock(queue)

      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0, lock_timeout: 100)
      assert [%{oid: ^oid}] = triggers(queue)

      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 500, lock_timeout: 100)
      assert [%{oid: ^oid, initially_deferred: true}] = triggers(queue)
      assert throttle_interval(queue) == 500

      release(locker)
    end

    test "the fast path restores a missing throttle row without DDL", %{queue: queue} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      [%{oid: oid}] = triggers(queue)
      delete_throttle_row!(queue)

      locker = hold_table_lock(queue)

      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 250, lock_timeout: 100)
      assert [%{oid: ^oid, initially_deferred: true}] = triggers(queue)
      assert throttle_interval(queue) == 250

      release(locker)

      channel = "pgmq.q_#{queue}.INSERT"
      listen!(channel)
      assert {:ok, :sent} = send_with_lock_timeout(queue)
      assert_receive {:notification, _, _, ^channel, _}, 2_000
    end

    test "returns lock_not_available when the table lock is not granted in time", %{queue: queue} do
      locker = hold_table_lock(queue)

      {elapsed_us, result} =
        :timer.tc(fn -> Pgmq.enable_notify_insert(TestRepo, queue, 0, lock_timeout: 200) end)

      assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} = result
      assert elapsed_us < 2_000_000

      release(locker)

      assert [] = triggers(queue)
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      assert [%{initially_deferred: true}] = triggers(queue)
    end

    test "refuses to run inside a transaction", %{queue: queue} do
      assert {:ok, {:error, :in_transaction}} =
               TestRepo.transaction(fn -> Pgmq.enable_notify_insert(TestRepo, queue, 0) end)

      assert {:ok, {:error, :in_transaction}} =
               TestRepo.transaction(fn ->
                 Pgmq.enable_notify_insert(TestRepo, queue, 0, trigger: :pgmq_default)
               end)

      assert [] = triggers(queue)
    end

    test ":pgmq_default leaves pgmq's immediate trigger", %{queue: queue} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0, trigger: :pgmq_default)
      assert [%{deferrable: true, initially_deferred: false, args: []}] = triggers(queue)

      # An immediate trigger locks the throttle row at insert time.
      holder = hold_open_send(queue)

      assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} =
               lock_throttle_row(queue)

      finish(holder, :rollback)
    end

    test "an open producer holds no throttle row lock and a rollback sends nothing", %{
      queue: queue
    } do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      channel = "pgmq.q_#{queue}.INSERT"
      listen!(channel)

      holder = hold_open_send(queue)
      assert {:ok, :locked} = lock_throttle_row(queue)

      finish(holder, :rollback)
      refute_receive {:notification, _, _, ^channel, _}, 300
    end

    test "keeps pgmq's immediate trigger when the deferred swap fails" do
      queue = "notify_defer_swap_#{System.unique_integer([:positive])}"
      TestRepo.query!("SELECT pgmq.create($1::text)", [queue])
      on_exit(fn -> drop_queue!(queue) end)

      reject_deferred_notify_triggers!()

      log =
        capture_log(fn ->
          assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
        end)

      assert log =~ "kept pgmq's immediate notify trigger"
      assert [%{deferrable: true, initially_deferred: false}] = triggers(queue)
      assert %{rows: [[0]]} = throttle_row(queue)

      channel = "pgmq.q_#{queue}.INSERT"
      listen!(channel)
      assert {:ok, :sent} = send_with_lock_timeout(queue)
      assert_receive {:notification, _, _, ^channel, _}, 2_000
    end

    test "an open producer transaction does not block another producer", %{queue: queue} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)

      holder = hold_open_send(queue)

      assert {:ok, :sent} = send_with_lock_timeout(queue)

      finish(holder, :commit)

      assert %{rows: [[2]]} = TestRepo.query!("SELECT count(*) FROM pgmq.q_#{queue}")
    end

    test "each producer notifies when it commits", %{queue: queue} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      channel = "pgmq.q_#{queue}.INSERT"
      listen!(channel)

      holder = hold_open_send(queue)
      refute_receive {:notification, _, _, ^channel, _}, 200

      assert {:ok, :sent} = send_with_lock_timeout(queue)
      assert_receive {:notification, _, _, ^channel, _}, 2_000

      finish(holder, :commit)
      assert_receive {:notification, _, _, ^channel, _}, 2_000
    end

    test "passes the queue name to listeners that read TG_ARGV" do
      queue = "Notify_Defer_Mixed_#{System.unique_integer([:positive])}"
      TestRepo.query!("SELECT pgmq.create($1::text)", [queue])
      on_exit(fn -> drop_queue!(queue) end)

      use_tg_argv_listener!()

      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      assert [%{initially_deferred: true, args: [^queue]}] = triggers(queue)

      channel = "pgmq.q_#{queue}.INSERT"
      listen!(channel)

      assert {:ok, :sent} = send_with_lock_timeout(queue)
      assert_receive {:notification, _, _, ^channel, _}, 2_000
    end

    test "returns the database error for a missing queue" do
      assert {:error, %Postgrex.Error{}} =
               Pgmq.enable_notify_insert(TestRepo, "notify_defer_missing_queue", 0)
    end
  end

  describe "restore_notify_throttle_rows/3" do
    test "restores missing rows only for queues with a notify trigger", %{queue: queue} do
      untriggered = "notify_defer_untriggered_#{System.unique_integer([:positive])}"
      TestRepo.query!("SELECT pgmq.create($1::text)", [untriggered])
      on_exit(fn -> drop_queue!(untriggered) end)

      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      [%{oid: oid}] = triggers(queue)
      delete_throttle_row!(queue)

      locker = hold_table_lock(queue)
      queues = [queue, untriggered, "notify_defer_missing_queue"]

      assert {:ok, [^queue]} = Pgmq.restore_notify_throttle_rows(TestRepo, queues, 100)
      assert {:ok, []} = Pgmq.restore_notify_throttle_rows(TestRepo, queues, 100)
      assert {:ok, []} = Pgmq.restore_notify_throttle_rows(TestRepo, [], 100)

      release(locker)

      assert throttle_interval(queue) == 100
      assert [%{oid: ^oid}] = triggers(queue)
      assert %{rows: []} = throttle_row(untriggered)
    end

    test "Signal.Notify restores rows for the queues of registered workers", %{queue: queue} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      delete_throttle_row!(queue)

      state = %{
        repo: TestRepo,
        notify_throttle_ms: 0,
        throttle_reconcile_interval_ms: 60_000,
        conn: nil,
        workers: %{queue => %{worker_pid: self(), monitor_ref: make_ref(), listen_ref: nil}},
        channels: %{}
      }

      log =
        capture_log(fn ->
          assert {:noreply, ^state} = Notify.handle_info(:reconcile_throttle, state)
        end)

      assert log =~ "restored missing pgmq.notify_insert_throttle rows for #{queue}"
      assert throttle_interval(queue) == 0

      quiet =
        capture_log(fn ->
          assert {:noreply, ^state} = Notify.handle_info(:reconcile_throttle, state)
        end)

      refute quiet =~ "restored"
    end
  end

  describe "pending deferred trigger events" do
    test "a send followed by a queue drop or purge in one transaction fails without a flush", %{
      queue: queue
    } do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)

      ["SELECT pgmq.drop_queue($1::text)", "SELECT pgmq.purge_queue($1::text)"]
      |> Enum.each(fn statement ->
        assert {:error, %Postgrex.Error{postgres: %{code: :object_in_use, message: message}}} =
                 send_then(queue, fn -> TestRepo.query(statement, [queue]) end)

        assert message =~ "pending trigger events"
      end)
    end

    test "flush_notify_events/1 lets the same transaction drop the queue", %{queue: queue} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      channel = "pgmq.q_#{queue}.INSERT"
      listen!(channel)

      assert {:ok, _result} =
               send_then(queue, fn ->
                 with :ok <- Pgmq.flush_notify_events(TestRepo) do
                   TestRepo.query("SELECT pgmq.drop_queue($1::text)", [queue])
                 end
               end)

      assert_receive {:notification, _, _, ^channel, _}, 2_000
      assert %{rows: [[false]]} = queue_exists(queue)
    end

    test "Flows.delete_flow/2 drops a queue sent to earlier in the caller's transaction" do
      slug = "notify_defer_delete_#{System.unique_integer([:positive])}"
      TestRepo.query!("SELECT pgflow.create_flow($1::text)", [slug])
      TestRepo.query!("SELECT pgflow.add_step($1::text, 'work'::text)", [slug])

      on_exit(fn ->
        TestRepo.query!("DELETE FROM pgflow.steps WHERE flow_slug = $1", [slug])
        TestRepo.query!("DELETE FROM pgflow.flows WHERE flow_slug = $1", [slug])
        drop_queue!(slug)
      end)

      assert :ok = Pgmq.enable_notify_insert(TestRepo, slug, 0)

      assert {:ok, :ok} = send_then(slug, fn -> Flows.delete_flow(TestRepo, slug) end)
      assert %{rows: [[false]]} = queue_exists(slug)
    end

    test "flush_notify_events/1 does nothing when no notify trigger exists" do
      result =
        TestRepo.transaction(fn ->
          drop_all_notify_triggers!()

          assert %{rows: [[0]]} =
                   TestRepo.query!(
                     "SELECT count(*) FROM pg_constraint WHERE conname = $1 AND connamespace = 'pgmq'::regnamespace",
                     [Pgmq.notify_trigger_name()]
                   )

          assert :ok = Pgmq.flush_notify_events(TestRepo)
          assert %{rows: [[1]]} = TestRepo.query!("SELECT 1")
          TestRepo.rollback(:done)
        end)

      assert {:error, :done} = result
    end
  end

  describe "disable_notify_insert/2" do
    test "removes the deferred trigger", %{queue: queue} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, queue, 0)
      assert [_trigger] = triggers(queue)

      assert :ok = Pgmq.disable_notify_insert(TestRepo, queue)
      assert [] = triggers(queue)
    end
  end

  describe "SchemaCheck.warnings/2" do
    setup do
      slug = "notify_defer_flow_#{System.unique_integer([:positive])}"
      step_queue = "notify_defer_step_#{System.unique_integer([:positive])}"
      TestRepo.query!("SELECT pgflow.create_flow($1::text)", [slug])
      TestRepo.query!("SELECT pgflow.add_step($1::text, 'work'::text)", [slug])
      TestRepo.query!("SELECT pgmq.create($1::text)", [step_queue])

      TestRepo.query!("UPDATE pgflow.steps SET queue_name = $2 WHERE flow_slug = $1", [
        slug,
        step_queue
      ])

      on_exit(fn ->
        TestRepo.query!("DELETE FROM pgflow.steps WHERE flow_slug = $1", [slug])
        TestRepo.query!("DELETE FROM pgflow.flows WHERE flow_slug = $1", [slug])
        drop_queue!(slug)
        drop_queue!(step_queue)
      end)

      {:ok, slug: slug, step_queue: step_queue}
    end

    test "reports an immediate trigger only on managed flow queues", %{
      slug: slug,
      step_queue: step_queue,
      queue: unmanaged_queue
    } do
      TestRepo.query!("SELECT pgmq.enable_notify_insert($1::text, 0)", [slug])
      TestRepo.query!("SELECT pgmq.enable_notify_insert($1::text, 0)", [step_queue])
      TestRepo.query!("SELECT pgmq.enable_notify_insert($1::text, 0)", [unmanaged_queue])

      warnings = SchemaCheck.warnings(TestRepo)
      assert [warning] = Enum.filter(warnings, &(&1 =~ "pgmq.q_#{slug}"))
      assert warning =~ "DEFERRABLE INITIALLY DEFERRED"
      refute Enum.any?(warnings, &(&1 =~ "pgmq.q_#{step_queue}"))
      refute Enum.any?(warnings, &(&1 =~ "pgmq.q_#{unmanaged_queue}"))
      assert :ok = SchemaCheck.run(TestRepo)

      refute Enum.any?(
               SchemaCheck.warnings(TestRepo, notify_trigger: :pgmq_default),
               &(&1 =~ "pgmq.q_#{slug}")
             )

      output = capture_io(fn -> CheckSchema.run(["--repo", "PgFlow.TestRepo"]) end)
      assert output =~ "Warnings:"
      assert output =~ "pgmq.q_#{slug}"
      assert output =~ "All checks passed"

      quiet =
        capture_io(fn ->
          CheckSchema.run(["--repo", "PgFlow.TestRepo", "--notify-trigger", "pgmq_default"])
        end)

      refute quiet =~ "pgmq.q_#{slug}"

      assert :ok = Pgmq.enable_notify_insert(TestRepo, slug, 0)
      refute Enum.any?(SchemaCheck.warnings(TestRepo), &(&1 =~ "pgmq.q_#{slug}"))
    end

    test "names ALTER EXTENSION pgmq UPDATE in the immediate-trigger warning", %{slug: slug} do
      TestRepo.query!("SELECT pgmq.enable_notify_insert($1::text, 0)", [slug])

      assert [warning] = Enum.filter(SchemaCheck.warnings(TestRepo), &(&1 =~ "pgmq.q_#{slug}"))
      assert warning =~ "ALTER EXTENSION pgmq UPDATE"
      assert warning =~ "next boot"
    end

    test "reports a managed queue trigger with no throttle row in both modes", %{slug: slug} do
      assert :ok = Pgmq.enable_notify_insert(TestRepo, slug, 0)
      delete_throttle_row!(slug)

      Enum.each([:deferred, :pgmq_default], fn mode ->
        assert [warning] =
                 TestRepo
                 |> SchemaCheck.warnings(notify_trigger: mode)
                 |> Enum.filter(&(&1 =~ "pgmq.q_#{slug}"))

        assert warning =~ "no pgmq.notify_insert_throttle row"
      end)

      assert :ok = Pgmq.enable_notify_insert(TestRepo, slug, 0)
      refute Enum.any?(SchemaCheck.warnings(TestRepo), &(&1 =~ "pgmq.q_#{slug}"))
    end

    test "reports a partitioned managed queue whose listener ignores the trigger argument" do
      slug = "notify_defer_part_#{System.unique_integer([:positive])}"
      TestRepo.query!("SELECT pgflow.create_flow($1::text)", [slug])
      drop_queue!(slug)
      create_partitioned_notify_queue!(slug)

      on_exit(fn ->
        TestRepo.query!("DROP TABLE IF EXISTS pgmq.q_#{slug} CASCADE")
        TestRepo.query!("DELETE FROM pgflow.flows WHERE flow_slug = $1", [slug])
      end)

      assert [warning] =
               TestRepo
               |> SchemaCheck.warnings(notify_trigger: :pgmq_default)
               |> Enum.filter(&(&1 =~ "partitioned"))

      assert warning =~ "pgmq.q_#{slug}"
      assert warning =~ "1.13.1"

      use_tg_argv_listener!()

      refute Enum.any?(SchemaCheck.warnings(TestRepo), &(&1 =~ "partitioned"))
    end
  end

  # Sends one message in a transaction on the test repo, runs `fun` in that
  # same transaction, and commits when `fun` returns `:ok` or `{:ok, _}`.
  # Returns `fun`'s result, or the error that rolled the transaction back.
  defp send_then(queue, fun) do
    TestRepo.transaction(fn ->
      TestRepo.query!(@send_sql, [queue])

      case fun.() do
        :ok -> :ok
        {:ok, result} -> result
        {:error, error} -> TestRepo.rollback(error)
      end
    end)
  end

  defp queue_exists(queue) do
    TestRepo.query!(
      "SELECT to_regclass(format('pgmq.%I', pgmq.format_table_name($1::text, 'q'))) IS NOT NULL",
      [queue]
    )
  end

  defp delete_throttle_row!(queue) do
    TestRepo.query!("DELETE FROM pgmq.notify_insert_throttle WHERE queue_name = $1", [queue])
  end

  # Drops every notify trigger in the pgmq schema; callers run it inside a
  # transaction that they roll back.
  defp drop_all_notify_triggers! do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT format('DROP TRIGGER %I ON %s', t.tgname, t.tgrelid::regclass)
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        WHERE c.relnamespace = 'pgmq'::regnamespace
          AND t.tgname = $1
          AND t.tgparentid = 0
        """,
        [Pgmq.notify_trigger_name()]
      )

    Enum.each(rows, fn [drop_sql] -> TestRepo.query!(drop_sql) end)
  end

  # Creates a range-partitioned `pgmq.q_<queue>` table with one partition and
  # pgflow's deferred notify trigger on the parent, which Postgres clones onto
  # the partition. No `pgmq.meta` or throttle row is created.
  defp create_partitioned_notify_queue!(queue) do
    TestRepo.query!("""
    CREATE TABLE pgmq.q_#{queue} (
      msg_id bigint NOT NULL,
      read_ct integer NOT NULL DEFAULT 0,
      enqueued_at timestamptz NOT NULL DEFAULT now(),
      vt timestamptz NOT NULL,
      message jsonb
    ) PARTITION BY RANGE (msg_id)
    """)

    TestRepo.query!(
      "CREATE TABLE pgmq.q_#{queue}_p0 PARTITION OF pgmq.q_#{queue} FOR VALUES FROM (0) TO (1000)"
    )

    TestRepo.query!("""
    CREATE CONSTRAINT TRIGGER #{Pgmq.notify_trigger_name()} AFTER INSERT ON pgmq.q_#{queue}
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    EXECUTE FUNCTION pgmq.notify_queue_listeners('#{queue}')
    """)
  end

  # Opens a transaction on its own connection, sends one message, and keeps
  # the transaction open until `finish/2`.
  defp hold_open_send(queue) do
    hold_open(fn tx -> Postgrex.query!(tx, @send_sql, [queue]) end)
  end

  # Holds ROW EXCLUSIVE on the queue table, the lock a producer's insert takes,
  # in an open transaction until `release/1`.
  defp hold_table_lock(queue) do
    hold_open(fn tx ->
      Postgrex.query!(tx, "LOCK TABLE pgmq.q_#{queue} IN ROW EXCLUSIVE MODE", [])
    end)
  end

  defp hold_open(statement) do
    parent = self()

    task =
      Task.async(fn ->
        {:ok, conn} = Postgrex.start_link(connection_opts())

        result =
          Postgrex.transaction(conn, fn tx ->
            statement.(tx)
            send(parent, {:holder_ready, self()})

            receive do
              :commit -> :committed
              :rollback -> Postgrex.rollback(tx, :rolled_back)
            end
          end)

        GenServer.stop(conn)
        result
      end)

    assert_receive {:holder_ready, holder_pid}, 5_000
    %{task: task, pid: holder_pid}
  end

  defp finish(%{task: task, pid: pid}, :commit) do
    send(pid, :commit)
    assert {:ok, :committed} = Task.await(task, 5_000)
  end

  defp finish(%{task: task, pid: pid}, :rollback) do
    send(pid, :rollback)
    assert {:error, :rolled_back} = Task.await(task, 5_000)
  end

  defp release(holder), do: finish(holder, :rollback)

  # Sends and commits one message on a separate connection, failing fast if any
  # lock is held against it.
  defp send_with_lock_timeout(queue) do
    in_fast_transaction(fn tx -> Postgrex.query(tx, @send_sql, [queue]) end, :sent)
  end

  # Tries to lock the queue's throttle row from a separate connection without
  # waiting.
  defp lock_throttle_row(queue) do
    in_fast_transaction(
      fn tx ->
        Postgrex.query(
          tx,
          "SELECT 1 FROM pgmq.notify_insert_throttle WHERE queue_name = $1 FOR UPDATE NOWAIT",
          [queue]
        )
      end,
      :locked
    )
  end

  defp in_fast_transaction(statement, success) do
    with_conn(fn conn ->
      Postgrex.transaction(conn, &run_with_lock_timeout(&1, statement, success))
    end)
  end

  defp run_with_lock_timeout(tx, statement, success) do
    Postgrex.query!(tx, "SET LOCAL lock_timeout = '200ms'", [])

    case statement.(tx) do
      {:ok, _} -> success
      {:error, error} -> Postgrex.rollback(tx, error)
    end
  end

  defp listen!(channel) do
    notifications = start_supervised!({Postgrex.Notifications, connection_opts()})
    {:ok, _ref} = Postgrex.Notifications.listen(notifications, channel)
    notifications
  end

  defp triggers(queue) do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT t.oid, t.tgdeferrable, t.tginitdeferred,
               array_remove(string_to_array(encode(t.tgargs, 'escape'), '\\000'), '')
        FROM pg_trigger t
        WHERE t.tgrelid = to_regclass(format('pgmq.%I', pgmq.format_table_name($1::text, 'q')))
          AND t.tgname = $2
        """,
        [queue, Pgmq.notify_trigger_name()]
      )

    Enum.map(rows, fn [oid, deferrable, initially_deferred, args] ->
      %{
        oid: oid,
        deferrable: deferrable,
        initially_deferred: initially_deferred,
        args: args || []
      }
    end)
  end

  defp throttle_row(queue) do
    TestRepo.query!(
      "SELECT throttle_interval_ms FROM pgmq.notify_insert_throttle WHERE queue_name = $1",
      [queue]
    )
  end

  defp throttle_interval(queue) do
    %{rows: [[interval]]} = throttle_row(queue)
    interval
  end

  # Swaps in pgmq main's TG_ARGV-reading listener for the rest of the test.
  # The restore runs in `on_exit`, which still runs when the test process is
  # killed.
  defp use_tg_argv_listener! do
    %{rows: [[original]]} =
      TestRepo.query!("SELECT pg_get_functiondef('pgmq.notify_queue_listeners()'::regprocedure)")

    on_exit(fn -> with_conn(&Postgrex.query!(&1, original, [])) end)
    TestRepo.query!(@tg_argv_listener_sql)
  end

  # Installs an event trigger that rejects any initially deferred trigger
  # created on a `notify_defer_swap_*` queue table, so pgmq's own (immediate)
  # trigger is created and pgflow's deferred swap fails. Removed in `on_exit`.
  defp reject_deferred_notify_triggers! do
    on_exit(fn ->
      with_conn(fn conn ->
        Postgrex.query!(conn, "DROP EVENT TRIGGER IF EXISTS #{@reject_deferred_trigger}", [])
        Postgrex.query!(conn, "DROP FUNCTION IF EXISTS public.#{@reject_deferred_trigger}()", [])
      end)
    end)

    TestRepo.query!("""
    CREATE OR REPLACE FUNCTION public.#{@reject_deferred_trigger}()
    RETURNS event_trigger AS $$
    DECLARE
      cmd record;
    BEGIN
      FOR cmd IN SELECT objid FROM pg_event_trigger_ddl_commands() WHERE object_type = 'trigger' LOOP
        IF EXISTS (
          SELECT 1
          FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
          WHERE t.oid = cmd.objid
            AND t.tginitdeferred
            AND c.relname LIKE 'q\\_notify\\_defer\\_swap\\_%'
        ) THEN
          RAISE EXCEPTION 'deferred notify trigger rejected by test'
            USING ERRCODE = 'insufficient_privilege';
        END IF;
      END LOOP;
    END;
    $$ LANGUAGE plpgsql
    """)

    TestRepo.query!("""
    CREATE EVENT TRIGGER #{@reject_deferred_trigger} ON ddl_command_end
    WHEN TAG IN ('CREATE TRIGGER')
    EXECUTE FUNCTION public.#{@reject_deferred_trigger}()
    """)
  end

  defp drop_queue!(queue) do
    TestRepo.query!(
      "SELECT pgmq.drop_queue(queue_name) FROM pgmq.meta WHERE queue_name = $1",
      [queue]
    )
  end

  defp notify_api_present?(conn) do
    %{rows: [[present]]} =
      Postgrex.query!(
        conn,
        "SELECT to_regprocedure('pgmq.enable_notify_insert(text,integer)') IS NOT NULL",
        []
      )

    present
  end

  defp load_notify_objects! do
    repo_config = TestRepo.config()

    {output, status} =
      System.cmd("psql", ["-1" | DatabaseHelpers.psql_args(repo_config, @notify_sql_path)],
        env: [{"PGPASSWORD", Keyword.fetch!(repo_config, :password)}],
        stderr_to_stdout: true
      )

    if status != 0, do: raise("failed to load #{@notify_sql_path}: #{output}")
  end

  defp drop_notify_objects!(conn) do
    [
      "DROP FUNCTION IF EXISTS pgmq.enable_notify_insert(text, integer)",
      "DROP FUNCTION IF EXISTS pgmq.disable_notify_insert(text)",
      "DROP FUNCTION IF EXISTS pgmq.notify_queue_listeners() CASCADE",
      "DROP TABLE IF EXISTS pgmq.notify_insert_throttle"
    ]
    |> Enum.each(&Postgrex.query!(conn, &1, []))
  end

  defp with_conn(fun) do
    {:ok, conn} = Postgrex.start_link(connection_opts())

    try do
      fun.(conn)
    after
      GenServer.stop(conn)
    end
  end

  defp connection_opts, do: DatabaseHelpers.connection_opts(TestRepo.config())
end
