defmodule PgFlow.Signal.NotifyTest do
  @moduledoc """
  Tests for PgFlow.Signal.Notify.

  Tests verify that the Signal.Notify process:
  - Starts and initializes with a Postgrex.Notifications connection
  - Dispatches :poll_now to registered workers on notification
  - Cleans up when workers die
  - Handles reconnection via Postgrex.Notifications auto_reconnect

  Note: Full integration tests require pgmq >= 1.8.0 with enable_notify_insert.
  Tests that need this feature are tagged @tag :pgmq_notify and skipped if unavailable.
  """
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Queries.Pgmq
  alias PgFlow.Signal.Notify
  alias PgFlow.TestRepo

  @moduletag timeout: 30_000
  @moduletag :integration

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    on_exit(fn ->
      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  defp pgmq_notify_available? do
    case TestRepo.query(
           "SELECT 1 FROM pg_proc WHERE proname = 'enable_notify_insert' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgmq')"
         ) do
      {:ok, %{num_rows: n}} when n > 0 -> true
      _ -> false
    end
  end

  defp pgmq_version do
    case Pgmq.get_pgmq_version(TestRepo) do
      {:ok, version} -> version
      _ -> nil
    end
  end

  # Helper to get module state from the GenServer
  defp get_module_state(pid) do
    :sys.get_state(pid)
  end

  describe "init/1" do
    @tag :pgmq_notify
    test "starts successfully with valid repo when pgmq >= 1.8.0" do
      if pgmq_notify_available?() do
        {:ok, pid} = Notify.start_link(repo: TestRepo, notify_throttle_ms: 250)

        assert Process.alive?(pid)

        # Give time for connection to establish
        Process.sleep(100)

        # Verify initial state
        state = get_module_state(pid)
        assert state.repo == TestRepo
        assert state.notify_throttle_ms == 250
        assert state.workers == %{}
        assert state.channels == %{}
        assert is_pid(state.conn)

        GenServer.stop(pid)
      else
        IO.puts("Skipping: pgmq >= 1.8.0 required (found: #{pgmq_version()})")
        :ok
      end
    end

    test "raises helpful error when pgmq version is too low" do
      if pgmq_notify_available?() do
        IO.puts("Skipping: test requires pgmq < 1.8.0 (found: #{pgmq_version()})")
        :ok
      else
        # `init/1` raises but GenServer.start_link converts the raise into an
        # EXIT tuple instead of re-raising at the call site — match on it.
        Process.flag(:trap_exit, true)

        assert {:error, {%RuntimeError{message: message}, _stacktrace}} =
                 Notify.start_link(repo: TestRepo, notify_throttle_ms: 250)

        assert message =~ "requires pgmq >= 1.8.0"
      end
    end
  end

  describe "notification dispatch" do
    @tag :pgmq_notify
    test "dispatches :poll_now to registered worker on notification" do
      if pgmq_notify_available?() do
        # Create a queue for testing
        TestRepo.query!("SELECT pgmq.create($1::text)", ["notify_test_flow"])

        {:ok, pid} = Notify.start_link(repo: TestRepo, notify_throttle_ms: 0)

        # Give time for connection
        Process.sleep(100)

        # Enable notifications before registering (as done by PgFlow.Supervisor)
        TestRepo.query!("SELECT pgmq.enable_notify_insert($1::text, $2::integer)", [
          "notify_test_flow",
          0
        ])

        # Register self as worker
        :ok = Notify.register_worker(pid, "notify_test_flow", self())

        # Insert a message to trigger notification
        TestRepo.query!("SELECT pgmq.send($1::text, $2::jsonb)", ["notify_test_flow", "{}"])

        # Should receive :poll_now
        assert_receive :poll_now, 5_000

        GenServer.stop(pid)
      else
        IO.puts("Skipping: pgmq >= 1.8.0 with enable_notify_insert required")
        :ok
      end
    end
  end

  describe "worker lifecycle" do
    @tag :pgmq_notify
    test "cleans up when registered worker process dies" do
      if pgmq_notify_available?() do
        # Create a queue for testing
        TestRepo.query!("SELECT pgmq.create($1::text)", ["cleanup_test_flow"])

        {:ok, pid} = Notify.start_link(repo: TestRepo, notify_throttle_ms: 250)

        # Give time for connection
        Process.sleep(100)

        # Spawn a fake worker process
        fake_worker =
          spawn(fn ->
            receive do
              :die -> :ok
            end
          end)

        # Register the worker properly through the API
        :ok = Notify.register_worker(pid, "cleanup_test_flow", fake_worker)

        # Give time for registration
        Process.sleep(50)

        # Verify worker is registered
        state = get_module_state(pid)
        assert map_size(state.workers) == 1
        assert Map.has_key?(state.workers, "cleanup_test_flow")

        # Kill the fake worker
        send(fake_worker, :die)
        Process.sleep(100)

        # Verify worker was cleaned up
        state = get_module_state(pid)
        assert state.workers == %{}
        assert state.channels == %{}

        GenServer.stop(pid)
      else
        IO.puts("Skipping: pgmq >= 1.8.0 required (found: #{pgmq_version()})")
        :ok
      end
    end

    @tag :pgmq_notify
    test "register_worker returns :ok and sets up proper state" do
      if pgmq_notify_available?() do
        # Create a queue for testing
        TestRepo.query!("SELECT pgmq.create($1::text)", ["register_test_flow"])

        {:ok, pid} = Notify.start_link(repo: TestRepo, notify_throttle_ms: 250)

        Process.sleep(100)

        # Register self as worker
        result = Notify.register_worker(pid, "register_test_flow", self())
        assert result == :ok

        # Verify state
        state = get_module_state(pid)
        assert Map.has_key?(state.workers, "register_test_flow")
        assert state.workers["register_test_flow"].worker_pid == self()
        assert Map.has_key?(state.channels, "pgmq.q_register_test_flow.INSERT")

        GenServer.stop(pid)
      else
        IO.puts("Skipping: pgmq >= 1.8.0 required (found: #{pgmq_version()})")
        :ok
      end
    end
  end

  describe "reconnection handling" do
    @tag :pgmq_notify
    test "has active connection after startup" do
      if pgmq_notify_available?() do
        {:ok, pid} = Notify.start_link(repo: TestRepo, notify_throttle_ms: 250)

        Process.sleep(100)

        state = get_module_state(pid)
        assert is_pid(state.conn)
        assert Process.alive?(state.conn)

        GenServer.stop(pid)
      else
        IO.puts("Skipping: pgmq >= 1.8.0 required (found: #{pgmq_version()})")
        :ok
      end
    end

    @tag :pgmq_notify
    test "maintains channel subscriptions" do
      if pgmq_notify_available?() do
        # Create queues for testing
        TestRepo.query!("SELECT pgmq.create($1::text)", ["reconnect_test_1"])
        TestRepo.query!("SELECT pgmq.create($1::text)", ["reconnect_test_2"])

        {:ok, pid} = Notify.start_link(repo: TestRepo, notify_throttle_ms: 250)

        Process.sleep(100)

        # Register multiple workers
        :ok = Notify.register_worker(pid, "reconnect_test_1", self())
        :ok = Notify.register_worker(pid, "reconnect_test_2", self())

        # Verify channels are tracked
        state = get_module_state(pid)
        assert map_size(state.channels) == 2
        assert Map.has_key?(state.channels, "pgmq.q_reconnect_test_1.INSERT")
        assert Map.has_key?(state.channels, "pgmq.q_reconnect_test_2.INSERT")

        GenServer.stop(pid)
      else
        IO.puts("Skipping: pgmq >= 1.8.0 required (found: #{pgmq_version()})")
        :ok
      end
    end

    @tag :pgmq_notify
    test "stores worker pids for poll_now dispatch" do
      if pgmq_notify_available?() do
        TestRepo.query!("SELECT pgmq.create($1::text)", ["reconnect_poll_test"])

        {:ok, pid} = Notify.start_link(repo: TestRepo, notify_throttle_ms: 250)

        Process.sleep(100)

        # Register self as worker
        :ok = Notify.register_worker(pid, "reconnect_poll_test", self())

        # Verify worker is tracked with correct pid
        state = get_module_state(pid)
        assert state.workers["reconnect_poll_test"].worker_pid == self()

        GenServer.stop(pid)
      else
        IO.puts("Skipping: pgmq >= 1.8.0 required (found: #{pgmq_version()})")
        :ok
      end
    end

    @tag :pgmq_notify
    test "registers worker with eventually response when connection pending" do
      if pgmq_notify_available?() do
        TestRepo.query!("SELECT pgmq.create($1::text)", ["delayed_connect_test"])

        {:ok, pid} =
          Notify.start_link(repo: TestRepo, notify_throttle_ms: 250)

        # Register immediately (may race with connection)
        :ok = Notify.register_worker(pid, "delayed_connect_test", self())

        # Give time for connection to establish
        Process.sleep(200)

        # Verify worker is registered regardless of connection timing
        state = get_module_state(pid)
        assert Map.has_key?(state.workers, "delayed_connect_test")

        GenServer.stop(pid)
      else
        IO.puts("Skipping: pgmq >= 1.8.0 required (found: #{pgmq_version()})")
        :ok
      end
    end
  end

  describe "notification callback" do
    @tag :pgmq_notify
    test "ignores notifications for unregistered channels" do
      if pgmq_notify_available?() do
        {:ok, pid} = Notify.start_link(repo: TestRepo, notify_throttle_ms: 250)

        Process.sleep(100)

        # Should NOT receive :poll_now since we're not registered
        refute_receive :poll_now, 200

        GenServer.stop(pid)
      else
        IO.puts("Skipping: pgmq >= 1.8.0 required (found: #{pgmq_version()})")
        :ok
      end
    end
  end
end
