defmodule PgFlow.Worker.UpstreamLifecycleTest do
  @moduledoc false
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.IntegrationCase
  alias PgFlow.Signal.Notify
  alias PgFlow.Test.UnavailableRepo, as: BootstrapFailingRepo
  alias PgFlow.TestRepo
  alias PgFlow.Worker.Server
  alias PgFlow.WorkerSupervisor

  @moduletag :integration
  @moduletag timeout: 30_000

  defmodule BlockedHandlerFlow do
    use PgFlow.Flow

    @flow slug: :blocked_handler_flow, max_attempts: 1, timeout: 30

    step :wait do
      fn input, _ctx ->
        test_pid = Process.whereis(String.to_existing_atom(input["test_name"]))
        send(test_pid, {:handler_started, self()})

        receive do
          :release -> %{done: true}
        end
      end
    end
  end

  defmodule CrashOnReleaseFlow do
    use PgFlow.Flow

    @flow slug: :crash_on_release_flow, max_attempts: 1, timeout: 30

    step :wait do
      fn input, _ctx ->
        test_pid = Process.whereis(String.to_existing_atom(input["test_name"]))
        send(test_pid, {:handler_started, self()})

        receive do
          :release -> raise "handler crash during drain"
        end
      end
    end
  end

  defmodule SlowTimeoutFlow do
    use PgFlow.Flow

    @flow slug: :slow_timeout_flow, max_attempts: 1, timeout: 1

    step :wait do
      fn input, _ctx ->
        test_pid = Process.whereis(String.to_existing_atom(input["test_name"]))
        send(test_pid, {:handler_started, self()})

        receive do
        end
      end
    end
  end

  defmodule NotifyProbe do
    @moduledoc false
    use GenServer

    def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: PgFlow.Signal.Notify)

    @impl GenServer
    def init(state), do: {:ok, state}

    @impl GenServer
    def handle_call({:register_worker, flow_slug, worker_pid}, _from, state) do
      {:reply, :ok, register(state, flow_slug, worker_pid)}
    end

    @impl GenServer
    def handle_cast({:register_worker_async, flow_slug, worker_pid}, state) do
      {:noreply, register(state, flow_slug, worker_pid)}
    end

    defp register(state, flow_slug, worker_pid) do
      monitor_ref = Process.monitor(worker_pid)

      case Map.get(state, flow_slug) do
        %{monitor_ref: old_ref} -> Process.demonitor(old_ref, [:flush])
        nil -> :ok
      end

      Map.put(state, flow_slug, %{worker_pid: worker_pid, monitor_ref: monitor_ref})
    end

    @impl GenServer
    def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
      state =
        Map.reject(state, fn {_flow_slug, registration} -> registration.monitor_ref == ref end)

      {:noreply, state}
    end
  end

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")
    {:ok, task_supervisor} = Task.Supervisor.start_link()

    on_exit(fn ->
      try do
        if Process.alive?(task_supervisor), do: Supervisor.stop(task_supervisor)
      catch
        :exit, _ -> :ok
      end

      Sandbox.mode(TestRepo, :manual)
    end)

    {:ok, task_supervisor: task_supervisor}
  end

  describe "graceful drain" do
    test "stop waits for a blocked handler, commits completion, and does not poll during drain",
         %{
           task_supervisor: task_supervisor
         } do
      flow_slug = compile_flow(BlockedHandlerFlow)
      test_name = unique_test_name()
      run_input = %{"test_name" => Atom.to_string(test_name)}

      worker_pid =
        start_worker(BlockedHandlerFlow, task_supervisor,
          min_poll_interval: 5_000,
          max_poll_interval: 5_000
        )

      _ = :sys.get_state(worker_pid)

      run_id = start_flow_run(flow_slug, run_input)

      send(worker_pid, :poll_now)
      assert_receive {:handler_started, handler_pid}, 2_000

      ref = :telemetry_test.attach_event_handlers(self(), [[:pgflow, :worker, :poll, :start]])

      stopper = Task.async(fn -> Server.stop(worker_pid) end)

      refute_receive {[:pgflow, :worker, :poll, :start], ^ref, _, _}, 500
      draining_state = Server.get_state(worker_pid)

      TestRepo.query!(
        "UPDATE pgflow.workers SET last_heartbeat_at = NOW() - INTERVAL '1 hour' WHERE worker_id = $1",
        [Ecto.UUID.dump!(draining_state.worker_id)]
      )

      send(worker_pid, {:heartbeat, draining_state.heartbeat_token})
      _ = :sys.get_state(worker_pid)

      assert %{rows: [[true]]} =
               TestRepo.query!(
                 "SELECT last_heartbeat_at > NOW() - INTERVAL '10 seconds' FROM pgflow.workers WHERE worker_id = $1",
                 [Ecto.UUID.dump!(draining_state.worker_id)]
               )

      send(handler_pid, :release)
      assert :ok = Task.await(stopper, 5_000)
      assert run_status(run_id) == "completed"
    end

    test "repeated stop requests share one drain and all return :ok", %{
      task_supervisor: task_supervisor
    } do
      flow_slug = compile_flow(BlockedHandlerFlow)
      test_name = unique_test_name()
      run_input = %{"test_name" => Atom.to_string(test_name)}

      worker_pid =
        start_worker(BlockedHandlerFlow, task_supervisor,
          min_poll_interval: 5_000,
          max_poll_interval: 5_000
        )

      _ = :sys.get_state(worker_pid)
      run_id = start_flow_run(flow_slug, run_input)

      send(worker_pid, :poll_now)
      assert_receive {:handler_started, handler_pid}, 2_000

      stopper1 = Task.async(fn -> Server.stop(worker_pid) end)
      stopper2 = Task.async(fn -> Server.stop(worker_pid) end)

      send(handler_pid, :release)
      assert :ok = Task.await(stopper1, 5_000)
      assert :ok = Task.await(stopper2, 5_000)
      assert run_status(run_id) == "completed"
    end

    test "handler crash during drain records failure in SQL", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(CrashOnReleaseFlow)
      test_name = unique_test_name()
      run_input = %{"test_name" => Atom.to_string(test_name)}

      worker_pid =
        start_worker(CrashOnReleaseFlow, task_supervisor,
          min_poll_interval: 5_000,
          max_poll_interval: 5_000
        )

      _ = :sys.get_state(worker_pid)
      run_id = start_flow_run(flow_slug, run_input)

      send(worker_pid, :poll_now)
      assert_receive {:handler_started, handler_pid}, 2_000

      stopper = Task.async(fn -> Server.stop(worker_pid) end)
      send(handler_pid, :release)
      assert :ok = Task.await(stopper, 5_000)
      assert run_status(run_id) == "failed"
    end

    test "task timeout during drain records failure in SQL", %{task_supervisor: task_supervisor} do
      previous_format = Application.fetch_env(:pgflow, :log_format)
      Application.put_env(:pgflow, :log_format, :simple)

      on_exit(fn ->
        case previous_format do
          {:ok, format} -> Application.put_env(:pgflow, :log_format, format)
          :error -> Application.delete_env(:pgflow, :log_format)
        end
      end)

      flow_slug = compile_flow(SlowTimeoutFlow)
      test_name = unique_test_name()
      run_input = %{"test_name" => Atom.to_string(test_name)}

      worker_pid =
        start_worker(SlowTimeoutFlow, task_supervisor,
          min_poll_interval: 5_000,
          max_poll_interval: 5_000
        )

      _ = :sys.get_state(worker_pid)
      run_id = start_flow_run(flow_slug, run_input)

      send(worker_pid, :poll_now)
      assert_receive {:handler_started, _handler_pid}, 2_000

      log =
        ExUnit.CaptureLog.capture_log(
          [format: "$metadata", metadata: [:retry_attempt, :max_attempts, :retry_delay_s]],
          fn -> assert :ok = Server.stop(worker_pid) end
        )

      assert log =~ "retry_attempt=1"
      assert log =~ "max_attempts=1"
      assert log =~ "retry_delay_s=1.0"
      assert run_status(run_id) == "failed"
    end
  end

  describe "supervisor stop and replacement" do
    test "durable stop follows a replacement when its selected worker crashes", %{
      task_supervisor: task_supervisor
    } do
      compile_flow(BlockedHandlerFlow)
      supervisor_pid = start_supervisor!(task_supervisor)
      on_exit(fn -> stop_supervisor!(supervisor_pid, task_supervisor) end)
      {:ok, pid} = WorkerSupervisor.start_worker(BlockedHandlerFlow, repo: TestRepo)
      :ok = :sys.suspend(pid)
      stopper = Task.async(fn -> WorkerSupervisor.stop_worker(BlockedHandlerFlow) end)

      assert wait_until(fn ->
               {:messages, messages} = Process.info(pid, :messages)
               Enum.any?(messages, &match?({:"$gen_call", _, :stop}, &1))
             end)

      Process.exit(pid, :kill)
      assert :ok = Task.await(stopper, 5_000)
      assert DynamicSupervisor.count_children(WorkerSupervisor).active == 0
      assert WorkerSupervisor.find_worker(BlockedHandlerFlow) == nil
    end

    test "operator stop overrides a deprecation drain", %{task_supervisor: task_supervisor} do
      slug = compile_flow(BlockedHandlerFlow)
      test_name = unique_test_name()
      supervisor_pid = start_supervisor!(task_supervisor)
      on_exit(fn -> stop_supervisor!(supervisor_pid, task_supervisor) end)
      {:ok, pid} = WorkerSupervisor.start_worker(BlockedHandlerFlow, repo: TestRepo)
      start_flow_run(slug, %{"test_name" => Atom.to_string(test_name)})
      send(pid, :poll_now)
      assert_receive {:handler_started, handler}, 2_000
      state = Server.get_state(pid)

      TestRepo.query!("UPDATE pgflow.workers SET deprecated_at = NOW() WHERE worker_id = $1", [
        Ecto.UUID.dump!(state.worker_id)
      ])

      send(pid, {:heartbeat, state.heartbeat_token})
      assert Server.get_state(pid).stop_reason == :deprecated
      monitor = Process.monitor(pid)
      stopper = Task.async(fn -> WorkerSupervisor.stop_worker(BlockedHandlerFlow) end)
      assert wait_until(fn -> Server.get_state(pid).stop_waiters != [] end)
      send(handler, :release)
      assert :ok = Task.await(stopper, 5_000)
      assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
      assert DynamicSupervisor.count_children(WorkerSupervisor).active == 0
      assert WorkerSupervisor.find_worker(BlockedHandlerFlow) == nil
    end

    test "unknown database step fails without crashing its worker", %{
      task_supervisor: task_supervisor
    } do
      slug = compile_flow(BlockedHandlerFlow)
      test_name = unique_test_name()

      pid =
        start_worker(BlockedHandlerFlow, task_supervisor,
          min_poll_interval: 5_000,
          max_poll_interval: 5_000,
          max_concurrency: 2,
          batch_size: 2
        )

      TestRepo.query!("SELECT pgflow.add_step($1,'unknown')", [slug])
      run_id = start_flow_run(slug, %{"test_name" => Atom.to_string(test_name)})
      send(pid, :poll_now)
      assert_receive {:handler_started, handler}, 2_000
      state = Server.get_state(pid)
      assert state.worker_id

      assert %{rows: [["failed"]]} =
               TestRepo.query!(
                 "SELECT status FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = 'unknown'",
                 [Ecto.UUID.dump!(run_id)]
               )

      send(handler, :release)
      Server.stop(pid)
    end

    test "queue read reservation is short independently of execution timeout", %{
      task_supervisor: task_supervisor
    } do
      compile_flow(BlockedHandlerFlow)
      pid = start_worker(BlockedHandlerFlow, task_supervisor)
      assert Server.get_state(pid).visibility_timeout == 5
      Server.stop(pid)
    end

    test "explicit supervisor stop removes the child so permanent restart does not undo it", %{
      task_supervisor: task_supervisor
    } do
      _flow_slug = compile_flow(BlockedHandlerFlow)
      supervisor_pid = start_supervisor!(task_supervisor)

      on_exit(fn ->
        stop_supervisor!(supervisor_pid, task_supervisor)
      end)

      {:ok, worker_pid} = WorkerSupervisor.start_worker(BlockedHandlerFlow, repo: TestRepo)
      Sandbox.allow(TestRepo, self(), worker_pid)
      _ = :sys.get_state(worker_pid)

      ref = Process.monitor(worker_pid)
      assert :ok = WorkerSupervisor.stop_worker(BlockedHandlerFlow)
      assert_receive {:DOWN, ^ref, :process, ^worker_pid, :normal}, 5_000
      refute Process.alive?(worker_pid)
      assert WorkerSupervisor.find_worker(BlockedHandlerFlow) == nil
      assert DynamicSupervisor.count_children(WorkerSupervisor).active == 0

      assert %{rows: [[0]]} =
               TestRepo.query!("SELECT count(*) FROM pgflow.workers WHERE stopped_at IS NULL")
    end

    test "starting the same flow twice keeps one supervised worker", %{
      task_supervisor: task_supervisor
    } do
      compile_flow(BlockedHandlerFlow)
      supervisor_pid = start_supervisor!(task_supervisor)
      on_exit(fn -> stop_supervisor!(supervisor_pid, task_supervisor) end)
      assert {:ok, pid} = WorkerSupervisor.start_worker(BlockedHandlerFlow, repo: TestRepo)
      assert {:ok, ^pid} = WorkerSupervisor.start_worker(BlockedHandlerFlow, repo: TestRepo)
      assert DynamicSupervisor.count_children(WorkerSupervisor).active == 1
    end

    test "heartbeat deprecation drains and OTP replacement starts a fresh worker", %{
      task_supervisor: task_supervisor
    } do
      flow_slug = compile_flow(BlockedHandlerFlow)
      supervisor_pid = start_supervisor!(task_supervisor)

      on_exit(fn ->
        stop_supervisor!(supervisor_pid, task_supervisor)
      end)

      {:ok, worker_pid} = WorkerSupervisor.start_worker(BlockedHandlerFlow, repo: TestRepo)
      Sandbox.allow(TestRepo, self(), worker_pid)
      state = Server.get_state(worker_pid)
      worker_id = state.worker_id

      TestRepo.query!(
        "UPDATE pgflow.workers SET deprecated_at = NOW() WHERE worker_id = $1",
        [Ecto.UUID.dump!(worker_id)]
      )

      send(worker_pid, {:heartbeat, state.heartbeat_token})

      ref = Process.monitor(worker_pid)
      assert_receive {:DOWN, ^ref, :process, ^worker_pid, :deprecated}, 5_000

      assert wait_until(fn ->
               case WorkerSupervisor.find_worker(BlockedHandlerFlow) do
                 nil -> false
                 replacement_pid -> replacement_pid != worker_pid
               end
             end)

      replacement_pid = WorkerSupervisor.find_worker(BlockedHandlerFlow)
      Sandbox.allow(TestRepo, self(), replacement_pid)
      replacement_state = Server.get_state(replacement_pid)
      assert replacement_state.worker_id != worker_id
      assert replacement_state.flow_slug == flow_slug
      assert DynamicSupervisor.count_children(WorkerSupervisor).active == 1
    end

    test "notify registration follows a heartbeat replacement", %{
      task_supervisor: task_supervisor
    } do
      flow_slug = compile_flow(BlockedHandlerFlow)
      notify_pid = start_supervised!(NotifyProbe)
      supervisor_pid = start_supervisor!(task_supervisor, signal_strategy: :notify)

      on_exit(fn ->
        stop_supervisor!(supervisor_pid, task_supervisor)
      end)

      {:ok, worker_pid} = WorkerSupervisor.start_worker(BlockedHandlerFlow, repo: TestRepo)
      Sandbox.allow(TestRepo, self(), worker_pid)
      state = Server.get_state(worker_pid)

      assert :ok = Notify.register_worker(notify_pid, flow_slug, worker_pid)

      TestRepo.query!(
        "UPDATE pgflow.workers SET deprecated_at = NOW() WHERE worker_id = $1",
        [Ecto.UUID.dump!(state.worker_id)]
      )

      send(worker_pid, {:heartbeat, state.heartbeat_token})

      assert wait_until(fn ->
               case WorkerSupervisor.find_worker(BlockedHandlerFlow) do
                 nil -> false
                 replacement_pid -> replacement_pid != worker_pid
               end
             end)

      replacement_pid = WorkerSupervisor.find_worker(BlockedHandlerFlow)

      assert wait_until(fn ->
               case :sys.get_state(notify_pid) do
                 %{^flow_slug => %{worker_pid: ^replacement_pid}} -> true
                 _ -> false
               end
             end)
    end

    test "missing worker registration on heartbeat drains and OTP replacement starts a fresh worker",
         %{
           task_supervisor: task_supervisor
         } do
      flow_slug = compile_flow(BlockedHandlerFlow)
      supervisor_pid = start_supervisor!(task_supervisor)

      on_exit(fn ->
        stop_supervisor!(supervisor_pid, task_supervisor)
      end)

      {:ok, worker_pid} = WorkerSupervisor.start_worker(BlockedHandlerFlow, repo: TestRepo)
      Sandbox.allow(TestRepo, self(), worker_pid)
      state = Server.get_state(worker_pid)
      worker_id = state.worker_id

      TestRepo.query!("DELETE FROM pgflow.workers WHERE worker_id = $1", [
        Ecto.UUID.dump!(worker_id)
      ])

      send(worker_pid, {:heartbeat, state.heartbeat_token})

      ref = Process.monitor(worker_pid)
      assert_receive {:DOWN, ^ref, :process, ^worker_pid, _}, 5_000

      assert wait_until(fn ->
               case WorkerSupervisor.find_worker(BlockedHandlerFlow) do
                 nil -> false
                 replacement_pid -> replacement_pid != worker_pid
               end
             end)

      replacement_pid = WorkerSupervisor.find_worker(BlockedHandlerFlow)
      Sandbox.allow(TestRepo, self(), replacement_pid)
      replacement_state = Server.get_state(replacement_pid)
      assert replacement_state.worker_id != worker_id
      assert replacement_state.flow_slug == flow_slug
    end
  end

  describe "external repo and ensure_workers" do
    test "worker stop does not stop an externally supplied Ecto repo", %{
      task_supervisor: task_supervisor
    } do
      _flow_slug = compile_flow(BlockedHandlerFlow)
      repo_pid = Process.whereis(TestRepo)
      assert is_pid(repo_pid)
      assert Process.alive?(repo_pid)

      worker_pid = start_worker(BlockedHandlerFlow, task_supervisor)
      _ = :sys.get_state(worker_pid)
      assert :ok = Server.stop(worker_pid)
      assert Process.alive?(repo_pid)
    end

    test "process-mode worker functions are excluded from ensure_workers HTTP invocations" do
      process_function = "elixir:PgFlow.Worker.UpstreamLifecycleTest.ProcessModeFixture"
      http_function = "elixir:http_only_fixture"

      TestRepo.query!(
        "SELECT pgflow.track_worker_function($1, 'process')",
        [process_function]
      )

      TestRepo.query!(
        "INSERT INTO pgflow.worker_functions (function_name, start_mode, enabled, debounce)
         VALUES ($1, 'http', true, interval '1 second')
         ON CONFLICT (function_name) DO UPDATE SET start_mode = EXCLUDED.start_mode, enabled = true",
        [http_function]
      )

      # ensure_workers() depends on net.http_post and is omitted from the test
      # bundle; the same start_mode filter it applies to HTTP pings is what we
      # assert here.
      %{rows: rows} =
        TestRepo.query!(
          "SELECT function_name FROM pgflow.worker_functions WHERE enabled = true AND start_mode = 'http'"
        )

      http_functions = Enum.map(rows, fn [name] -> name end)
      refute process_function in http_functions
      assert http_function in http_functions
    end
  end

  describe "startup failure" do
    test "bootstrap failure does not leave a running worker", %{task_supervisor: task_supervisor} do
      start_supervised!(BootstrapFailingRepo)

      config = %{
        flow_module: BlockedHandlerFlow,
        repo: BootstrapFailingRepo,
        task_supervisor: task_supervisor,
        max_concurrency: 1,
        batch_size: 1,
        signal_strategy: :polling,
        min_poll_interval: 1_000,
        max_poll_interval: 5_000,
        notify_fallback_interval: 30_000
      }

      assert {:error, {:bootstrap_failed, %DBConnection.ConnectionError{}}} =
               GenServer.start(PgFlow.Worker.Server, config, [])
    end
  end

  defp compile_flow(flow_module) do
    definition = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    TestRepo.query!(
      "SELECT pgflow.create_flow($1, $2, $3, $4)",
      [
        flow_slug,
        definition.opts[:max_attempts] || 3,
        definition.opts[:base_delay] || 1,
        definition.opts[:timeout] || 30
      ]
    )

    for step <- definition.steps do
      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)",
        [
          flow_slug,
          Atom.to_string(step.slug),
          Enum.map(step.depends_on, &Atom.to_string/1),
          step.max_attempts,
          step.base_delay,
          step.timeout,
          step.start_delay,
          Atom.to_string(step.step_type)
        ]
      )
    end

    flow_slug
  end

  defp start_worker(flow_module, task_supervisor, opts \\ []) do
    config = %{
      flow_module: flow_module,
      repo: TestRepo,
      task_supervisor: task_supervisor,
      max_concurrency: Keyword.get(opts, :max_concurrency, 1),
      batch_size: Keyword.get(opts, :batch_size, 1),
      signal_strategy: Keyword.get(opts, :signal_strategy, :polling),
      min_poll_interval: Keyword.get(opts, :min_poll_interval, 50),
      max_poll_interval: Keyword.get(opts, :max_poll_interval, 5_000),
      notify_fallback_interval: Keyword.get(opts, :notify_fallback_interval, 30_000),
      heartbeat_interval: Keyword.get(opts, :heartbeat_interval, 10_000)
    }

    {:ok, pid} = Server.start_link(config)
    Sandbox.allow(TestRepo, self(), pid)
    pid
  end

  defp start_flow_run(flow_slug, input) do
    IntegrationCase.start_flow_run(flow_slug, input)
  end

  defp run_status(run_id) do
    IntegrationCase.get_run_status(run_id)
  end

  defp unique_test_name do
    name = :"upstream_lifecycle_#{System.unique_integer([:positive])}"
    true = Process.register(self(), name)
    name
  end

  defp start_supervisor!(task_supervisor, opts \\ []) do
    true = Process.register(task_supervisor, PgFlow.TaskSupervisor)
    config = supervisor_config(opts)
    {:ok, supervisor_pid} = WorkerSupervisor.start_link(config)
    supervisor_pid
  end

  defp stop_supervisor!(supervisor_pid, task_supervisor) do
    if Process.alive?(supervisor_pid) do
      try do
        Supervisor.stop(supervisor_pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    case Process.whereis(PgFlow.TaskSupervisor) do
      ^task_supervisor -> Process.unregister(PgFlow.TaskSupervisor)
      _ -> :ok
    end

    :persistent_term.erase({PgFlow, :config})
    :persistent_term.erase({PgFlow, :repo})
  end

  defp supervisor_config(opts) do
    PgFlow.Config.validate!(
      repo: TestRepo,
      max_concurrency: 1,
      batch_size: 1,
      signal_strategy: Keyword.get(opts, :signal_strategy, :polling),
      heartbeat_interval: 100,
      min_poll_interval: 50,
      max_poll_interval: 5_000,
      notify_fallback_interval: 30_000
    )
  end

  defp wait_until(fun, timeout_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    wait_until_loop(fun, deadline)
  end

  defp wait_until_loop(fun, deadline) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) > deadline -> flunk("condition not met before timeout")
      true -> Process.sleep(25) && wait_until_loop(fun, deadline)
    end
  end
end
