defmodule PgFlow.AwaitSignalsTest do
  @moduledoc """
  Worker integration tests for park/resume via `Context.await_signal/2`
  and `PgFlow.signal/3`. Copies compile/start/wait helpers from
  `server_test.exs` and drives the real worker (no mocks).
  """
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Queries.{Flows, Signals, Workers}
  alias PgFlow.TestRepo
  alias PgFlow.Worker.{Server, WaitingTaskRecovery}

  @moduletag timeout: 30_000
  @moduletag :integration

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")
    :persistent_term.put({PgFlow, :repo}, TestRepo)

    {:ok, task_supervisor} = Task.Supervisor.start_link()

    on_exit(fn ->
      :persistent_term.erase({PgFlow, :repo})

      try do
        if Process.alive?(task_supervisor), do: Supervisor.stop(task_supervisor)
      catch
        :exit, _ -> :ok
      end

      Sandbox.mode(TestRepo, :manual)
    end)

    {:ok, task_supervisor: task_supervisor}
  end

  defmodule ApprovalFlow do
    use PgFlow.Flow
    @flow slug: :await_approval_flow, max_attempts: 3, timeout: 30

    step :approval do
      fn input, ctx ->
        case PgFlow.Context.await_signal(ctx, wait_for: {1, :hour}, wait_timeout: 0) do
          {:ok, %{"decision" => "approved"}} -> Map.put(input, "charged", true)
          {:ok, _} -> raise "rejected"
          {:error, :timeout} -> raise "no decision"
        end
      end
    end
  end

  defmodule RetryAfterSignalFlow do
    use PgFlow.Flow
    @flow slug: :retry_after_signal_flow, max_attempts: 2, base_delay: 0, timeout: 30

    step :approval do
      fn input, ctx ->
        {:ok, payload} =
          PgFlow.Context.await_signal(ctx, wait_timeout: 0, wait_for: {1, :hour})

        if ctx.attempt == 1 do
          raise "fail after signal"
        else
          Map.merge(input, payload)
        end
      end
    end
  end

  defmodule TerminalFailMapFlow do
    use PgFlow.Flow
    @flow slug: :terminal_fail_map_flow, max_attempts: 1, base_delay: 0, timeout: 30

    map :approval, max_attempts: 1, when_exhausted: :fail do
      fn input, ctx -> PgFlow.AwaitSignalsTest.terminal_sibling_handler(input, ctx) end
    end
  end

  defmodule TerminalSkipMapFlow do
    use PgFlow.Flow
    @flow slug: :terminal_skip_map_flow, max_attempts: 1, base_delay: 0, timeout: 30

    map :approval, max_attempts: 1, when_exhausted: :skip do
      fn input, ctx -> PgFlow.AwaitSignalsTest.terminal_sibling_handler(input, ctx) end
    end
  end

  defmodule TerminalSkipCascadeMapFlow do
    use PgFlow.Flow
    @flow slug: :terminal_skip_cascade_map_flow, max_attempts: 1, base_delay: 0, timeout: 30

    map :approval, max_attempts: 1, when_exhausted: :skip_cascade do
      fn input, ctx -> PgFlow.AwaitSignalsTest.terminal_sibling_handler(input, ctx) end
    end
  end

  defmodule SignalWinsRaceFlow do
    use PgFlow.Flow
    @flow slug: :signal_wins_race_flow, max_attempts: 1, timeout: 30

    step :approval do
      fn input, _ctx -> input end
    end
  end

  defmodule TerminalWinsRaceFlow do
    use PgFlow.Flow
    @flow slug: :terminal_wins_race_flow, max_attempts: 1, timeout: 30

    step :root do
      fn input, _ctx -> input end
    end

    step :approval, depends_on: [:root] do
      fn input, _ctx -> input end
    end
  end

  defp compile_flow(flow_module) do
    definition = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    Enum.each(PgFlow.FlowCompiler.compile(definition), &TestRepo.query!/1)

    flow_slug
  end

  def terminal_sibling_handler(%{"action" => "wait"}, ctx) do
    {:ok, payload} = PgFlow.Context.await_signal(ctx, wait_timeout: 0, wait_for: {1, :hour})
    payload
  end

  def terminal_sibling_handler(%{"action" => "fail"}, ctx) do
    assert_task_zero_waiting(ctx.run_id)
    raise "terminal sibling exhausted"
  end

  defp assert_task_zero_waiting(run_id) do
    case wait_until(fn ->
           match?(%{status: "waiting"}, get_task_details(run_id, "approval", 0))
         end) do
      :ok -> :ok
      {:error, :timeout} -> raise "task index 0 did not enter waiting before sibling failure"
    end
  end

  defp start_worker(flow_module, task_supervisor, opts \\ []) do
    config = %{
      flow_module: flow_module,
      repo: TestRepo,
      task_supervisor: task_supervisor,
      max_concurrency: Keyword.get(opts, :max_concurrency, 10),
      batch_size: Keyword.get(opts, :batch_size, 10),
      signal_strategy: Keyword.get(opts, :signal_strategy, :polling),
      min_poll_interval: Keyword.get(opts, :min_poll_interval, 50),
      max_poll_interval: Keyword.get(opts, :max_poll_interval, 5_000),
      notify_fallback_interval: Keyword.get(opts, :notify_fallback_interval, 30_000)
    }

    {:ok, pid} = Server.start_link(config)
    Sandbox.allow(TestRepo, self(), pid)
    pid
  end

  defp start_flow_run(flow_slug, input) do
    %{rows: [[result]]} =
      TestRepo.query!(
        "SELECT pgflow.start_flow($1, cast($2 as text)::jsonb)",
        [flow_slug, Jason.encode!(input)]
      )

    case result do
      {run_id, _, _, _, _, _, _, _, _} -> Ecto.UUID.load!(run_id)
      _ -> raise "Unexpected result: #{inspect(result)}"
    end
  end

  defp get_run_status(run_id) do
    %{rows: [[status]]} =
      TestRepo.query!("SELECT status FROM pgflow.runs WHERE run_id = $1", [
        Ecto.UUID.dump!(run_id)
      ])

    status
  end

  defp get_task_details(run_id, step_slug, task_index) do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT status, attempts_count, error_message, message_id
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
        """,
        [Ecto.UUID.dump!(run_id), step_slug, task_index]
      )

    case rows do
      [[status, attempts, error, message_id]] ->
        %{
          status: status,
          attempts_count: attempts,
          error_message: error,
          message_id: message_id
        }

      [] ->
        nil
    end
  end

  defp wait_until(condition_fn, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 5_000)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, 50)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(condition_fn, deadline, poll_interval_ms)
  end

  defp do_wait_until(condition_fn, deadline, poll_interval_ms) do
    if condition_fn.() do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        {:error, :timeout}
      else
        Process.sleep(poll_interval_ms)
        do_wait_until(condition_fn, deadline, poll_interval_ms)
      end
    end
  end

  defp wait_for_run_completion(run_id, timeout_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    wait_loop(run_id, deadline)
  end

  defp start_and_park_task(flow_module) do
    flow_slug = compile_flow(flow_module)
    run_id = start_flow_run(flow_slug, %{})
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
    {:ok, messages} = Flows.read(TestRepo, flow_slug, 30, 1)
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    {:ok, _} = Flows.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)
    task = get_task_details(run_id, "approval", 0)

    assert :parked =
             Signals.await_task_signal(
               TestRepo,
               run_id,
               "approval",
               0,
               task.attempts_count,
               task.message_id,
               nil,
               true
             )

    {flow_slug, run_id}
  end

  defp start_started_task(flow_module) do
    flow_slug = compile_flow(flow_module)
    run_id = start_flow_run(flow_slug, %{})
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
    {:ok, messages} = Flows.read(TestRepo, flow_slug, 30, 1)
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    {:ok, _} = Flows.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)
    {flow_slug, run_id, get_task_details(run_id, "approval", 0)}
  end

  defp independent_connection do
    opts =
      TestRepo.config()
      |> Keyword.take([:hostname, :port, :username, :password, :database, :socket_dir, :ssl])

    {:ok, connection} = Postgrex.start_link(opts)
    connection
  end

  defp signal_in_open_transaction(parent, run_id, decision) do
    connection = independent_connection()

    result =
      Postgrex.transaction(connection, fn connection ->
        %{rows: [[backend_pid]]} = Postgrex.query!(connection, "SELECT pg_backend_pid()", [])
        send(parent, {:signal_query_started, self(), backend_pid})

        %{rows: [[outcome]]} =
          Postgrex.query!(
            connection,
            "SELECT outcome FROM pgflow.signal_task($1, $2, $3, cast($4 as text)::jsonb)",
            [Ecto.UUID.dump!(run_id), "approval", 0, Jason.encode!(%{"decision" => decision})]
          )

        send(parent, {:signal_query_finished, self(), outcome})

        receive do
          :commit -> outcome
        end
      end)

    GenServer.stop(connection)
    result
  end

  defp fail_run_in_open_transaction(parent, run_id) do
    connection = independent_connection()

    result =
      Postgrex.transaction(connection, fn connection ->
        %{rows: [[backend_pid]]} = Postgrex.query!(connection, "SELECT pg_backend_pid()", [])
        send(parent, {:terminal_query_started, self(), backend_pid})

        Postgrex.query!(
          connection,
          "UPDATE pgflow.runs SET status = 'failed', failed_at = now() WHERE run_id = $1",
          [Ecto.UUID.dump!(run_id)]
        )

        send(parent, {:terminal_query_finished, self()})

        receive do
          :commit -> :failed
        end
      end)

    GenServer.stop(connection)
    result
  end

  defp await_in_open_transaction(parent, run_id, task) do
    connection = independent_connection()

    result =
      Postgrex.transaction(connection, fn connection ->
        %{rows: [[backend_pid]]} = Postgrex.query!(connection, "SELECT pg_backend_pid()", [])
        send(parent, {:await_query_started, self(), backend_pid})

        %{rows: [[outcome, payload]]} =
          Postgrex.query!(
            connection,
            """
            SELECT outcome, payload
            FROM pgflow.await_task_signal($1, 'approval', 0, $2, $3, NULL, true)
            """,
            [Ecto.UUID.dump!(run_id), task.attempts_count, task.message_id]
          )

        send(parent, {:await_query_finished, self(), outcome, payload})

        receive do
          :commit -> {outcome, payload}
        end
      end)

    GenServer.stop(connection)
    result
  end

  defp expire_once(parent) do
    connection = independent_connection()

    %{rows: [[backend_pid]]} = Postgrex.query!(connection, "SELECT pg_backend_pid()", [])
    send(parent, {:expire_query_started, self(), backend_pid})

    %{rows: [[count]]} =
      Postgrex.query!(connection, "SELECT pgflow.expire_waiting_tasks(100)", [])

    send(parent, {:expire_query_finished, self(), count})
    GenServer.stop(connection)
    count
  end

  defp backend_blocked?(backend_pid) do
    %{rows: [[blocked?]]} =
      TestRepo.query!("SELECT cardinality(pg_blocking_pids($1)) > 0", [backend_pid])

    blocked?
  end

  defp wait_loop(run_id, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :timeout}
    else
      case get_run_status(run_id) do
        status when status in ["completed", "failed"] ->
          {:ok, status}

        _ ->
          Process.sleep(50)
          wait_loop(run_id, deadline)
      end
    end
  end

  test "parks then resumes when signalled", %{task_supervisor: task_supervisor} do
    :telemetry_test.attach_event_handlers(self(), [
      [:pgflow, :worker, :task, :exception],
      [:pgflow, :worker, :task, :waiting]
    ])

    compile_flow(ApprovalFlow)
    start_worker(ApprovalFlow, task_supervisor)
    run_id = start_flow_run("await_approval_flow", %{"order_id" => 1})

    assert :ok =
             wait_until(fn ->
               case get_task_details(run_id, "approval", 0) do
                 %{status: "waiting"} -> true
                 _ -> false
               end
             end)

    task = get_task_details(run_id, "approval", 0)
    assert task.status == "waiting"
    assert is_nil(task.error_message)
    assert task.attempts_count == 0
    assert get_run_status(run_id) == "started"

    assert_received {[:pgflow, :worker, :task, :waiting], _, _,
                     %{step_slug: "approval", run_id: ^run_id}}

    refute_received {[:pgflow, :worker, :task, :exception], _, _, _}

    assert {:ok, :requeued} =
             PgFlow.signal(run_id, :approval, %{"decision" => "approved"})

    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "completed"
  end

  test "a claimed signal is replayed after a normal handler retry", %{
    task_supervisor: task_supervisor
  } do
    compile_flow(RetryAfterSignalFlow)
    start_worker(RetryAfterSignalFlow, task_supervisor)
    run_id = start_flow_run("retry_after_signal_flow", %{"order_id" => 1})

    assert :ok =
             wait_until(fn ->
               match?(%{status: "waiting"}, get_task_details(run_id, "approval", 0))
             end)

    assert {:ok, :requeued} =
             Signals.signal_task(TestRepo, run_id, "approval", 0, %{
               "decision" => "approved"
             })

    assert {:ok, "completed"} = wait_for_run_completion(run_id)
    assert get_task_details(run_id, "approval", 0).attempts_count == 2
  end

  for {flow_module, flow_slug, expected_status} <- [
        {TerminalFailMapFlow, "terminal_fail_map_flow", "failed"},
        {TerminalSkipMapFlow, "terminal_skip_map_flow", "completed"},
        {TerminalSkipCascadeMapFlow, "terminal_skip_cascade_map_flow", "completed"}
      ] do
    @flow_module flow_module
    @flow_slug flow_slug
    @expected_status expected_status

    test "terminal #{flow_slug} settles a waiting sibling and rejects its old signal address", %{
      task_supervisor: task_supervisor
    } do
      compile_flow(@flow_module)
      start_worker(@flow_module, task_supervisor)

      run_id =
        start_flow_run(@flow_slug, [
          %{"action" => "wait"},
          %{"action" => "fail"}
        ])

      assert {:ok, @expected_status} = wait_for_run_completion(run_id)

      assert %{status: "failed", message_id: nil} =
               get_task_details(run_id, "approval", 0)

      assert %{rows: [[0]]} =
               TestRepo.query!(
                 "SELECT count(*) FROM pgflow.task_signals WHERE run_id = $1",
                 [Ecto.UUID.dump!(run_id)]
               )

      queued_before_signal = queued_message_count(@flow_slug)
      task_messages_before_signal = task_message_count(@flow_slug, run_id, "approval", 0)
      assert task_messages_before_signal == 0

      assert {:ok, :terminal} =
               Signals.signal_task(TestRepo, run_id, "approval", 0, %{
                 "decision" => "late"
               })

      assert queued_message_count(@flow_slug) == queued_before_signal

      assert task_message_count(@flow_slug, run_id, "approval", 0) ==
               task_messages_before_signal

      assert %{rows: [[0]]} =
               TestRepo.query!(
                 "SELECT count(*) FROM pgflow.task_signals WHERE run_id = $1",
                 [Ecto.UUID.dump!(run_id)]
               )
    end
  end

  test "terminal cleanup wins after a concurrent signal requeues", %{} do
    {flow_slug, run_id} = start_and_park_task(SignalWinsRaceFlow)
    parent = self()
    signal = Task.async(fn -> signal_in_open_transaction(parent, run_id, "approved") end)

    assert_receive {:signal_query_started, signal_pid, _signal_backend_pid}, 5_000
    assert_receive {:signal_query_finished, ^signal_pid, "requeued"}, 5_000
    terminal = Task.async(fn -> fail_run_in_open_transaction(parent, run_id) end)
    assert_receive {:terminal_query_started, terminal_pid, terminal_backend_pid}, 5_000
    assert :ok = wait_until(fn -> backend_blocked?(terminal_backend_pid) end)

    send(signal_pid, :commit)
    assert {:ok, "requeued"} = Task.await(signal, 5_000)
    assert_receive {:terminal_query_finished, ^terminal_pid}, 5_000
    send(terminal_pid, :commit)
    assert {:ok, :failed} = Task.await(terminal, 5_000)

    assert %{status: "failed", message_id: nil} = get_task_details(run_id, "approval", 0)
    assert task_message_count(flow_slug, run_id, "approval", 0) == 0
    assert signal_count(run_id) == 0
  end

  test "a signal blocked behind terminal cleanup cannot buffer or requeue", %{} do
    flow_slug = compile_flow(TerminalWinsRaceFlow)
    run_id = start_flow_run(flow_slug, %{})
    assert get_task_details(run_id, "approval", 0) == nil
    parent = self()
    terminal = Task.async(fn -> fail_run_in_open_transaction(parent, run_id) end)
    assert_receive {:terminal_query_started, terminal_pid, _terminal_backend_pid}, 5_000
    assert_receive {:terminal_query_finished, ^terminal_pid}, 5_000

    signal = Task.async(fn -> signal_in_open_transaction(parent, run_id, "late") end)
    assert_receive {:signal_query_started, signal_pid, signal_backend_pid}, 5_000
    assert :ok = wait_until(fn -> backend_blocked?(signal_backend_pid) end)

    send(terminal_pid, :commit)
    assert {:ok, :failed} = Task.await(terminal, 5_000)
    assert_receive {:signal_query_finished, ^signal_pid, "terminal"}, 5_000
    send(signal_pid, :commit)
    assert {:ok, "terminal"} = Task.await(signal, 5_000)

    assert %{status: "failed", message_id: nil} = get_task_details(run_id, "root", 0)
    assert get_task_details(run_id, "approval", 0) == nil
    assert task_message_count(flow_slug, run_id, "root", 0) == 0
    assert signal_count(run_id) == 0
  end

  test "an await blocked behind a terminal run cannot return a buffered payload or park", %{} do
    {flow_slug, run_id, task} = start_started_task(SignalWinsRaceFlow)

    assert {:ok, :buffered} =
             Signals.signal_task(TestRepo, run_id, "approval", 0, %{"decision" => "approved"})

    parent = self()
    terminal = Task.async(fn -> fail_run_in_open_transaction(parent, run_id) end)
    assert_receive {:terminal_query_started, terminal_pid, _terminal_backend_pid}, 5_000
    assert_receive {:terminal_query_finished, ^terminal_pid}, 5_000

    awaiter = Task.async(fn -> await_in_open_transaction(parent, run_id, task) end)
    assert_receive {:await_query_started, await_pid, await_backend_pid}, 5_000
    assert :ok = wait_until(fn -> backend_blocked?(await_backend_pid) end)

    send(terminal_pid, :commit)
    assert {:ok, :failed} = Task.await(terminal, 5_000)
    assert_receive {:await_query_finished, ^await_pid, "terminal", nil}, 5_000
    send(await_pid, :commit)
    assert {:ok, {"terminal", nil}} = Task.await(awaiter, 5_000)

    assert get_run_status(run_id) == "failed"

    assert %{status: "failed", message_id: nil, error_message: "abandoned: run became failed"} =
             get_task_details(run_id, "approval", 0)

    assert task_message_count(flow_slug, run_id, "approval", 0) == 0
    assert signal_count(run_id) == 0
  end

  test "terminal cleanup locked first makes expiry skip without counting or requeueing", %{} do
    {flow_slug, run_id} = start_and_park_task(SignalWinsRaceFlow)

    TestRepo.query!(
      """
      UPDATE pgflow.task_signals
      SET wait_deadline_at = now() - interval '1 second'
      WHERE run_id = $1 AND step_slug = 'approval' AND task_index = 0
      """,
      [Ecto.UUID.dump!(run_id)]
    )

    parent = self()
    terminal = Task.async(fn -> fail_run_in_open_transaction(parent, run_id) end)
    assert_receive {:terminal_query_started, terminal_pid, _terminal_backend_pid}, 5_000
    assert_receive {:terminal_query_finished, ^terminal_pid}, 5_000

    sweeper = Task.async(fn -> expire_once(parent) end)
    assert_receive {:expire_query_started, sweeper_pid, _sweeper_backend_pid}, 5_000
    assert_receive {:expire_query_finished, ^sweeper_pid, 0}, 5_000
    assert Task.await(sweeper, 5_000) == 0

    send(terminal_pid, :commit)
    assert {:ok, :failed} = Task.await(terminal, 5_000)
    assert get_run_status(run_id) == "failed"
    assert %{status: "failed", message_id: nil} = get_task_details(run_id, "approval", 0)
    assert task_message_count(flow_slug, run_id, "approval", 0) == 0
    assert signal_count(run_id) == 0
  end

  defmodule TimeoutFlow do
    use PgFlow.Flow
    @flow slug: :await_timeout_flow, max_attempts: 1, timeout: 30

    step :gate do
      fn _input, ctx ->
        case PgFlow.Context.await_signal(ctx, wait_for: 1, wait_timeout: 0) do
          {:ok, _} -> %{"ok" => true}
          {:error, :timeout} -> raise "no decision"
        end
      end
    end
  end

  test "wait_for deadline yields {:error, :timeout} and handler can fail the run",
       %{task_supervisor: task_supervisor} do
    compile_flow(TimeoutFlow)
    start_worker(TimeoutFlow, task_supervisor)
    run_id = start_flow_run("await_timeout_flow", %{})

    assert :ok =
             wait_until(fn ->
               match?(%{status: "waiting"}, get_task_details(run_id, "gate", 0))
             end)

    TestRepo.query!(
      """
      UPDATE pgflow.task_signals
      SET wait_deadline_at = now() - interval '1 second'
      WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
      """,
      [Ecto.UUID.dump!(run_id), "gate", 0]
    )

    assert {:ok, 1} = Signals.expire_waiting_tasks(TestRepo, 100)

    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "failed"
  end

  test "stalled recovery ignores waiting tasks", %{task_supervisor: task_supervisor} do
    compile_flow(ApprovalFlow)
    start_worker(ApprovalFlow, task_supervisor)
    run_id = start_flow_run("await_approval_flow", %{"order_id" => 1})

    assert :ok =
             wait_until(fn ->
               match?(%{status: "waiting"}, get_task_details(run_id, "approval", 0))
             end)

    assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 0)
    assert get_task_details(run_id, "approval", 0).status == "waiting"
  end

  test "WaitingTaskRecovery expires waiting tasks on :recover" do
    compile_flow(TimeoutFlow)
    run_id = start_flow_run("await_timeout_flow", %{})

    {:ok, pid} =
      WaitingTaskRecovery.start_link(
        repo: TestRepo,
        waiting_recovery_interval: 60_000,
        waiting_recovery_batch_size: 1
      )

    # Park via SQL wrappers (no worker) so the sweeper is what requeues.
    worker_id = Ecto.UUID.generate()

    {:ok, _} =
      Workers.register_worker(
        TestRepo,
        worker_id,
        "await_timeout_flow",
        "elixir:test"
      )

    {:ok, messages} = Flows.read(TestRepo, "await_timeout_flow", 30, 10)
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    {:ok, _} = Flows.start_tasks(TestRepo, "await_timeout_flow", msg_ids, worker_id)

    task = get_task_details(run_id, "gate", 0)

    assert :parked =
             Signals.await_task_signal(
               TestRepo,
               run_id,
               "gate",
               0,
               task.attempts_count,
               task.message_id,
               60,
               true
             )

    TestRepo.query!(
      """
      UPDATE pgflow.task_signals
      SET wait_deadline_at = now() - interval '1 second'
      WHERE run_id = $1 AND step_slug = 'gate' AND task_index = 0
      """,
      [Ecto.UUID.dump!(run_id)]
    )

    send(pid, :recover)
    _ = :sys.get_state(pid)

    {:ok, resumed_messages} = Flows.read(TestRepo, "await_timeout_flow", 30, 10)
    resumed_message_ids = Enum.map(resumed_messages, fn [message_id | _] -> message_id end)
    {:ok, _} = Flows.start_tasks(TestRepo, "await_timeout_flow", resumed_message_ids, worker_id)
    resumed_task = get_task_details(run_id, "gate", 0)

    assert :timeout =
             Signals.await_task_signal(
               TestRepo,
               run_id,
               "gate",
               0,
               resumed_task.attempts_count,
               resumed_task.message_id,
               nil,
               false
             )

    GenServer.stop(pid)
  end

  defmodule ApprovalJob do
    use PgFlow.Job
    @job queue: :await_approval_job, max_attempts: 2, timeout: 30

    perform :approve do
      fn _input, ctx ->
        case PgFlow.Context.await_signal(ctx, wait_timeout: 0, wait_for: {1, :hour}) do
          {:ok, %{"decision" => "approved"}} -> %{"done" => true}
          other -> raise "unexpected #{inspect(other)}"
        end
      end
    end
  end

  test "job parks and resumes", %{task_supervisor: task_supervisor} do
    compile_flow(ApprovalJob)
    start_worker(ApprovalJob, task_supervisor)
    run_id = start_flow_run("await_approval_job", %{})

    assert :ok =
             wait_until(fn ->
               match?(%{status: "waiting"}, get_task_details(run_id, "approve", 0))
             end)

    assert {:ok, :requeued} =
             PgFlow.signal(run_id, :approve, %{"decision" => "approved"})

    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "completed"
  end

  test "early signal before handler runs", %{task_supervisor: task_supervisor} do
    compile_flow(ApprovalFlow)
    run_id = start_flow_run("await_approval_flow", %{"order_id" => 1})

    assert {:ok, :buffered} =
             PgFlow.signal(run_id, :approval, %{"decision" => "approved"})

    start_worker(ApprovalFlow, task_supervisor)
    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "completed"
  end

  test "last write wins", %{task_supervisor: task_supervisor} do
    compile_flow(ApprovalFlow)
    run_id = start_flow_run("await_approval_flow", %{"order_id" => 1})

    assert {:ok, :buffered} =
             PgFlow.signal(run_id, :approval, %{"decision" => "rejected"})

    assert {:ok, :buffered} =
             PgFlow.signal(run_id, :approval, %{"decision" => "approved"})

    start_worker(ApprovalFlow, task_supervisor)
    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "completed"
  end

  defp queued_message_count(queue_name) do
    %{rows: [[count]]} = TestRepo.query!("SELECT count(*) FROM pgmq.q_#{queue_name}")
    count
  end

  defp task_message_count(queue_name, run_id, step_slug, task_index) do
    %{rows: [[count]]} =
      TestRepo.query!(
        """
        SELECT count(*)
        FROM pgmq.q_#{queue_name}
        WHERE message->>'run_id' = $1
          AND message->>'step_slug' = $2
          AND (message->>'task_index')::integer = $3
        """,
        [run_id, step_slug, task_index]
      )

    count
  end

  defp signal_count(run_id) do
    %{rows: [[count]]} =
      TestRepo.query!("SELECT count(*) FROM pgflow.task_signals WHERE run_id = $1", [
        Ecto.UUID.dump!(run_id)
      ])

    count
  end
end
