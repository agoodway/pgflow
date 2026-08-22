defmodule PgFlow.AwaitSignalsTest do
  @moduledoc """
  Worker integration tests for park/resume via `Context.await_signal/2`
  and `PgFlow.signal/3`. Copies compile/start/wait helpers from
  `server_test.exs` and drives the real worker (no mocks).
  """
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Queries.Flows
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

  defp compile_flow(flow_module) do
    definition = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    TestRepo.query!("SELECT pgflow.create_flow($1, $2, $3, $4)", [
      flow_slug,
      definition.opts[:max_attempts] || 3,
      definition.opts[:base_delay] || 1,
      definition.opts[:timeout] || 30
    ])

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
        SELECT status, attempts_count, error_message
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
        """,
        [Ecto.UUID.dump!(run_id), step_slug, task_index]
      )

    case rows do
      [[status, attempts, error]] ->
        %{status: status, attempts_count: attempts, error_message: error}

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

    assert :ok = PgFlow.signal(run_id, :approval, %{"decision" => "approved"})

    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "completed"
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
      "UPDATE pgflow.task_signals SET wait_deadline_at = now() - interval '1 second'"
    )

    assert {:ok, _} = PgFlow.Queries.Signals.expire_waiting_tasks(TestRepo)

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
        waiting_recovery_interval: 60_000
      )

    # Park via SQL wrappers (no worker) so the sweeper is what requeues.
    worker_id = Ecto.UUID.generate()

    {:ok, _} =
      PgFlow.Queries.Workers.register_worker(
        TestRepo,
        worker_id,
        "await_timeout_flow",
        "elixir:test"
      )

    {:ok, messages} = Flows.read(TestRepo, "await_timeout_flow", 30, 10)
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    {:ok, _} = Flows.start_tasks(TestRepo, "await_timeout_flow", msg_ids, worker_id)

    deadline = DateTime.add(DateTime.utc_now(), -60, :second)
    assert :ok = PgFlow.Queries.Signals.park_waiting_task(TestRepo, run_id, "gate", 0, deadline)

    send(pid, :recover)
    _ = :sys.get_state(pid)

    assert {:error, :timeout} =
             PgFlow.Queries.Signals.consume_task_signal(TestRepo, run_id, "gate", 0)

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

    assert :ok = PgFlow.signal(run_id, :approve, %{"decision" => "approved"})
    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "completed"
  end

  test "early signal before handler runs", %{task_supervisor: task_supervisor} do
    compile_flow(ApprovalFlow)
    run_id = start_flow_run("await_approval_flow", %{"order_id" => 1})
    assert :ok = PgFlow.signal(run_id, :approval, %{"decision" => "approved"})
    start_worker(ApprovalFlow, task_supervisor)
    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "completed"
  end

  test "last write wins", %{task_supervisor: task_supervisor} do
    compile_flow(ApprovalFlow)
    run_id = start_flow_run("await_approval_flow", %{"order_id" => 1})
    assert :ok = PgFlow.signal(run_id, :approval, %{"decision" => "rejected"})
    assert :ok = PgFlow.signal(run_id, :approval, %{"decision" => "approved"})
    start_worker(ApprovalFlow, task_supervisor)
    {:ok, status} = wait_for_run_completion(run_id)
    assert status == "completed"
  end
end
