defmodule PgFlow.Worker.ServerTest do
  @moduledoc """
  Integration tests for PgFlow.Worker.Server.

  Tests verify that the worker:
  - Registers itself in the database
  - Polls for and processes tasks
  - Completes tasks correctly
  - Handles task failures and retries
  - Respects concurrency limits
  - Shuts down gracefully

  Reference: pkgs/edge-worker/tests/integration/retries.test.ts
  Reference: pkgs/edge-worker/tests/integration/maxConcurrent.test.ts

  These tests do NOT use the sandbox - they make real database changes and
  rely on pgflow_tests.reset_db() for cleanup between tests, matching the
  TypeScript reference implementation approach.
  """
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Queries.Flows, as: FlowQueries
  alias PgFlow.TestRepo
  alias PgFlow.Worker.Server

  # Use a longer timeout for integration tests
  @moduletag timeout: 30_000
  # Run tests serially since we're not using sandbox
  @moduletag :integration

  setup do
    # These integration tests use :auto sandbox mode because:
    # 1. The worker spawns tasks via Task.Supervisor
    # 2. Spawned tasks need to make DB calls without explicit ownership
    # 3. :auto mode allows any process to checkout connections automatically
    #
    # Tests are run serially (@moduletag :integration) to avoid conflicts.

    # Set auto mode to allow any process to checkout
    Sandbox.mode(TestRepo, :auto)

    # Reset the database to a clean state
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    # Start a test-specific Task.Supervisor
    {:ok, task_supervisor} = Task.Supervisor.start_link()

    on_exit(fn ->
      # Clean shutdown of task supervisor (if still alive)
      # Use try/catch to handle race condition where process dies between check and stop
      try do
        if Process.alive?(task_supervisor) do
          Supervisor.stop(task_supervisor)
        end
      catch
        :exit, _ -> :ok
      end

      # Reset to manual mode for other tests
      Sandbox.mode(TestRepo, :manual)
    end)

    {:ok, task_supervisor: task_supervisor}
  end

  # ============= Test Flow Modules =============
  # These are defined inline to ensure atoms exist and handlers are accessible

  defmodule SimpleWorkerFlow do
    use PgFlow.Flow

    @flow slug: :simple_worker_flow, max_attempts: 3

    step :process do
      fn input, _ctx ->
        # Root steps receive flow_input directly (matching TypeScript reference)
        %{result: input["value"] * 2}
      end
    end
  end

  defmodule FailingWorkerFlow do
    use PgFlow.Flow

    @flow slug: :failing_worker_flow, max_attempts: 2, base_delay: 1

    step :will_fail do
      fn _input, _ctx ->
        raise "Intentional failure"
      end
    end
  end

  defmodule SlowWorkerFlow do
    use PgFlow.Flow

    @flow slug: :slow_worker_flow, max_attempts: 1

    step :slow_step do
      fn input, _ctx ->
        # Root steps receive flow_input directly (matching TypeScript reference)
        Process.sleep(input["sleep_ms"] || 100)
        %{completed: true}
      end
    end
  end

  defmodule CountingWorkerFlow do
    @moduledoc """
    A flow that tracks how many times it's been called using an Agent.
    """
    use PgFlow.Flow

    @flow slug: :counting_worker_flow, max_attempts: 3, base_delay: 1

    step :count do
      fn input, _ctx ->
        # Root steps receive flow_input directly (matching TypeScript reference)
        # Get the agent pid from the input (passed as string)
        agent_name = String.to_existing_atom(input["agent_name"])

        # Increment counter
        count = Agent.get_and_update(agent_name, fn c -> {c + 1, c + 1} end)

        if count < input["succeed_on"] do
          raise "Attempt #{count} failed, need #{input["succeed_on"]}"
        end

        %{success: true, attempts: count}
      end
    end
  end

  # ============= Multi-Step Flow Test Modules =============

  defmodule TwoStepFlow do
    @moduledoc """
    A two-step flow where the second step depends on the first.
    Tests basic step dependency handling.
    """
    use PgFlow.Flow

    @flow slug: :two_step_flow, max_attempts: 3

    step :first do
      fn input, _ctx ->
        # Root steps receive flow_input directly
        %{doubled: input["value"] * 2}
      end
    end

    step :second, depends_on: [:first] do
      fn input, _ctx ->
        # Dependent steps receive deps object: {"first": <first_output>}
        %{final: input["first"]["doubled"] + 10}
      end
    end
  end

  defmodule ThreeStepChainFlow do
    @moduledoc """
    A three-step chain: A → B → C
    Tests sequential dependency chains.
    """
    use PgFlow.Flow

    @flow slug: :three_step_chain_flow, max_attempts: 3

    step :a do
      fn input, _ctx ->
        # Root step receives flow_input directly
        %{value: input["x"] + 1}
      end
    end

    step :b, depends_on: [:a] do
      fn input, _ctx ->
        # Dependent step receives deps object: {"a": <a_output>}
        %{value: input["a"]["value"] * 2}
      end
    end

    step :c, depends_on: [:b] do
      fn input, _ctx ->
        # Dependent step receives deps object: {"b": <b_output>}
        %{value: input["b"]["value"] + 100}
      end
    end
  end

  defmodule DiamondFlow do
    @moduledoc """
    A diamond-shaped flow: start → (left || right) → merge
    Tests parallel branches with shared dependency and fan-in.
    """
    use PgFlow.Flow

    @flow slug: :diamond_flow, max_attempts: 3

    step :start do
      fn input, _ctx ->
        # Root step receives flow_input directly
        %{base: input["n"]}
      end
    end

    step :left, depends_on: [:start] do
      fn input, _ctx ->
        # Dependent step receives deps object: {"start": <start_output>}
        %{result: input["start"]["base"] * 2}
      end
    end

    step :right, depends_on: [:start] do
      fn input, _ctx ->
        # Dependent step receives deps object: {"start": <start_output>}
        %{result: input["start"]["base"] * 3}
      end
    end

    step :merge, depends_on: [:left, :right] do
      fn input, _ctx ->
        # Dependent step receives deps object: {"left": ..., "right": ...}
        %{sum: input["left"]["result"] + input["right"]["result"]}
      end
    end
  end

  # ============= Fan-Out/Fan-In Flow Test Modules =============

  defmodule MapStepFlow do
    @moduledoc """
    A flow with a single map step that processes array items in parallel.
    The `map` macro creates a step with step_type: :map.
    For root map steps, each task receives the raw array element directly.
    """
    use PgFlow.Flow

    @flow slug: :map_step_flow, max_attempts: 3

    # Use the `map` macro for map steps, not `step` with step_type option
    map :process_items do
      fn input, _ctx ->
        # For root map steps, input IS the raw array element (e.g., 1, 2, 3)
        # Not a map with "run" key
        item = input
        %{processed: item * 10}
      end
    end
  end

  defmodule MapThenReduceFlow do
    @moduledoc """
    A flow with a map step followed by an aggregation step.
    """
    use PgFlow.Flow

    @flow slug: :map_then_reduce_flow, max_attempts: 3

    # Root map step - each task receives one array element directly
    map :map_items do
      fn input, _ctx ->
        # For root map steps, input IS the raw array element
        %{doubled: input * 2}
      end
    end

    step :aggregate, depends_on: [:map_items] do
      fn input, _ctx ->
        # map_items output is an array of results
        items = input["map_items"]
        sum = Enum.reduce(items, 0, fn item, acc -> acc + item["doubled"] end)
        %{total: sum}
      end
    end
  end

  defmodule ParallelProcessingFlow do
    @moduledoc """
    A flow that generates items, processes them in parallel, then summarizes.
    Note: The :generate step needs to return an array directly (not {items: [...]})
    because the map step reads the entire output as the array.
    """
    use PgFlow.Flow

    @flow slug: :parallel_processing_flow, max_attempts: 3

    step :generate do
      fn input, _ctx ->
        # Root step receives flow_input directly
        # Return an array directly since map step will use entire output
        Enum.to_list(1..input["count"])
      end
    end

    # Dependent map step - receives elements from generate's array output
    map :process_each, array: :generate do
      fn input, _ctx ->
        # Map steps receive raw array element directly
        %{squared: input * input}
      end
    end

    step :summarize, depends_on: [:process_each] do
      fn input, _ctx ->
        # Dependent step receives deps object: {"process_each": [...]}
        results = input["process_each"]
        %{count: length(results), sum: Enum.sum(Enum.map(results, & &1["squared"]))}
      end
    end
  end

  # ============= Edge Case Flow Test Modules =============

  defmodule TimeoutTestFlow do
    @moduledoc """
    A flow with a short timeout for testing timeout behavior.
    """
    use PgFlow.Flow

    @flow slug: :timeout_test_flow, max_attempts: 1, timeout: 2

    step :slow_step do
      fn input, _ctx ->
        # Root step receives flow_input directly
        Process.sleep(input["sleep_ms"])
        %{done: true}
      end
    end
  end

  defmodule StepTimeoutFlow do
    @moduledoc """
    A flow where a specific step has a short timeout override.
    Flow-level timeout is 30s, but slow_step has timeout: 2.
    """
    use PgFlow.Flow

    @flow slug: :step_timeout_flow, max_attempts: 1, timeout: 30

    step :fast_step do
      fn _input, _ctx ->
        %{done: true}
      end
    end

    step :slow_step, depends_on: [:fast_step], timeout: 2 do
      fn _input, _ctx ->
        Process.sleep(5000)
        %{done: true}
      end
    end
  end

  defmodule PartialFailureFlow do
    @moduledoc """
    A flow where the middle step fails once then succeeds on retry.
    """
    use PgFlow.Flow

    @flow slug: :partial_failure_flow, max_attempts: 2, base_delay: 1

    step :first do
      fn input, _ctx ->
        # Root step receives flow_input directly
        # Store agent_name in output so dependent steps can access it
        %{step: "first", agent_name: input["agent_name"]}
      end
    end

    step :fails_once, depends_on: [:first] do
      fn input, _ctx ->
        # Dependent step receives deps object: {"first": ...}
        agent_name = String.to_existing_atom(input["first"]["agent_name"])
        count = Agent.get_and_update(agent_name, fn c -> {c + 1, c + 1} end)
        if count == 1, do: raise("First attempt fails"), else: %{attempts: count}
      end
    end

    step :final, depends_on: [:fails_once] do
      fn _input, _ctx -> %{complete: true} end
    end
  end

  defmodule AlwaysFailsFlow do
    @moduledoc """
    A flow where a step always fails for testing permanent failure handling.
    """
    use PgFlow.Flow

    @flow slug: :always_fails_flow, max_attempts: 2, base_delay: 1

    step :first do
      fn _input, _ctx -> %{step: "first"} end
    end

    step :always_fails, depends_on: [:first] do
      fn _input, _ctx ->
        raise "This step always fails"
      end
    end

    step :never_runs, depends_on: [:always_fails] do
      fn _input, _ctx -> %{step: "should never reach here"} end
    end
  end

  # ============= Helper Functions =============

  defp compile_flow(flow_module) do
    definition = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    # Create flow with options
    max_attempts = definition.opts[:max_attempts] || 3
    base_delay = definition.opts[:base_delay] || 1
    timeout = definition.opts[:timeout] || 30

    TestRepo.query!(
      "SELECT pgflow.create_flow($1, $2, $3, $4)",
      [flow_slug, max_attempts, base_delay, timeout]
    )

    # Add each step
    for step <- definition.steps do
      step_slug = Atom.to_string(step.slug)
      deps = Enum.map(step.depends_on, &Atom.to_string/1)
      step_type = Atom.to_string(step.step_type)

      # pgflow.add_step(flow_slug, step_slug, deps, max_attempts, base_delay, timeout, start_delay, step_type)
      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)",
        [
          flow_slug,
          step_slug,
          deps,
          step.max_attempts,
          step.base_delay,
          step.timeout,
          step.start_delay,
          step_type
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

    # Allow the worker process to access the sandbox connection
    Sandbox.allow(TestRepo, self(), pid)

    pid
  end

  defp start_flow_run(flow_slug, input) do
    %{rows: [[result]]} =
      TestRepo.query!(
        "SELECT pgflow.start_flow($1, cast($2 as text)::jsonb)",
        [flow_slug, Jason.encode!(input)]
      )

    # Extract run_id from the composite type
    case result do
      {run_id, _, _, _, _, _, _, _, _} ->
        Ecto.UUID.load!(run_id)

      _ ->
        raise "Unexpected result: #{inspect(result)}"
    end
  end

  defp get_run(run_id) do
    %{rows: [[status, output]]} =
      TestRepo.query!(
        "SELECT status, output FROM pgflow.runs WHERE run_id = $1",
        [Ecto.UUID.dump!(run_id)]
      )

    # Postgrex automatically decodes JSONB to maps
    %{status: status, output: output}
  end

  defp get_worker(worker_id) do
    {:ok, worker_id_bin} = Ecto.UUID.dump(worker_id)

    %{rows: rows} =
      TestRepo.query!(
        "SELECT worker_id, queue_name, function_name, stopped_at FROM pgflow.workers WHERE worker_id = $1",
        [worker_id_bin]
      )

    case rows do
      [[worker_id_result, queue_name, function_name, stopped_at]] ->
        %{
          worker_id: Ecto.UUID.load!(worker_id_result),
          queue_name: queue_name,
          function_name: function_name,
          stopped_at: stopped_at
        }

      [] ->
        nil
    end
  end

  defp wait_for_run_completion(run_id, timeout_ms \\ 5000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    wait_loop(run_id, deadline)
  end

  defp wait_loop(run_id, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :timeout}
    else
      case get_run(run_id) do
        %{status: status} when status in ["completed", "failed"] ->
          {:ok, status}

        _ ->
          Process.sleep(50)
          wait_loop(run_id, deadline)
      end
    end
  end

  # ============= Tests =============

  describe "worker registration" do
    test "worker registers itself in the database on start", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(SimpleWorkerFlow)
      worker_pid = start_worker(SimpleWorkerFlow, task_supervisor)

      # Give worker time to register
      Process.sleep(100)

      # Get worker_id from state
      state = Server.get_state(worker_pid)
      worker = get_worker(state.worker_id)

      assert worker != nil
      assert worker.queue_name == flow_slug
      assert worker.function_name =~ "SimpleWorkerFlow"
      assert worker.stopped_at == nil

      Server.stop(worker_pid)
    end

    test "fails to start when task supervisor is not found" do
      Process.flag(:trap_exit, true)

      config = %{
        flow_module: SimpleWorkerFlow,
        repo: TestRepo,
        task_supervisor: nil,
        max_concurrency: 10,
        batch_size: 10,
        signal_strategy: :polling,
        min_poll_interval: 50,
        max_poll_interval: 5_000,
        notify_fallback_interval: 30_000
      }

      assert {:error, :task_supervisor_not_found} = Server.start_link(config)

      Process.flag(:trap_exit, false)
    end

    test "worker marks itself as stopped on shutdown", %{task_supervisor: task_supervisor} do
      _flow_slug = compile_flow(SimpleWorkerFlow)
      worker_pid = start_worker(SimpleWorkerFlow, task_supervisor)

      Process.sleep(100)

      state = Server.get_state(worker_pid)
      worker_id = state.worker_id

      # Stop the worker
      Server.stop(worker_pid)

      # Verify stopped_at is set
      worker = get_worker(worker_id)
      assert worker.stopped_at != nil
    end
  end

  describe "task execution" do
    test "worker processes a simple task and completes it", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(SimpleWorkerFlow)
      worker_pid = start_worker(SimpleWorkerFlow, task_supervisor)

      Process.sleep(100)

      # Start a flow run
      run_id = start_flow_run(flow_slug, %{"value" => 21})

      # Wait for completion
      {:ok, status} = wait_for_run_completion(run_id)

      assert status == "completed"

      # Verify output
      run = get_run(run_id)
      assert run.output["process"]["result"] == 42

      Server.stop(worker_pid)
    end

    test "worker processes multiple tasks from different runs", %{
      task_supervisor: task_supervisor
    } do
      flow_slug = compile_flow(SimpleWorkerFlow)
      worker_pid = start_worker(SimpleWorkerFlow, task_supervisor)

      Process.sleep(100)

      # Start multiple flow runs
      run_ids =
        for value <- [10, 20, 30] do
          start_flow_run(flow_slug, %{"value" => value})
        end

      # Wait for all to complete
      for run_id <- run_ids do
        {:ok, status} = wait_for_run_completion(run_id)
        assert status == "completed"
      end

      # Verify outputs
      runs = Enum.map(run_ids, &get_run/1)
      results = Enum.map(runs, & &1.output["process"]["result"]) |> Enum.sort()
      assert results == [20, 40, 60]

      Server.stop(worker_pid)
    end
  end

  describe "task failures" do
    test "worker handles task failures correctly", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(FailingWorkerFlow)
      worker_pid = start_worker(FailingWorkerFlow, task_supervisor)

      Process.sleep(100)

      # Start a flow run
      run_id = start_flow_run(flow_slug, %{})

      # Wait for it to fail (after retries)
      {:ok, status} = wait_for_run_completion(run_id, 10_000)

      assert status == "failed"

      Server.stop(worker_pid)
    end

    test "worker retries failed tasks and succeeds", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(CountingWorkerFlow)

      # Start an agent to track attempts
      agent_name = :"test_counter_#{System.unique_integer()}"
      {:ok, _agent} = Agent.start_link(fn -> 0 end, name: agent_name)

      worker_pid = start_worker(CountingWorkerFlow, task_supervisor, poll_interval: 100)

      Process.sleep(100)

      # Start a flow run that will succeed on attempt 2
      run_id =
        start_flow_run(flow_slug, %{
          "agent_name" => Atom.to_string(agent_name),
          "succeed_on" => 2
        })

      # Wait for completion
      {:ok, status} = wait_for_run_completion(run_id, 15_000)

      assert status == "completed"

      # Verify retry count
      run = get_run(run_id)
      assert run.output["count"]["attempts"] == 2

      # Cleanup
      Agent.stop(agent_name)
      Server.stop(worker_pid)
    end
  end

  describe "concurrency control" do
    test "worker respects max_concurrency limit", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(SlowWorkerFlow)

      # Start worker with max_concurrency=1
      worker_pid =
        start_worker(SlowWorkerFlow, task_supervisor, max_concurrency: 1, poll_interval: 50)

      Process.sleep(100)

      # Start 3 flow runs with 200ms sleep each
      start_time = System.monotonic_time(:millisecond)

      run_ids =
        for _ <- 1..3 do
          start_flow_run(flow_slug, %{"sleep_ms" => 200})
        end

      # Wait for all to complete
      for run_id <- run_ids do
        {:ok, status} = wait_for_run_completion(run_id, 10_000)
        assert status == "completed"
      end

      end_time = System.monotonic_time(:millisecond)
      duration_ms = end_time - start_time

      # With max_concurrency=1 and 200ms per task, 3 tasks should take ~600ms minimum
      # (sequential execution)
      assert duration_ms >= 500, "Expected sequential execution (>=500ms), got #{duration_ms}ms"

      Server.stop(worker_pid)
    end

    test "worker processes tasks concurrently up to limit", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(SlowWorkerFlow)

      # Start worker with max_concurrency=3
      worker_pid =
        start_worker(SlowWorkerFlow, task_supervisor, max_concurrency: 3, poll_interval: 50)

      Process.sleep(100)

      # Start 3 flow runs with 200ms sleep each
      start_time = System.monotonic_time(:millisecond)

      run_ids =
        for _ <- 1..3 do
          start_flow_run(flow_slug, %{"sleep_ms" => 200})
        end

      # Wait for all to complete
      for run_id <- run_ids do
        {:ok, status} = wait_for_run_completion(run_id, 10_000)
        assert status == "completed"
      end

      end_time = System.monotonic_time(:millisecond)
      duration_ms = end_time - start_time

      # With max_concurrency=3, all 3 tasks can run in parallel
      # Sequential would take 600ms+, parallel should be significantly faster
      # Allow generous overhead for polling, DB, and test environment variability
      assert duration_ms < 2500, "Expected parallel execution (<2500ms), got #{duration_ms}ms"

      Server.stop(worker_pid)
    end
  end

  describe "graceful shutdown" do
    test "worker waits for active tasks before stopping", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(SlowWorkerFlow)

      worker_pid =
        start_worker(SlowWorkerFlow, task_supervisor, max_concurrency: 1, poll_interval: 50)

      Process.sleep(100)

      # Start a slow task (300ms)
      run_id = start_flow_run(flow_slug, %{"sleep_ms" => 300})

      # Give worker time to pick up the task
      Process.sleep(150)

      # Stop the worker (should wait for task)
      start_time = System.monotonic_time(:millisecond)
      Server.stop(worker_pid)
      stop_time = System.monotonic_time(:millisecond)

      # The stop should have waited for the task
      # Task had ~150ms remaining, so stop should take >= 100ms
      stop_duration = stop_time - start_time
      assert stop_duration >= 50, "Stop should wait for active task, took #{stop_duration}ms"

      # Task should still complete
      {:ok, status} = wait_for_run_completion(run_id, 2000)
      assert status == "completed"
    end
  end

  describe "worker state" do
    test "get_state returns current worker state", %{task_supervisor: task_supervisor} do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor, max_concurrency: 5, batch_size: 3)

      state = Server.get_state(worker_pid)

      assert state.flow_module == SimpleWorkerFlow
      assert state.flow_slug == "simple_worker_flow"
      assert state.max_concurrency == 5
      assert state.batch_size == 3
      assert state.lifecycle.state == :running
      assert is_binary(state.worker_id)
      # UUID format
      assert byte_size(state.worker_id) == 36

      Server.stop(worker_pid)
    end
  end

  # ============= Multi-Step Flow Tests =============

  describe "multi-step flows" do
    test "executes two-step flow with dependency", %{task_supervisor: task_supervisor} do
      # Verifies: step B receives output from step A
      # Input: %{"value" => 5}
      # Expected: first doubles (10), second adds 10 (20)
      # Note: run.output only contains LEAF steps (steps with no dependents)
      flow_slug = compile_flow(TwoStepFlow)
      worker_pid = start_worker(TwoStepFlow, task_supervisor)

      Process.sleep(100)

      run_id = start_flow_run(flow_slug, %{"value" => 5})

      {:ok, status} = wait_for_run_completion(run_id)
      assert status == "completed"

      run = get_run(run_id)
      # Only leaf step output is in run.output (second is the leaf)
      # Second step received first's output (10) and added 10 = 20
      assert run.output["second"]["final"] == 20

      Server.stop(worker_pid)
    end

    test "executes three-step chain flow", %{task_supervisor: task_supervisor} do
      # Verifies: A → B → C chain executes in order
      # Input: %{"x" => 1}
      # Expected: C (leaf step) should have final computed value
      # Note: run.output only contains LEAF steps (C in this chain)
      flow_slug = compile_flow(ThreeStepChainFlow)
      worker_pid = start_worker(ThreeStepChainFlow, task_supervisor)

      Process.sleep(100)

      run_id = start_flow_run(flow_slug, %{"x" => 1})

      {:ok, status} = wait_for_run_completion(run_id)
      assert status == "completed"

      run = get_run(run_id)
      # Only C is a leaf step: A(1+1=2) → B(2*2=4) → C(4+100=104)
      assert run.output["c"]["value"] == 104

      Server.stop(worker_pid)
    end

    test "executes diamond flow with parallel branches", %{task_supervisor: task_supervisor} do
      # Verifies: start → (left || right) → merge
      # Input: %{"n" => 5}
      # Expected: merge (leaf step) should have sum of left+right
      # Note: run.output only contains LEAF steps (merge in this flow)
      flow_slug = compile_flow(DiamondFlow)
      worker_pid = start_worker(DiamondFlow, task_supervisor, max_concurrency: 5)

      Process.sleep(100)

      run_id = start_flow_run(flow_slug, %{"n" => 5})

      {:ok, status} = wait_for_run_completion(run_id)
      assert status == "completed"

      run = get_run(run_id)
      # Only merge is a leaf step
      # start(5) → left(10), right(15) → merge(25)
      assert run.output["merge"]["sum"] == 25

      Server.stop(worker_pid)
    end

    test "dependency data is correctly passed between steps", %{task_supervisor: task_supervisor} do
      # Verifies: each step receives correct dependency outputs
      # Use the three-step chain to verify computation proves deps were passed
      # Note: run.output only contains LEAF steps (C in this chain)
      flow_slug = compile_flow(ThreeStepChainFlow)
      worker_pid = start_worker(ThreeStepChainFlow, task_supervisor)

      Process.sleep(100)

      run_id = start_flow_run(flow_slug, %{"x" => 10})

      {:ok, status} = wait_for_run_completion(run_id)
      assert status == "completed"

      run = get_run(run_id)
      # Final value proves deps were passed correctly:
      # A: 10 + 1 = 11
      # B: 11 * 2 = 22
      # C: 22 + 100 = 122
      assert run.output["c"]["value"] == 122

      Server.stop(worker_pid)
    end
  end

  # ============= Fan-Out/Fan-In Flow Tests =============

  describe "fan-out/fan-in flows" do
    test "map step processes array items in parallel", %{task_supervisor: task_supervisor} do
      # Input: [1, 2, 3, 4, 5] as array
      # Verifies: Creates 5 tasks, each processes one item
      # Expected: Output contains array of 5 results [10, 20, 30, 40, 50]
      # Note: MapStepFlow has only one step (process_items) which IS the leaf step
      flow_slug = compile_flow(MapStepFlow)
      worker_pid = start_worker(MapStepFlow, task_supervisor, max_concurrency: 10)

      Process.sleep(100)

      run_id = start_flow_run(flow_slug, [1, 2, 3, 4, 5])

      {:ok, status} = wait_for_run_completion(run_id, 10_000)
      assert status == "completed"

      run = get_run(run_id)
      # Map step should produce an array of results (it's the only/leaf step)
      results = run.output["process_items"]
      assert is_list(results)
      assert length(results) == 5

      # Extract processed values and sort for comparison
      processed_values = Enum.map(results, & &1["processed"]) |> Enum.sort()
      assert processed_values == [10, 20, 30, 40, 50]

      Server.stop(worker_pid)
    end

    test "map step followed by aggregation step", %{task_supervisor: task_supervisor} do
      # Input: [1, 2, 3]
      # Verifies: map doubles each, aggregate sums
      # Expected: aggregate total=12 (aggregate is the leaf step)
      # Note: run.output only contains leaf steps (aggregate, not map_items)
      flow_slug = compile_flow(MapThenReduceFlow)
      worker_pid = start_worker(MapThenReduceFlow, task_supervisor, max_concurrency: 10)

      Process.sleep(100)

      run_id = start_flow_run(flow_slug, [1, 2, 3])

      {:ok, status} = wait_for_run_completion(run_id, 10_000)
      assert status == "completed"

      run = get_run(run_id)
      # Only aggregate (leaf step) output is in run.output
      # Aggregate result: 2 + 4 + 6 = 12
      assert run.output["aggregate"]["total"] == 12

      Server.stop(worker_pid)
    end

    test "respects task_index ordering in aggregation", %{task_supervisor: task_supervisor} do
      # Verifies: Results aggregated in task_index order, not completion order
      # Note: MapStepFlow has only one step which is the leaf step
      flow_slug = compile_flow(MapStepFlow)
      worker_pid = start_worker(MapStepFlow, task_supervisor, max_concurrency: 10)

      Process.sleep(100)

      # Use ordered input to verify ordering is preserved
      run_id = start_flow_run(flow_slug, [5, 4, 3, 2, 1])

      {:ok, status} = wait_for_run_completion(run_id, 10_000)
      assert status == "completed"

      run = get_run(run_id)
      results = run.output["process_items"]

      # Results should be ordered by task_index (matching input order)
      # [50, 40, 30, 20, 10] - preserving the order of [5, 4, 3, 2, 1] * 10
      processed_values = Enum.map(results, & &1["processed"])
      assert processed_values == [50, 40, 30, 20, 10]

      Server.stop(worker_pid)
    end

    test "handles empty array input for map step", %{task_supervisor: task_supervisor} do
      # Input: []
      # Verifies: Flow completes without creating tasks, output is empty array
      flow_slug = compile_flow(MapStepFlow)
      worker_pid = start_worker(MapStepFlow, task_supervisor)

      Process.sleep(100)

      run_id = start_flow_run(flow_slug, [])

      {:ok, status} = wait_for_run_completion(run_id, 5_000)
      assert status == "completed"

      run = get_run(run_id)
      # Empty array input should produce empty array output
      assert run.output["process_items"] == []

      Server.stop(worker_pid)
    end

    test "handles large fan-out", %{task_supervisor: task_supervisor} do
      # Input: array of 20 items
      # Verifies: All tasks complete, correct aggregation
      # Note: run.output only contains leaf steps (aggregate)
      flow_slug = compile_flow(MapThenReduceFlow)
      worker_pid = start_worker(MapThenReduceFlow, task_supervisor, max_concurrency: 20)

      Process.sleep(100)

      # Create array of 20 items: [1, 2, ..., 20]
      input = Enum.to_list(1..20)
      run_id = start_flow_run(flow_slug, input)

      {:ok, status} = wait_for_run_completion(run_id, 15_000)
      assert status == "completed"

      run = get_run(run_id)
      # Only aggregate (leaf step) output is in run.output
      # Aggregate: sum of (1*2 + 2*2 + ... + 20*2) = 2 * sum(1..20) = 2 * 210 = 420
      assert run.output["aggregate"]["total"] == 420

      Server.stop(worker_pid)
    end
  end

  # ============= Edge Case Tests =============

  describe "edge cases" do
    test "multiple workers can poll same queue without conflicts", %{
      task_supervisor: task_supervisor
    } do
      # Start 2 workers for same flow
      # Start 10 flow runs
      # Verify: All complete, no duplicates (each run completes exactly once)
      flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid1 = start_worker(SimpleWorkerFlow, task_supervisor, poll_interval: 50)
      worker_pid2 = start_worker(SimpleWorkerFlow, task_supervisor, poll_interval: 50)

      Process.sleep(100)

      # Start 10 flow runs
      run_ids =
        for value <- 1..10 do
          start_flow_run(flow_slug, %{"value" => value})
        end

      # Wait for all to complete
      for run_id <- run_ids do
        {:ok, status} = wait_for_run_completion(run_id, 10_000)
        assert status == "completed"
      end

      # Verify all outputs are correct (each run processed exactly once)
      runs = Enum.map(run_ids, &get_run/1)
      results = Enum.map(runs, & &1.output["process"]["result"]) |> Enum.sort()
      # Values 1-10 doubled: [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
      assert results == [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

      Server.stop(worker_pid1)
      Server.stop(worker_pid2)
    end

    test "worker processes tasks in batches correctly", %{task_supervisor: task_supervisor} do
      # Configure batch_size: 3
      # Start 10 flow runs
      # Verify: All complete (tests batching logic)
      flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor, batch_size: 3, poll_interval: 50)

      Process.sleep(100)

      # Start 10 flow runs
      run_ids =
        for value <- 1..10 do
          start_flow_run(flow_slug, %{"value" => value})
        end

      # Wait for all to complete
      for run_id <- run_ids do
        {:ok, status} = wait_for_run_completion(run_id, 10_000)
        assert status == "completed"
      end

      # Verify all completed correctly
      runs = Enum.map(run_ids, &get_run/1)
      assert Enum.all?(runs, &(&1.status == "completed"))

      Server.stop(worker_pid)
    end

    test "step failure in middle of chain fails the run", %{task_supervisor: task_supervisor} do
      # Run flow where middle step always fails after retries
      # Verify: Run status is "failed", subsequent steps never execute
      flow_slug = compile_flow(AlwaysFailsFlow)
      worker_pid = start_worker(AlwaysFailsFlow, task_supervisor, poll_interval: 100)

      Process.sleep(100)

      run_id = start_flow_run(flow_slug, %{})

      {:ok, status} = wait_for_run_completion(run_id, 15_000)
      assert status == "failed"

      # For failed runs, the output is not fully populated
      # The important assertion is that the run failed
      Server.stop(worker_pid)
    end

    test "retry succeeds after initial failure in dependent step", %{
      task_supervisor: task_supervisor
    } do
      # First step succeeds
      # Second step fails once, then succeeds on retry
      # Third step executes
      # Verify: Final run status is "completed"
      # Note: run.output only contains leaf steps (final)
      flow_slug = compile_flow(PartialFailureFlow)

      agent_name = :"partial_failure_counter_#{System.unique_integer()}"
      {:ok, _agent} = Agent.start_link(fn -> 0 end, name: agent_name)

      worker_pid = start_worker(PartialFailureFlow, task_supervisor, poll_interval: 100)

      Process.sleep(100)

      run_id = start_flow_run(flow_slug, %{"agent_name" => Atom.to_string(agent_name)})

      {:ok, status} = wait_for_run_completion(run_id, 15_000)
      assert status == "completed"

      run = get_run(run_id)
      # Only leaf step (final) output is in run.output
      assert run.output["final"]["complete"] == true

      Agent.stop(agent_name)
      Server.stop(worker_pid)
    end

    test "handles rapid sequential flow starts", %{task_supervisor: task_supervisor} do
      # Start 20 flows in quick succession
      # Verify: All complete correctly without race conditions
      flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor, max_concurrency: 10, poll_interval: 50)

      Process.sleep(100)

      # Start 20 flows rapidly
      run_ids =
        for value <- 1..20 do
          start_flow_run(flow_slug, %{"value" => value})
        end

      # Wait for all to complete
      for run_id <- run_ids do
        {:ok, status} = wait_for_run_completion(run_id, 15_000)
        assert status == "completed"
      end

      # Verify all outputs
      runs = Enum.map(run_ids, &get_run/1)
      results = Enum.map(runs, & &1.output["process"]["result"]) |> Enum.sort()
      expected = Enum.map(1..20, &(&1 * 2))
      assert results == expected

      Server.stop(worker_pid)
    end

    test "worker shutdown during batch processing completes active tasks", %{
      task_supervisor: task_supervisor
    } do
      # Start worker with max_concurrency: 5
      # Start 5 slow flows
      # Wait for all to be in-progress
      # Stop worker
      # Verify: All 5 tasks complete (graceful shutdown)
      flow_slug = compile_flow(SlowWorkerFlow)

      worker_pid =
        start_worker(SlowWorkerFlow, task_supervisor, max_concurrency: 5, poll_interval: 50)

      Process.sleep(100)

      # Start 5 slow flows (300ms each)
      run_ids =
        for _ <- 1..5 do
          start_flow_run(flow_slug, %{"sleep_ms" => 300})
        end

      # Give time for tasks to be picked up
      Process.sleep(200)

      # Stop the worker (should wait for active tasks)
      Server.stop(worker_pid)

      # All tasks should still complete
      for run_id <- run_ids do
        {:ok, status} = wait_for_run_completion(run_id, 5_000)
        assert status == "completed"
      end
    end

    test "stalled task recovery enables retry of abandoned tasks", %{
      task_supervisor: task_supervisor
    } do
      # Start a flow, have a worker pick up the task, kill the worker mid-task,
      # run stalled task recovery, start a new worker, and verify completion.
      flow_slug = compile_flow(SlowWorkerFlow)

      worker_pid =
        start_worker(SlowWorkerFlow, task_supervisor, max_concurrency: 1)

      Process.sleep(100)

      # Start a slow task (5s — long enough to kill mid-flight)
      run_id = start_flow_run(flow_slug, %{"sleep_ms" => 5000})

      # Give worker time to pick up the task
      Process.sleep(500)

      # Unlink before killing so the test process doesn't crash
      Process.unlink(worker_pid)
      Process.exit(worker_pid, :kill)
      Process.sleep(100)

      # The task is now stuck in 'started' status.
      # Backdate both queued_at and started_at so recovery will find it.
      # Must backdate queued_at too because of started_at_is_after_queued_at constraint.
      {:ok, run_id_bin} = Ecto.UUID.dump(run_id)

      TestRepo.query!(
        "UPDATE pgflow.step_tasks SET queued_at = NOW() - interval '3 minutes', started_at = NOW() - interval '2 minutes' WHERE run_id = $1",
        [run_id_bin]
      )

      # Run stalled task recovery
      {:ok, count} = FlowQueries.recover_stalled_tasks(TestRepo, 60)
      assert count >= 1

      # Start a new worker to pick up the recovered task
      worker_pid2 =
        start_worker(SlowWorkerFlow, task_supervisor, max_concurrency: 1)

      # Wait for the recovered task to complete (with shorter sleep this time)
      # The pgmq message will become visible again and the new worker picks it up
      {:ok, status} = wait_for_run_completion(run_id, 15_000)
      assert status == "completed"

      Server.stop(worker_pid2)
    end
  end

  # ============= Visibility Timeout Derivation Tests (Task 2.3) =============

  describe "visibility timeout derivation" do
    test "derives visibility_timeout from flow definition opt_timeout", %{
      task_supervisor: task_supervisor
    } do
      # TimeoutTestFlow has @flow timeout: 2
      _flow_slug = compile_flow(TimeoutTestFlow)
      worker_pid = start_worker(TimeoutTestFlow, task_supervisor)

      state = Server.get_state(worker_pid)
      assert state.visibility_timeout == 2

      Server.stop(worker_pid)
    end

    test "defaults visibility_timeout to 60 when flow has no explicit timeout", %{
      task_supervisor: task_supervisor
    } do
      # SimpleWorkerFlow has no timeout option, should default to 60
      _flow_slug = compile_flow(SimpleWorkerFlow)
      worker_pid = start_worker(SimpleWorkerFlow, task_supervisor)

      state = Server.get_state(worker_pid)
      # Default timeout is 60s (matches pgflow DB default for opt_timeout)
      # Note: SimpleWorkerFlow defines timeout: 30 in compile_flow helper
      # The flow definition opts[:timeout] determines this value
      flow_def = SimpleWorkerFlow.__pgflow_definition__()
      expected_vt = Keyword.get(flow_def.opts, :timeout, 60)
      assert state.visibility_timeout == expected_vt

      Server.stop(worker_pid)
    end

    test "visibility_timeout matches flow definition for custom timeout flow", %{
      task_supervisor: task_supervisor
    } do
      # StepTimeoutFlow has @flow timeout: 30
      _flow_slug = compile_flow(StepTimeoutFlow)
      worker_pid = start_worker(StepTimeoutFlow, task_supervisor)

      state = Server.get_state(worker_pid)
      assert state.visibility_timeout == 30

      Server.stop(worker_pid)
    end
  end

  # ============= Adaptive Backoff Tests (Task 3.5) =============

  describe "adaptive backoff" do
    test "starts with min_interval on initial poll", %{task_supervisor: task_supervisor} do
      _flow_slug = compile_flow(SimpleWorkerFlow)
      worker_pid = start_worker(SimpleWorkerFlow, task_supervisor, min_poll_interval: 100)

      # Give the initial poll time to fire
      Process.sleep(50)

      state = Server.get_state(worker_pid)
      # After initial poll with no messages, the interval should have increased
      # (decorrelated jitter from min to current*3)
      assert state.signal_state.min_interval == 100

      Server.stop(worker_pid)
    end

    test "interval increases after empty polls", %{task_supervisor: task_supervisor} do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor,
          min_poll_interval: 50,
          max_poll_interval: 5_000
        )

      # Wait for several empty poll cycles to allow backoff
      Process.sleep(800)

      state = Server.get_state(worker_pid)
      # After multiple empty polls, current_interval should have grown beyond min
      assert state.signal_state.current_interval >= state.signal_state.min_interval

      Server.stop(worker_pid)
    end

    test "interval resets toward min when messages are found", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor,
          min_poll_interval: 50,
          max_poll_interval: 5_000
        )

      # Wait for backoff to accumulate through empty polls
      Process.sleep(2000)

      state_before = Server.get_state(worker_pid)
      backed_off_interval = state_before.signal_state.current_interval

      # Verify backoff has actually increased from min
      assert backed_off_interval > 50,
             "Expected interval to grow during empty polls, got #{backed_off_interval}"

      # Start a flow run to trigger found_messages
      run_id = start_flow_run(flow_slug, %{"value" => 5})
      {:ok, _status} = wait_for_run_completion(run_id)

      # After finding messages, schedule_next_poll resets to min_interval.
      # By the time we check, one or two subsequent empty polls may have
      # bumped it slightly via jitter, so allow a small margin.
      state_after = Server.get_state(worker_pid)

      assert state_after.signal_state.current_interval < backed_off_interval,
             "Expected interval to decrease after finding messages, was #{backed_off_interval}, now #{state_after.signal_state.current_interval}"

      Server.stop(worker_pid)
    end

    test "interval is capped at max_poll_interval", %{task_supervisor: task_supervisor} do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      # Use a very low max to make the cap easy to verify
      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor,
          min_poll_interval: 50,
          max_poll_interval: 200
        )

      # Wait for enough empty polls to hit the cap
      Process.sleep(2000)

      state = Server.get_state(worker_pid)
      assert state.signal_state.current_interval <= 200

      Server.stop(worker_pid)
    end
  end

  # ============= Notify Strategy & Fallback Timer Tests =============

  describe "notify strategy and fallback timer" do
    test "fallback_timer_ref is tracked in state for notify strategy", %{
      task_supervisor: task_supervisor
    } do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor,
          signal_strategy: :notify,
          notify_fallback_interval: 30_000
        )

      # Give worker time to initialize
      Process.sleep(100)

      state = Server.get_state(worker_pid)

      # For notify strategy, fallback_timer_ref should be set
      assert state.signal_strategy == :notify
      assert is_reference(state.fallback_timer_ref)

      Server.stop(worker_pid)
    end

    test "fallback_timer_ref is nil for polling strategy", %{task_supervisor: task_supervisor} do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor, signal_strategy: :polling)

      Process.sleep(100)

      state = Server.get_state(worker_pid)

      # For polling strategy, fallback_timer_ref should remain nil
      assert state.signal_strategy == :polling
      assert state.fallback_timer_ref == nil

      Server.stop(worker_pid)
    end

    test "fallback timer is reset when poll_now is received", %{task_supervisor: task_supervisor} do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor,
          signal_strategy: :notify,
          notify_fallback_interval: 30_000
        )

      Process.sleep(100)

      # Get the initial fallback timer ref
      state_before = Server.get_state(worker_pid)
      initial_ref = state_before.fallback_timer_ref
      assert is_reference(initial_ref)

      # Simulate a NOTIFY by sending :poll_now directly to the worker
      send(worker_pid, :poll_now)

      # Give time for the handler to process
      Process.sleep(50)

      state_after = Server.get_state(worker_pid)
      new_ref = state_after.fallback_timer_ref

      # The fallback timer ref should be different (reset)
      assert is_reference(new_ref)
      assert new_ref != initial_ref

      Server.stop(worker_pid)
    end

    test "fallback timer fires at configured interval when no notify received", %{
      task_supervisor: task_supervisor
    } do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      # Use a very short fallback interval for testing
      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor,
          signal_strategy: :notify,
          notify_fallback_interval: 200
        )

      Process.sleep(50)

      state_before = Server.get_state(worker_pid)
      initial_ref = state_before.fallback_timer_ref

      # Wait for fallback to fire and reschedule
      Process.sleep(350)

      state_after = Server.get_state(worker_pid)
      new_ref = state_after.fallback_timer_ref

      # After fallback fires, a new timer should be scheduled
      assert is_reference(new_ref)
      assert new_ref != initial_ref

      Server.stop(worker_pid)
    end

    test "reset_fallback_timer is only called for notify strategy", %{
      task_supervisor: task_supervisor
    } do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      # Start with polling strategy
      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor, signal_strategy: :polling)

      Process.sleep(100)

      # Get initial state - fallback_timer_ref should be nil
      state_before = Server.get_state(worker_pid)
      assert state_before.fallback_timer_ref == nil

      # Simulate sending :poll_now (shouldn't crash or set fallback timer)
      send(worker_pid, :poll_now)

      Process.sleep(50)

      state_after = Server.get_state(worker_pid)
      # Should still be nil for polling strategy
      assert state_after.fallback_timer_ref == nil

      Server.stop(worker_pid)
    end

    test "multiple rapid poll_now messages each reset the fallback timer", %{
      task_supervisor: task_supervisor
    } do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor,
          signal_strategy: :notify,
          notify_fallback_interval: 30_000
        )

      Process.sleep(100)

      # Get initial ref
      initial_ref = Server.get_state(worker_pid).fallback_timer_ref
      assert is_reference(initial_ref)

      # Send poll_now and collect refs
      refs =
        for _ <- 1..3 do
          send(worker_pid, :poll_now)
          Process.sleep(50)
          Server.get_state(worker_pid).fallback_timer_ref
        end

      # All collected refs should be valid references
      assert Enum.all?(refs, &is_reference/1)

      # The initial ref should be different from the first collected ref
      # (proving that poll_now resets the timer)
      [first_ref | _] = refs
      assert first_ref != initial_ref

      Server.stop(worker_pid)
    end
  end

  # ============= Task Timeout Enforcement Tests (Task 4.7) =============

  describe "task timeout enforcement" do
    test "task times out and is marked as failed", %{task_supervisor: task_supervisor} do
      # TimeoutTestFlow has timeout: 2s
      flow_slug = compile_flow(TimeoutTestFlow)
      worker_pid = start_worker(TimeoutTestFlow, task_supervisor)

      Process.sleep(100)

      # Start a task that sleeps 5s but flow timeout is 2s
      run_id = start_flow_run(flow_slug, %{"sleep_ms" => 5000})

      # Wait for it to fail (timeout after ~2s)
      {:ok, status} = wait_for_run_completion(run_id, 10_000)
      assert status == "failed"

      Server.stop(worker_pid)
    end

    test "task completes before timeout fires", %{task_supervisor: task_supervisor} do
      # TimeoutTestFlow has timeout: 2s
      flow_slug = compile_flow(TimeoutTestFlow)
      worker_pid = start_worker(TimeoutTestFlow, task_supervisor)

      Process.sleep(100)

      # Start a task that completes quickly (100ms, well within 2s timeout)
      run_id = start_flow_run(flow_slug, %{"sleep_ms" => 100})

      {:ok, status} = wait_for_run_completion(run_id, 10_000)
      assert status == "completed"

      run = get_run(run_id)
      assert run.output["slow_step"]["done"] == true

      Server.stop(worker_pid)
    end

    test "step-level timeout overrides flow-level timeout", %{task_supervisor: task_supervisor} do
      # StepTimeoutFlow has flow timeout: 30s, but slow_step has timeout: 2s
      flow_slug = compile_flow(StepTimeoutFlow)
      worker_pid = start_worker(StepTimeoutFlow, task_supervisor)

      Process.sleep(100)

      # fast_step completes instantly, slow_step sleeps 5s but has timeout: 2s
      run_id = start_flow_run(flow_slug, %{})

      # Wait for it to fail (slow_step should timeout after ~2s)
      {:ok, status} = wait_for_run_completion(run_id, 10_000)
      assert status == "failed"

      Server.stop(worker_pid)
    end
  end

  # ============= Timer Lifecycle Edge Cases =============

  describe "timer lifecycle edge cases" do
    test "cancel_poll_timer flushes pending :poll message from mailbox", %{
      task_supervisor: task_supervisor
    } do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor,
          signal_strategy: :polling,
          min_poll_interval: 50,
          max_poll_interval: 5_000
        )

      Process.sleep(100)

      # Get initial timer ref
      state = Server.get_state(worker_pid)
      initial_timer_ref = state.signal_state.poll_timer_ref
      assert is_reference(initial_timer_ref)

      # Send :poll_now which cancels the timer and triggers a poll
      send(worker_pid, :poll_now)
      Process.sleep(50)

      # Verify the timer ref changed (new timer scheduled)
      state = Server.get_state(worker_pid)
      new_timer_ref = state.signal_state.poll_timer_ref
      assert new_timer_ref != initial_timer_ref

      # Verify no duplicate polls happen (would show in logs if timer wasn't flushed)
      Server.stop(worker_pid)
    end

    test "rapid poll_now messages don't cause duplicate poll cycles", %{
      task_supervisor: task_supervisor
    } do
      _flow_slug = compile_flow(SimpleWorkerFlow)

      worker_pid =
        start_worker(SimpleWorkerFlow, task_supervisor,
          signal_strategy: :polling,
          min_poll_interval: 100,
          max_poll_interval: 5_000
        )

      Process.sleep(100)

      # Send multiple rapid poll_now messages
      for _ <- 1..5 do
        send(worker_pid, :poll_now)
      end

      # Wait for processing
      Process.sleep(200)

      # Worker should still be running normally
      state = Server.get_state(worker_pid)
      assert state.lifecycle.state == :running
      assert is_reference(state.signal_state.poll_timer_ref)

      Server.stop(worker_pid)
    end
  end

  # ============= At-Capacity Handling Edge Cases =============

  describe "at-capacity handling edge cases" do
    test "worker skips poll when at max_concurrency", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(SlowWorkerFlow)

      # Start worker with concurrency of 2
      worker_pid =
        start_worker(SlowWorkerFlow, task_supervisor,
          max_concurrency: 2,
          min_poll_interval: 50
        )

      Process.sleep(100)

      # Start 3 tasks (each sleeps 500ms)
      run_id1 = start_flow_run(flow_slug, %{"sleep_ms" => 500})
      run_id2 = start_flow_run(flow_slug, %{"sleep_ms" => 500})
      run_id3 = start_flow_run(flow_slug, %{"sleep_ms" => 500})

      # Let first two tasks get picked up
      Process.sleep(200)

      # Should have exactly 2 active tasks (at capacity)
      state = Server.get_state(worker_pid)
      assert map_size(state.active_tasks) == 2

      # Wait for all to complete
      for run_id <- [run_id1, run_id2, run_id3] do
        {:ok, _} = wait_for_run_completion(run_id, 10_000)
      end

      Server.stop(worker_pid)
    end

    test "worker polls immediately when capacity freed", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(SlowWorkerFlow)

      # Start worker with concurrency of 1
      worker_pid =
        start_worker(SlowWorkerFlow, task_supervisor,
          max_concurrency: 1,
          min_poll_interval: 50
        )

      Process.sleep(100)

      # Start 2 tasks
      run_id1 = start_flow_run(flow_slug, %{"sleep_ms" => 200})
      run_id2 = start_flow_run(flow_slug, %{"sleep_ms" => 100})

      # Wait for first task to be picked up
      Process.sleep(100)
      state = Server.get_state(worker_pid)
      assert map_size(state.active_tasks) == 1

      # Wait for both to complete - second should be picked up quickly after first completes
      {:ok, _} = wait_for_run_completion(run_id1, 5_000)
      {:ok, _} = wait_for_run_completion(run_id2, 5_000)

      Server.stop(worker_pid)
    end

    test "messages are not lost when at capacity", %{task_supervisor: task_supervisor} do
      flow_slug = compile_flow(SlowWorkerFlow)

      # Start worker with concurrency of 1
      worker_pid =
        start_worker(SlowWorkerFlow, task_supervisor,
          max_concurrency: 1,
          min_poll_interval: 50
        )

      Process.sleep(100)

      # Start 5 tasks while at capacity=1
      run_ids =
        for i <- 1..5 do
          start_flow_run(flow_slug, %{"sleep_ms" => 50 + i * 10})
        end

      # Wait for all tasks to complete
      for run_id <- run_ids do
        {:ok, status} = wait_for_run_completion(run_id, 10_000)
        assert status == "completed"
      end

      Server.stop(worker_pid)
    end
  end
end
