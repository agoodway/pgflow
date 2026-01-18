defmodule PgFlow.IntegrationCase do
  @moduledoc """
  This module defines the setup for integration tests that require
  the full pgflow database schema.

  Uses `pgflow_tests.reset_db()` before each test to ensure a clean state,
  matching the TypeScript/Deno reference implementation test approach.

  Unlike DataCase, this does NOT use the SQL Sandbox - it makes actual
  database changes and relies on reset_db() for cleanup between tests.
  """

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox

  using do
    quote do
      alias PgFlow.TestRepo

      import Ecto.Query
      import PgFlow.IntegrationCase
    end
  end

  setup _tags do
    # For integration tests, we don't use the sandbox
    # Instead, we use pgflow_tests.reset_db() to clean up between tests
    # This matches the TypeScript reference implementation approach

    # Checkout connection without sandbox
    :ok = Sandbox.checkout(PgFlow.TestRepo)
    # Use shared mode to avoid ownership issues
    Sandbox.mode(PgFlow.TestRepo, {:shared, self()})

    # Reset the database to a clean state
    PgFlow.TestRepo.query!("SELECT pgflow_tests.reset_db()")
    :ok
  end

  # ============= Test Helpers =============

  @doc """
  Creates a flow definition in the database.

  ## Example

      create_flow("my_test_flow")
  """
  def create_flow(flow_slug) do
    PgFlow.TestRepo.query!("SELECT pgflow.create_flow($1)", [flow_slug])
  end

  @doc """
  Adds a step to a flow.

  ## Example

      add_step("my_flow", "first_step")
      add_step("my_flow", "second_step", deps: ["first_step"])
      add_step("my_flow", "map_step", type: "map")
  """
  def add_step(flow_slug, step_slug, opts \\ []) do
    deps = Keyword.get(opts, :deps, [])
    step_type = Keyword.get(opts, :type, "single")

    if Enum.empty?(deps) do
      PgFlow.TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, ARRAY[]::text[], null, null, null, null, $3)",
        [flow_slug, step_slug, step_type]
      )
    else
      PgFlow.TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, $3::text[], null, null, null, null, $4)",
        [flow_slug, step_slug, deps, step_type]
      )
    end
  end

  @doc """
  Adds a step with step-level retry options that override flow defaults.

  ## Options
    * `:deps` - List of dependency step slugs (default: [])
    * `:max_attempts` - Step-level max attempts (overrides flow default)
    * `:base_delay` - Step-level base delay (overrides flow default)
    * `:timeout` - Step-level timeout (overrides flow default)
    * `:start_delay` - Delay before task becomes visible
    * `:type` - Step type: "single" or "map" (default: "single")

  ## Example

      add_step_with_retry_options("my_flow", "custom_step", max_attempts: 2, base_delay: 5)
  """
  def add_step_with_retry_options(flow_slug, step_slug, opts \\ []) do
    deps = Keyword.get(opts, :deps, [])
    max_attempts = Keyword.get(opts, :max_attempts)
    base_delay = Keyword.get(opts, :base_delay)
    timeout = Keyword.get(opts, :timeout)
    start_delay = Keyword.get(opts, :start_delay)
    step_type = Keyword.get(opts, :type, "single")

    # pgflow.add_step(flow_slug, step_slug, deps_slugs, max_attempts, base_delay, timeout, start_delay, step_type)
    PgFlow.TestRepo.query!(
      "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)",
      [flow_slug, step_slug, deps, max_attempts, base_delay, timeout, start_delay, step_type]
    )
  end

  @doc """
  Creates a simple flow with multiple steps and their dependencies.

  ## Example

      create_simple_flow("test_flow", [
        %{slug: "step_a", deps: []},
        %{slug: "step_b", deps: ["step_a"]}
      ])
  """
  def create_simple_flow(flow_slug, steps) do
    create_flow(flow_slug)

    for step <- steps do
      add_step(flow_slug, step.slug, deps: step.deps)
    end
  end

  @doc """
  Starts a flow run with the given input.

  Returns the run_id as a UUID string.
  """
  def start_flow_run(flow_slug, input) do
    # start_flow returns a composite type - we need to extract run_id from it
    # Use cast($2 as text)::jsonb to properly parse the JSON string as JSONB
    # (simple ::jsonb cast interprets the string as a JSON string value, not parsed JSON)
    %{rows: [[result]]} =
      PgFlow.TestRepo.query!("SELECT pgflow.start_flow($1, cast($2 as text)::jsonb)", [
        flow_slug,
        Jason.encode!(input)
      ])

    # Result is a tuple containing the run record, extract the run_id (first field)
    case result do
      {run_id, _flow_slug, _status, _input, _output, _remaining_steps, _started_at, _completed_at,
       _failed_at} ->
        # Convert binary UUID to string format
        uuid_to_string(run_id)

      _ ->
        raise "Unexpected result from start_flow: #{inspect(result)}"
    end
  end

  defp uuid_to_string(binary) when byte_size(binary) == 16 do
    Ecto.UUID.load!(binary)
  end

  defp uuid_to_string(binary) when is_binary(binary) do
    # Already a string format
    binary
  end

  @doc """
  Gets the current status of a run.
  """
  def get_run_status(run_id) do
    %{rows: [[status]]} =
      PgFlow.TestRepo.query!("SELECT status FROM pgflow.runs WHERE run_id = $1", [
        ensure_uuid_binary(run_id)
      ])

    status
  end

  @doc """
  Gets the run record with status and output.
  """
  def get_run(run_id) do
    %{rows: [[status, output]]} =
      PgFlow.TestRepo.query!(
        "SELECT status, output FROM pgflow.runs WHERE run_id = $1",
        [ensure_uuid_binary(run_id)]
      )

    %{status: status, output: output}
  end

  @doc """
  Gets all step states for a run.
  """
  def get_step_states(run_id) do
    %{rows: rows} =
      PgFlow.TestRepo.query!(
        "SELECT step_slug, status FROM pgflow.step_states WHERE run_id = $1 ORDER BY step_slug",
        [ensure_uuid_binary(run_id)]
      )

    Enum.map(rows, fn [step_slug, status] ->
      %{step_slug: step_slug, status: status}
    end)
  end

  @doc """
  Gets all step tasks for a run.
  """
  def get_step_tasks(run_id) do
    %{rows: rows} =
      PgFlow.TestRepo.query!(
        "SELECT step_slug, status, output, task_index FROM pgflow.step_tasks WHERE run_id = $1 ORDER BY step_slug, task_index",
        [ensure_uuid_binary(run_id)]
      )

    Enum.map(rows, fn [step_slug, status, output, task_index] ->
      %{step_slug: step_slug, status: status, output: output, task_index: task_index}
    end)
  end

  # Ensure a run_id is in binary UUID format
  defp ensure_uuid_binary(run_id) when byte_size(run_id) == 16, do: run_id

  defp ensure_uuid_binary(run_id) when is_binary(run_id) do
    string_to_uuid(run_id)
  end

  @doc """
  Reads and starts tasks from the queue for processing.
  Uses the pgflow_tests.read_and_start helper.

  Returns a list of maps with task details.
  The step_task_record type has 7 fields:
  (flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input)
  """
  def read_and_start(flow_slug, opts \\ []) do
    vt = Keyword.get(opts, :vt, 1)
    qty = Keyword.get(opts, :qty, 1)

    %{rows: rows} =
      PgFlow.TestRepo.query!(
        "SELECT * FROM pgflow_tests.read_and_start($1, $2, $3)",
        [flow_slug, vt, qty]
      )

    # Parse rows into maps with named fields
    # step_task_record: (flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input)
    Enum.map(rows, fn row ->
      case row do
        # 7-element list format (current schema)
        [flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input] ->
          %{
            flow_slug: flow_slug,
            run_id: uuid_to_string(run_id),
            step_slug: step_slug,
            input: input,
            msg_id: msg_id,
            task_index: task_index,
            flow_input: flow_input
          }

        # 7-element tuple format
        {flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input} ->
          %{
            flow_slug: flow_slug,
            run_id: uuid_to_string(run_id),
            step_slug: step_slug,
            input: input,
            msg_id: msg_id,
            task_index: task_index,
            flow_input: flow_input
          }
      end
    end)
  end

  @doc """
  Polls and completes tasks for a flow.
  Uses the pgflow_tests.poll_and_complete helper.
  """
  def poll_and_complete(flow_slug, opts \\ []) do
    vt = Keyword.get(opts, :vt, 1)
    qty = Keyword.get(opts, :qty, 1)

    %{rows: rows} =
      PgFlow.TestRepo.query!(
        "SELECT * FROM pgflow_tests.poll_and_complete($1, $2, $3)",
        [flow_slug, vt, qty]
      )

    rows
  end

  @doc """
  Polls and fails tasks for a flow.
  Uses the pgflow_tests.poll_and_fail helper.
  """
  def poll_and_fail(flow_slug, opts \\ []) do
    vt = Keyword.get(opts, :vt, 1)
    qty = Keyword.get(opts, :qty, 1)

    %{rows: rows} =
      PgFlow.TestRepo.query!(
        "SELECT * FROM pgflow_tests.poll_and_fail($1, $2, $3)",
        [flow_slug, vt, qty]
      )

    rows
  end

  @doc """
  Polls a task and completes it with a specific output value.
  Useful for testing map flows where each task needs a different output.
  """
  def poll_and_complete_with_output(flow_slug, output, opts \\ []) do
    vt = Keyword.get(opts, :vt, 1)

    # First, read and start the task
    tasks = read_and_start(flow_slug, vt: vt, qty: 1)

    case tasks do
      [task | _] ->
        # Complete with the specified output
        complete_task(task.run_id, task.step_slug, task.task_index, output)

      [] ->
        {:error, :no_tasks}
    end
  end

  @doc """
  Ensures a worker is registered for a queue.
  Uses the pgflow_tests.ensure_worker helper.
  """
  def ensure_worker(queue_name, opts \\ []) do
    # Use default worker UUID or provided one
    worker_uuid_str =
      Keyword.get(opts, :worker_uuid, "11111111-1111-1111-1111-111111111111")

    function_name = Keyword.get(opts, :function_name, "test_worker")

    # Pass the UUID as a string and let PostgreSQL cast it
    %{rows: [[worker_id]]} =
      PgFlow.TestRepo.query!(
        "SELECT pgflow_tests.ensure_worker($1, $2, $3)",
        [queue_name, string_to_uuid(worker_uuid_str), function_name]
      )

    uuid_to_string(worker_id)
  end

  defp string_to_uuid(uuid_str) when is_binary(uuid_str) do
    # Use Ecto.UUID to convert string to 16-byte binary for Postgrex
    Ecto.UUID.dump!(uuid_str)
  end

  @doc """
  Waits for a run to reach a terminal state (completed or failed).
  """
  def wait_for_run_completion(run_id, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 5000)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, 100)
    max_attempts = div(timeout_ms, poll_interval_ms)

    wait_for_run_completion_loop(run_id, max_attempts, poll_interval_ms)
  end

  defp wait_for_run_completion_loop(_run_id, 0, _poll_interval_ms) do
    {:error, :timeout}
  end

  defp wait_for_run_completion_loop(run_id, attempts_left, poll_interval_ms) do
    case get_run_status(run_id) do
      status when status in ["completed", "failed"] ->
        {:ok, status}

      _ ->
        Process.sleep(poll_interval_ms)
        wait_for_run_completion_loop(run_id, attempts_left - 1, poll_interval_ms)
    end
  end

  # ============= Flow Creation with Options =============

  @doc """
  Creates a flow with specific retry configuration.

  ## Options
    * `:max_attempts` - Maximum retry attempts (default: 3)
    * `:base_delay` - Base delay in seconds for retries (default: 1)
    * `:timeout` - Task timeout in seconds (default: 60)

  ## Example

      create_flow_with_options("retry_flow", max_attempts: 3, base_delay: 2)
  """
  def create_flow_with_options(flow_slug, opts \\ []) do
    max_attempts = Keyword.get(opts, :max_attempts, 3)
    base_delay = Keyword.get(opts, :base_delay, 1)
    timeout = Keyword.get(opts, :timeout, 60)

    PgFlow.TestRepo.query!(
      "SELECT pgflow.create_flow($1, $2, $3, $4)",
      [flow_slug, max_attempts, base_delay, timeout]
    )
  end

  @doc """
  Adds a step with full options including start_delay.

  ## Options
    * `:deps` - List of dependency step slugs (default: [])
    * `:type` - Step type: "single" or "map" (default: "single")
    * `:start_delay` - Delay in seconds before task becomes visible (default: nil)

  ## Example

      add_step_with_options("flow", "delayed_step", deps: ["prev"], start_delay: 5)
  """
  def add_step_with_options(flow_slug, step_slug, opts \\ []) do
    deps = Keyword.get(opts, :deps, [])
    step_type = Keyword.get(opts, :type, "single")
    start_delay = Keyword.get(opts, :start_delay)

    deps_sql = if Enum.empty?(deps), do: "ARRAY[]::text[]", else: "$3::text[]"
    params = if Enum.empty?(deps), do: [flow_slug, step_slug], else: [flow_slug, step_slug, deps]

    # Build query with optional start_delay (7th positional param)
    query =
      if start_delay do
        "SELECT pgflow.add_step($1, $2, #{deps_sql}, null, null, null, $#{length(params) + 1}, '#{step_type}')"
      else
        "SELECT pgflow.add_step($1, $2, #{deps_sql}, null, null, null, null, '#{step_type}')"
      end

    params = if start_delay, do: params ++ [start_delay], else: params
    PgFlow.TestRepo.query!(query, params)
  end

  # ============= Task Operations =============

  @doc """
  Gets detailed task information including attempts_count.
  """
  def get_task_details(run_id, step_slug, task_index \\ 0) do
    %{rows: rows} =
      PgFlow.TestRepo.query!(
        """
        SELECT step_slug, status, attempts_count, error_message, output,
               queued_at, started_at, completed_at, failed_at, message_id
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
        """,
        [ensure_uuid_binary(run_id), step_slug, task_index]
      )

    case rows do
      [[step_slug, status, attempts, error, output, queued, started, completed, failed, msg_id]] ->
        %{
          step_slug: step_slug,
          status: status,
          attempts_count: attempts,
          error_message: error,
          output: output,
          queued_at: queued,
          started_at: started,
          completed_at: completed,
          failed_at: failed,
          message_id: msg_id
        }

      [] ->
        nil
    end
  end

  @doc """
  Directly fails a task with an error message.
  Returns the updated task record.
  """
  def fail_task(run_id, step_slug, task_index, error_message) do
    %{rows: rows} =
      PgFlow.TestRepo.query!(
        "SELECT * FROM pgflow.fail_task($1, $2, $3, $4)",
        [ensure_uuid_binary(run_id), step_slug, task_index, error_message]
      )

    rows
  end

  @doc """
  Directly completes a task with output.
  Returns the updated task record.
  """
  def complete_task(run_id, step_slug, task_index, output) do
    %{rows: rows} =
      PgFlow.TestRepo.query!(
        "SELECT * FROM pgflow.complete_task($1, $2, $3, cast($4 as text)::jsonb)",
        [ensure_uuid_binary(run_id), step_slug, task_index, Jason.encode!(output)]
      )

    rows
  end

  # ============= Message Queue Helpers =============

  @doc """
  Gets message timing information from pgmq for a specific step.
  Uses pgflow_tests.message_timing helper.

  Returns a list of maps with:
    * `:msg_id` - Message ID
    * `:read_ct` - Read count (number of times message was read)
    * `:enqueued_at` - When message was enqueued
    * `:vt` - Visibility time (when message becomes visible)
    * `:vt_offset_seconds` - Seconds from enqueued_at to vt
  """
  def get_message_timing(step_slug, queue_name) do
    %{rows: rows} =
      PgFlow.TestRepo.query!(
        "SELECT * FROM pgflow_tests.message_timing($1, $2)",
        [step_slug, queue_name]
      )

    Enum.map(rows, fn row ->
      # Row format: (msg_id, read_ct, enqueued_at, vt, message, vt_seconds)
      case row do
        [msg_id, read_ct, enqueued_at, vt, _message, vt_seconds] ->
          %{
            msg_id: msg_id,
            read_ct: read_ct,
            enqueued_at: enqueued_at,
            vt: vt,
            vt_offset_seconds: vt_seconds
          }

        {msg_id, read_ct, enqueued_at, vt, _message, vt_seconds} ->
          %{
            msg_id: msg_id,
            read_ct: read_ct,
            enqueued_at: enqueued_at,
            vt: vt,
            vt_offset_seconds: vt_seconds
          }
      end
    end)
  end

  @doc """
  Resets message visibility to make it immediately available.
  Useful for testing retry timing without waiting.
  """
  def reset_message_visibility(queue_name) do
    %{rows: [[count]]} =
      PgFlow.TestRepo.query!(
        "SELECT pgflow_tests.reset_message_visibility($1)",
        [queue_name]
      )

    count
  end

  # ============= Timing Assertions =============

  @doc """
  Calculates delays between timestamps in seconds.

  ## Example

      timestamps = [~U[2024-01-01 00:00:00Z], ~U[2024-01-01 00:00:02Z], ~U[2024-01-01 00:00:06Z]]
      calculate_delays(timestamps)
      # => [2.0, 4.0]
  """
  def calculate_delays(timestamps) when is_list(timestamps) do
    timestamps
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map(fn [t1, t2] ->
      DateTime.diff(t2, t1, :millisecond) / 1000
    end)
  end

  @doc """
  Asserts that actual delays match expected delays within tolerance.

  ## Example

      assert_delays_match([2.1, 4.05], [2, 4], 0.2)
      # passes if each delay is within ±0.2 seconds of expected
  """
  def assert_delays_match(actual, expected, tolerance \\ 0.2) do
    import ExUnit.Assertions

    assert length(actual) == length(expected),
           "Expected #{length(expected)} delays, got #{length(actual)}: #{inspect(actual)}"

    Enum.zip(actual, expected)
    |> Enum.with_index(1)
    |> Enum.each(fn {{actual_delay, expected_delay}, index} ->
      assert_in_delta actual_delay,
                      expected_delay,
                      tolerance,
                      "Delay ##{index} should be ~#{expected_delay}s, got #{actual_delay}s"
    end)
  end

  # ============= Map Flow Helpers =============

  @doc """
  Creates a flow with a root map step (processes array input).

  ## Example

      create_root_map_flow("array_flow", "process_item")
  """
  def create_root_map_flow(flow_slug, step_slug) do
    create_flow(flow_slug)
    add_step(flow_slug, step_slug, type: "map")
  end

  @doc """
  Creates a flow with mixed step types (single and map).

  ## Example

      create_mixed_flow("mixed", [
        %{slug: "generate", deps: [], type: "single"},
        %{slug: "process", deps: ["generate"], type: "map"},
        %{slug: "aggregate", deps: ["process"], type: "single"}
      ])
  """
  def create_mixed_flow(flow_slug, steps) do
    create_flow(flow_slug)

    for step <- steps do
      add_step(flow_slug, step.slug, deps: Map.get(step, :deps, []), type: step.type)
    end
  end

  @doc """
  Gets step tasks filtered by step_slug.
  """
  def get_step_tasks_for_step(run_id, step_slug) do
    %{rows: rows} =
      PgFlow.TestRepo.query!(
        """
        SELECT step_slug, status, output, task_index, attempts_count
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = $2
        ORDER BY task_index
        """,
        [ensure_uuid_binary(run_id), step_slug]
      )

    Enum.map(rows, fn [slug, status, output, index, attempts] ->
      %{
        step_slug: slug,
        status: status,
        output: output,
        task_index: index,
        attempts_count: attempts
      }
    end)
  end

  # ============= Start Delay Helpers =============

  @doc """
  Gets message visibility information for tasks in a queue.
  Returns list of maps with message_id, step_slug, vt (visibility time), and vt_offset_seconds.

  ## Example

      get_message_visibility("my_flow", run_id)
      # => [%{step_slug: "step1", vt_offset_seconds: 0.5}, %{step_slug: "step2", vt_offset_seconds: 300.2}]
  """
  def get_message_visibility(queue_name, run_id) do
    # Query the queue table to get visibility times
    # Join with step_tasks to get step_slug
    %{rows: rows} =
      PgFlow.TestRepo.query!(
        """
        SELECT
          st.step_slug,
          st.message_id,
          q.vt,
          EXTRACT(EPOCH FROM (q.vt - clock_timestamp())) as vt_offset_seconds
        FROM pgflow.step_tasks st
        JOIN pgmq.q_#{queue_name} q ON q.msg_id = st.message_id
        WHERE st.run_id = $1
        ORDER BY st.step_slug, st.task_index
        """,
        [ensure_uuid_binary(run_id)]
      )

    Enum.map(rows, fn [step_slug, message_id, vt, vt_offset] ->
      %{
        step_slug: step_slug,
        message_id: message_id,
        vt: vt,
        vt_offset_seconds: vt_offset |> Decimal.to_float()
      }
    end)
  end

  @doc """
  Checks if a message is currently visible (can be read).
  A message is visible if its vt (visibility time) is <= current time.
  """
  def message_visible?(queue_name, run_id, step_slug) do
    visibility = get_message_visibility(queue_name, run_id)
    msg = Enum.find(visibility, &(&1.step_slug == step_slug))

    case msg do
      nil -> false
      %{vt_offset_seconds: offset} -> offset <= 0
    end
  end

  @doc """
  Gets the visibility offset in seconds for a specific step's message.
  Positive value means message is delayed (not yet visible).
  Negative or zero means message is visible.
  """
  def get_step_visibility_offset(queue_name, run_id, step_slug) do
    visibility = get_message_visibility(queue_name, run_id)
    msg = Enum.find(visibility, &(&1.step_slug == step_slug))

    case msg do
      nil -> nil
      %{vt_offset_seconds: offset} -> offset
    end
  end

  # ============= Wait Helpers =============

  @doc """
  Generic wait helper that polls until condition is met or timeout.

  ## Example

      wait_until(fn -> get_task_details(run_id, "step").status == "completed" end,
        timeout_ms: 5000, poll_interval_ms: 100)
  """
  def wait_until(condition_fn, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 5000)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, 100)
    max_attempts = div(timeout_ms, poll_interval_ms)

    do_wait_until(condition_fn, max_attempts, poll_interval_ms)
  end

  defp do_wait_until(_condition_fn, 0, _poll_interval_ms) do
    {:error, :timeout}
  end

  defp do_wait_until(condition_fn, attempts_left, poll_interval_ms) do
    if condition_fn.() do
      :ok
    else
      Process.sleep(poll_interval_ms)
      do_wait_until(condition_fn, attempts_left - 1, poll_interval_ms)
    end
  end
end
