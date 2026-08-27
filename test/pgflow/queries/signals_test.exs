defmodule PgFlow.Queries.SignalsTest do
  @moduledoc """
  Integration tests for the atomic awaiting-signals transition. These call the
  shipped SQL, not a reimplementation.
  """
  use PgFlow.IntegrationCase, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Queries.{Flows, Signals, Workers}

  @moduletag timeout: 30_000
  @moduletag :integration

  defp repo, do: TestRepo

  defp compile_one_step_flow(flow_slug, step_slug) do
    create_flow(flow_slug)
    add_step(flow_slug, step_slug)
    flow_slug
  end

  defp start_started_task(flow_slug, input) do
    run_id = start_flow_run(flow_slug, input)
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(repo(), worker_id, flow_slug, "elixir:test")

    {:ok, messages} = Flows.read(repo(), flow_slug, 30, 10)
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    {:ok, _details} = Flows.start_tasks(repo(), flow_slug, msg_ids, worker_id)

    run_id
  end

  defp start_and_park_expired_task(flow_slug) do
    compile_one_step_flow(flow_slug, "approval")
    run_id = start_started_task(flow_slug, %{})
    task = get_task_details(run_id, "approval", 0)

    assert :parked =
             Signals.await_task_signal(
               repo(),
               run_id,
               "approval",
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
      WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
      """,
      [Ecto.UUID.dump!(run_id), "approval", 0]
    )

    assert %{status: "waiting", message_id: nil} = get_task_details(run_id, "approval", 0)

    run_id
  end

  defp park_current_task(run_id, step_slug, wait_for_seconds \\ nil) do
    task = get_task_details(run_id, step_slug, 0)

    Signals.await_task_signal(
      repo(),
      run_id,
      step_slug,
      0,
      task.attempts_count,
      task.message_id,
      wait_for_seconds,
      true
    )
  end

  defp independent_connection do
    opts =
      TestRepo.config()
      |> Keyword.take([:hostname, :port, :username, :password, :database, :socket_dir, :ssl])

    {:ok, connection} = Postgrex.start_link(opts)
    connection
  end

  defp expire_after_barrier(parent) do
    connection = independent_connection()
    send(parent, {:sweeper_ready, self()})

    receive do
      :expire -> :ok
    end

    %{rows: [[count]]} =
      Postgrex.query!(connection, "SELECT pgflow.expire_waiting_tasks(1)", [])

    GenServer.stop(connection)
    count
  end

  defp hold_run_lock(parent, run_id) do
    connection = independent_connection()

    result =
      Postgrex.transaction(connection, fn connection ->
        Postgrex.query!(connection, "SELECT 1 FROM pgflow.runs WHERE run_id = $1 FOR UPDATE", [
          Ecto.UUID.dump!(run_id)
        ])

        send(parent, {:run_lock_held, self()})

        receive do
          :release -> :released
        end
      end)

    GenServer.stop(connection)
    result
  end

  defp race_signal_or_park(parent, operation, run_id, task) do
    connection = independent_connection()
    %{rows: [[backend_pid]]} = Postgrex.query!(connection, "SELECT pg_backend_pid()", [])
    send(parent, {:race_query_started, self(), backend_pid})

    result =
      case operation do
        :signal ->
          %{rows: [[outcome]]} =
            Postgrex.query!(
              connection,
              "SELECT outcome FROM pgflow.signal_task($1, 'approval', 0, '{\"decision\":\"approved\"}'::jsonb)",
              [Ecto.UUID.dump!(run_id)]
            )

          {:signal, outcome}

        :park ->
          %{rows: [[outcome, payload]]} =
            Postgrex.query!(
              connection,
              "SELECT outcome, payload FROM pgflow.await_task_signal($1, 'approval', 0, $2, $3, NULL, true)",
              [Ecto.UUID.dump!(run_id), task.attempts_count, task.message_id]
            )

          {:park, outcome, payload}
      end

    GenServer.stop(connection)
    result
  end

  defp backend_blocked?(backend_pid) do
    %{rows: [[blocked?]]} =
      TestRepo.query!("SELECT cardinality(pg_blocking_pids($1)) > 0", [backend_pid])

    blocked?
  end

  defp await_requeued_task(run_id, flow_slug) do
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(repo(), worker_id, flow_slug, "elixir:test")
    {:ok, messages} = Flows.read(repo(), flow_slug, 30, 1)
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    {:ok, _details} = Flows.start_tasks(repo(), flow_slug, msg_ids, worker_id)
    task = get_task_details(run_id, "approval", 0)

    Signals.await_task_signal(
      repo(),
      run_id,
      "approval",
      0,
      task.attempts_count,
      task.message_id,
      nil,
      false
    )
  end

  defp signal_row(run_id, step_slug, task_index) do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT payload, timed_out, claimed_at
        FROM pgflow.task_signals
        WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
        """,
        [Ecto.UUID.dump!(run_id), step_slug, task_index]
      )

    rows
  end

  test "a signal committed before the final park is returned without parking" do
    compile_one_step_flow("signal_won_before_park", "approval")
    run_id = start_started_task("signal_won_before_park", %{})
    task = get_task_details(run_id, "approval", 0)

    assert {:ok, :buffered} =
             Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "approved"})

    assert {:ok, %{"decision" => "approved"}} =
             Signals.await_task_signal(
               repo(),
               run_id,
               "approval",
               0,
               task.attempts_count,
               task.message_id,
               nil,
               true
             )

    assert get_task_details(run_id, "approval", 0).status == "started"
  end

  test "a stale attempt cannot consume or park the current attempt" do
    compile_one_step_flow("stale_attempt_fence", "approval")
    run_id = start_started_task("stale_attempt_fence", %{})
    attempt_one = get_task_details(run_id, "approval", 0)

    TestRepo.query!(
      "UPDATE pgflow.step_tasks SET attempts_count = attempts_count + 1 WHERE run_id = $1 AND step_slug = $2",
      [Ecto.UUID.dump!(run_id), "approval"]
    )

    assert :stale =
             Signals.await_task_signal(
               repo(),
               run_id,
               "approval",
               0,
               attempt_one.attempts_count,
               attempt_one.message_id,
               nil,
               true
             )

    assert get_task_details(run_id, "approval", 0).status == "started"
  end

  test "a claimed payload is immutable and reports already_delivered" do
    compile_one_step_flow("claimed_signal_immutable", "approval")
    run_id = start_started_task("claimed_signal_immutable", %{})
    task = get_task_details(run_id, "approval", 0)

    assert {:ok, :buffered} =
             Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "approved"})

    assert {:ok, %{"decision" => "approved"}} =
             Signals.await_task_signal(
               repo(),
               run_id,
               "approval",
               0,
               task.attempts_count,
               task.message_id,
               nil,
               false
             )

    assert {:ok, :already_delivered} =
             Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "rejected"})

    assert [[%{"decision" => "approved"}, false, %DateTime{}]] =
             signal_row(run_id, "approval", 0)
  end

  test "a signal after the persisted deadline yields expired and cannot replace timeout" do
    run_id = start_and_park_expired_task("strict_deadline_signal")

    assert {:ok, :expired} =
             Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "approved"})

    assert :timeout = await_requeued_task(run_id, "strict_deadline_signal")
  end

  test "an on-time unclaimed payload survives a late duplicate after the deadline" do
    compile_one_step_flow("on_time_payload_wins", "approval")
    run_id = start_started_task("on_time_payload_wins", %{})
    original_payload = %{"decision" => "approved"}

    assert :parked = park_current_task(run_id, "approval", 60)

    assert {:ok, :requeued} =
             Signals.signal_task(repo(), run_id, "approval", 0, original_payload)

    assert [[^original_payload, false, nil]] = signal_row(run_id, "approval", 0)

    TestRepo.query!(
      """
      UPDATE pgflow.task_signals
      SET wait_deadline_at = now() - interval '1 second'
      WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
      """,
      [Ecto.UUID.dump!(run_id), "approval", 0]
    )

    assert {:ok, :expired} =
             Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "rejected"})

    assert [[^original_payload, false, nil]] = signal_row(run_id, "approval", 0)
    assert {:ok, ^original_payload} = await_requeued_task(run_id, "on_time_payload_wins")
  end

  test "a signal cannot clear a timeout already queued by recovery" do
    run_id = start_and_park_expired_task("timeout_marker_wins")
    assert {:ok, 1} = Signals.expire_waiting_tasks(repo(), 100)

    assert {:ok, :expired} =
             Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "approved"})

    assert [[nil, true, nil]] = signal_row(run_id, "approval", 0)
    assert :timeout = await_requeued_task(run_id, "timeout_marker_wins")
    assert [[nil, true, %DateTime{}]] = signal_row(run_id, "approval", 0)
  end

  test "a signal after a timeout was claimed remains expired" do
    run_id = start_and_park_expired_task("claimed_timeout_stays_expired")
    assert {:ok, 1} = Signals.expire_waiting_tasks(repo(), 100)
    assert :timeout = await_requeued_task(run_id, "claimed_timeout_stays_expired")

    assert {:ok, :expired} =
             Signals.signal_task(repo(), run_id, "approval", 0, %{"decision" => "approved"})

    assert [[nil, true, %DateTime{}]] = signal_row(run_id, "approval", 0)
  end

  test "a failed run settles every active sibling state, task, message, and signal" do
    flow_slug = "failed_run_sibling_cleanup"
    create_flow(flow_slug)
    add_step(flow_slug, "waiting")
    add_step(flow_slug, "active")
    add_step(flow_slug, "queued")
    add_step_with_retry_options(flow_slug, "failure", max_attempts: 1)
    add_step(flow_slug, "created", deps: ["active"])
    run_id = start_flow_run(flow_slug, %{})

    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(repo(), worker_id, flow_slug, "elixir:test")

    %{rows: task_rows} =
      TestRepo.query!(
        "SELECT step_slug, message_id FROM pgflow.step_tasks WHERE run_id = $1",
        [Ecto.UUID.dump!(run_id)]
      )

    message_ids =
      task_rows
      |> Map.new(fn [step_slug, message_id] -> {step_slug, message_id} end)
      |> Map.take(["waiting", "active", "failure"])
      |> Map.values()

    {:ok, _details} = Flows.start_tasks(repo(), flow_slug, message_ids, worker_id)
    assert :parked = park_current_task(run_id, "waiting", 60)
    assert %{status: "started"} = get_task_details(run_id, "active", 0)
    assert %{status: "queued"} = get_task_details(run_id, "queued", 0)

    TestRepo.query!(
      "SELECT pgflow.fail_task($1, 'failure', 0, 'terminal sibling failed')",
      [Ecto.UUID.dump!(run_id)]
    )

    assert get_run_status(run_id) == "failed"

    assert %{rows: [[0]]} =
             TestRepo.query!(
               "SELECT count(*) FROM pgflow.step_states WHERE run_id = $1 AND status IN ('created', 'started')",
               [Ecto.UUID.dump!(run_id)]
             )

    assert %{rows: [[0]]} =
             TestRepo.query!(
               "SELECT count(*) FROM pgflow.step_tasks WHERE run_id = $1 AND status IN ('queued', 'started', 'waiting')",
               [Ecto.UUID.dump!(run_id)]
             )

    assert %{rows: [[0]]} =
             TestRepo.query!(
               "SELECT count(*) FROM pgmq.q_failed_run_sibling_cleanup WHERE message->>'run_id' = $1",
               [run_id]
             )

    assert signal_row(run_id, "waiting", 0) == []
  end

  test "a skipped step only settles waiting siblings added by V05" do
    flow_slug = "skip_preserves_v04_task_rows"
    create_flow(flow_slug)

    add_conditional_step(flow_slug, "items",
      type: "map",
      max_attempts: 1,
      when_exhausted: "skip"
    )

    run_id = start_flow_run(flow_slug, [1, 2, 3, 4])
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(repo(), worker_id, flow_slug, "elixir:test")

    %{rows: task_rows} =
      TestRepo.query!(
        "SELECT task_index, message_id FROM pgflow.step_tasks WHERE run_id = $1 ORDER BY task_index",
        [Ecto.UUID.dump!(run_id)]
      )

    message_ids =
      task_rows
      |> Map.new(fn [task_index, message_id] -> {task_index, message_id} end)
      |> Map.take([0, 2, 3])
      |> Map.values()

    {:ok, _details} = Flows.start_tasks(repo(), flow_slug, message_ids, worker_id)
    waiting_task = get_task_details(run_id, "items", 3)

    assert :parked =
             Signals.await_task_signal(
               repo(),
               run_id,
               "items",
               3,
               waiting_task.attempts_count,
               waiting_task.message_id,
               60,
               true
             )

    TestRepo.query!(
      "SELECT pgflow.fail_task($1, 'items', 0, 'skip the map step')",
      [Ecto.UUID.dump!(run_id)]
    )

    assert get_run_status(run_id) == "completed"
    assert [%{status: "skipped"}] = get_step_states(run_id)

    assert %{rows: [[0, "failed"], [1, "queued"], [2, "started"], [3, "failed"]]} =
             TestRepo.query!(
               "SELECT task_index, status FROM pgflow.step_tasks WHERE run_id = $1 ORDER BY task_index",
               [Ecto.UUID.dump!(run_id)]
             )

    assert %{rows: [[0]]} =
             TestRepo.query!("SELECT count(*) FROM pgmq.q_skip_preserves_v04_task_rows")

    assert signal_row(run_id, "items", 3) == []
  end

  test "waiting-task recovery expires at most the requested batch size" do
    run_ids =
      for index <- 1..3 do
        start_and_park_expired_task("bounded_waiting_recovery_#{index}")
      end

    assert {:ok, 2} = Signals.expire_waiting_tasks(repo(), 2)

    %{rows: [[queued, waiting]]} =
      TestRepo.query!(
        """
        SELECT
          count(*) FILTER (WHERE status = 'queued'),
          count(*) FILTER (WHERE status = 'waiting')
        FROM pgflow.step_tasks
        WHERE run_id = ANY($1)
        """,
        [Enum.map(run_ids, &Ecto.UUID.dump!/1)]
      )

    assert {queued, waiting} == {2, 1}
    assert {:ok, 1} = Signals.expire_waiting_tasks(repo(), 2)
  end

  test "duplicate sweepers count only the transition they actually win" do
    Sandbox.unboxed_run(repo(), fn ->
      TestRepo.query!("SELECT pgflow_tests.reset_db()")
      run_id = start_and_park_expired_task("duplicate_waiting_sweepers")
      parent = self()
      first = Task.async(fn -> expire_after_barrier(parent) end)
      second = Task.async(fn -> expire_after_barrier(parent) end)

      assert_receive {:sweeper_ready, first_pid}, 5_000
      assert_receive {:sweeper_ready, second_pid}, 5_000
      send(first_pid, :expire)
      send(second_pid, :expire)

      assert Enum.sort([Task.await(first, 5_000), Task.await(second, 5_000)]) == [0, 1]
      assert %{status: "queued"} = get_task_details(run_id, "approval", 0)
      assert [[nil, true, nil]] = signal_row(run_id, "approval", 0)
    end)
  end

  test "a signal racing the final park has exactly one lossless serialization" do
    Sandbox.unboxed_run(repo(), fn ->
      TestRepo.query!("SELECT pgflow_tests.reset_db()")
      compile_one_step_flow("signal_final_park_race", "approval")
      run_id = start_started_task("signal_final_park_race", %{})
      task = get_task_details(run_id, "approval", 0)
      parent = self()
      blocker = Task.async(fn -> hold_run_lock(parent, run_id) end)
      assert_receive {:run_lock_held, blocker_pid}, 5_000

      signal = Task.async(fn -> race_signal_or_park(parent, :signal, run_id, task) end)
      park = Task.async(fn -> race_signal_or_park(parent, :park, run_id, task) end)
      assert_receive {:race_query_started, _signal_pid, signal_backend_pid}, 5_000
      assert_receive {:race_query_started, _park_pid, park_backend_pid}, 5_000
      assert :ok = wait_until(fn -> backend_blocked?(signal_backend_pid) end)
      assert :ok = wait_until(fn -> backend_blocked?(park_backend_pid) end)

      send(blocker_pid, :release)
      assert {:ok, :released} = Task.await(blocker, 5_000)

      outcomes = MapSet.new([Task.await(signal, 5_000), Task.await(park, 5_000)])

      assert outcomes in [
               MapSet.new([
                 {:signal, "buffered"},
                 {:park, "signal", %{"decision" => "approved"}}
               ]),
               MapSet.new([{:signal, "requeued"}, {:park, "parked", nil}])
             ]

      assert get_task_details(run_id, "approval", 0).status in ["queued", "started"]
      assert {:ok, []} = Signals.list_waiting_tasks(repo(), run_id)

      assert [[%{"decision" => "approved"}, false, claimed_at]] =
               signal_row(run_id, "approval", 0)

      assert is_nil(claimed_at) or match?(%DateTime{}, claimed_at)

      assert %{rows: [[1]]} =
               TestRepo.query!(
                 "SELECT count(*) FROM pgmq.q_signal_final_park_race WHERE message->>'run_id' = $1",
                 [run_id]
               )
    end)
  end

  test "direct SQL rejects null, JSON null, and scalar signal payloads before target lookup" do
    missing_run_id = Ecto.UUID.generate() |> Ecto.UUID.dump!()

    for {payload_sql, message} <- [
          {"NULL::jsonb", "signal payload must be a JSON object or array"},
          {"'null'::jsonb", "signal payload must be a JSON object or array"},
          {"'42'::jsonb", "signal payload must be a JSON object or array"},
          {"'\"scalar\"'::jsonb", "signal payload must be a JSON object or array"}
        ] do
      error =
        assert_raise Postgrex.Error, fn ->
          TestRepo.query!(
            "SELECT outcome FROM pgflow.signal_task($1, 'approval', 0, #{payload_sql})",
            [missing_run_id]
          )
        end

      assert Exception.message(error) =~ message
    end

    assert signal_row(Ecto.UUID.load!(missing_run_id), "approval", 0) == []
  end

  test "direct SQL enforces the one MiB ceiling and accepts a normal and exact-boundary payload" do
    compile_one_step_flow("direct_sql_payload_ceiling", "approval")
    run_id = start_started_task("direct_sql_payload_ceiling", %{})
    dumped_run_id = Ecto.UUID.dump!(run_id)

    assert %{rows: [["buffered"]]} =
             TestRepo.query!(
               "SELECT outcome FROM pgflow.signal_task($1, 'approval', 0, '{\"ok\":true}'::jsonb)",
               [dumped_run_id]
             )

    error =
      assert_raise Postgrex.Error, fn ->
        TestRepo.query!(
          """
          SELECT outcome
          FROM pgflow.signal_task(
            $1,
            'approval',
            0,
            jsonb_build_object('data', repeat('x', 1048576))
          )
          """,
          [dumped_run_id]
        )
      end

    assert Exception.message(error) =~ "signal payload exceeds the 1048576-byte database limit"

    assert %{rows: [["buffered", 1_048_576]]} =
             TestRepo.query!(
               """
               WITH boundary AS (
                 SELECT payload
                 FROM (
                   SELECT jsonb_build_object('data', repeat('x', n)) AS payload
                   FROM generate_series(1048400, 1048576) AS n
                 ) candidates
                 WHERE pg_column_size(payload) = 1048576
                 LIMIT 1
               )
               SELECT delivered.outcome, pg_column_size(boundary.payload)
               FROM boundary
               CROSS JOIN LATERAL pgflow.signal_task($1, 'approval', 0, boundary.payload) delivered
               """,
               [dumped_run_id]
             )
  end

  test "missing run and step targets insert no signal rows" do
    missing_run_id = Ecto.UUID.generate()

    assert {:ok, :missing} =
             Signals.signal_task(repo(), missing_run_id, "approval", 0, %{"decision" => "late"})

    assert signal_row(missing_run_id, "approval", 0) == []

    compile_one_step_flow("missing_signal_step", "approval")
    run_id = start_flow_run("missing_signal_step", %{})

    assert {:ok, :missing} =
             Signals.signal_task(repo(), run_id, "unknown", 0, %{"decision" => "late"})

    assert signal_row(run_id, "unknown", 0) == []
  end

  test "waiting-task discovery returns only addressing and timing metadata" do
    compile_one_step_flow("waiting_task_discovery", "approval")
    run_id = start_started_task("waiting_task_discovery", %{})
    assert :parked = park_current_task(run_id, "approval")

    assert {:ok, [waiting_task]} = Signals.list_waiting_tasks(repo(), run_id)

    assert %{
             step_slug: "approval",
             task_index: 0,
             wait_deadline_at: nil,
             waiting_since: %DateTime{}
           } = waiting_task

    assert Enum.sort(Map.keys(waiting_task)) ==
             Enum.sort([:step_slug, :task_index, :wait_deadline_at, :waiting_since])
  end

  test "waiting-task discovery orders multiple tasks by address" do
    flow_slug = "ordered_waiting_task_discovery"
    create_flow(flow_slug)
    add_step(flow_slug, "zeta")
    add_step(flow_slug, "alpha")
    run_id = start_flow_run(flow_slug, %{})
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(repo(), worker_id, flow_slug, "elixir:test")
    {:ok, messages} = Flows.read(repo(), flow_slug, 30, 10)
    message_ids = Enum.map(messages, fn [message_id | _] -> message_id end)
    {:ok, _} = Flows.start_tasks(repo(), flow_slug, message_ids, worker_id)

    assert :parked = park_current_task(run_id, "zeta")
    assert :parked = park_current_task(run_id, "alpha")
    assert {:ok, waiting_tasks} = Signals.list_waiting_tasks(repo(), run_id)

    assert Enum.map(waiting_tasks, &{&1.step_slug, &1.task_index}) ==
             [{"alpha", 0}, {"zeta", 0}]
  end
end

defmodule PgFlow.Queries.SignalsSourceContractTest do
  use ExUnit.Case, async: true

  @moduletag :source_contract

  @sql_path "priv/pgflow_helpers/sql/versions/v05/v05_up.sql"
  @query_path "lib/pgflow/queries/signals.ex"
  @client_path "lib/pgflow/client.ex"
  @public_path "lib/pgflow.ex"
  @context_integration_path "test/pgflow/context_await_signal_test.exs"
  @await_integration_path "test/pgflow/await_signals_test.exs"
  @down_path "priv/pgflow_helpers/sql/versions/v05/v05_down.sql"
  @reset_path "test/support/db/test_helpers.sql"
  @recovery_path "lib/pgflow/worker/waiting_task_recovery.ex"

  test "await transition is attempt-fenced, parent-first, and decoded into typed outcomes" do
    sql = File.read!(@sql_path)
    query_source = File.read!(@query_path)

    assert sql =~ "claimed_at timestamptz NULL"
    assert sql =~ "CONSTRAINT task_signals_task_index_nonnegative"
    assert sql =~ "CONSTRAINT task_signals_payload_shape"
    assert sql =~ "CONSTRAINT task_signals_step_state_fkey"
    refute sql =~ "DELETE FROM pgflow.task_signals ts"

    assert {await_offset, _length} =
             :binary.match(sql, "CREATE OR REPLACE FUNCTION $SCHEMA$.await_task_signal(")

    await_sql =
      sql
      |> binary_part(await_offset, byte_size(sql) - await_offset)
      |> String.split("\n--SPLIT--\n", parts: 2)
      |> hd()

    assert {run_lock_offset, _length} = :binary.match(await_sql, "FROM pgflow.runs")
    assert {step_lock_offset, _length} = :binary.match(await_sql, "FROM pgflow.step_states")
    assert {task_lock_offset, _length} = :binary.match(await_sql, "FROM pgflow.step_tasks")
    assert {signal_lock_offset, _length} = :binary.match(await_sql, "FROM pgflow.task_signals")
    assert run_lock_offset < step_lock_offset
    assert step_lock_offset < task_lock_offset
    assert task_lock_offset < signal_lock_offset

    for {from_offset, to_offset} <- [
          {run_lock_offset, step_lock_offset},
          {step_lock_offset, task_lock_offset},
          {task_lock_offset, signal_lock_offset}
        ] do
      assert await_sql
             |> binary_part(from_offset, to_offset - from_offset)
             |> then(&(&1 =~ "FOR UPDATE"))
    end

    assert await_sql
           |> binary_part(signal_lock_offset, byte_size(await_sql) - signal_lock_offset)
           |> then(&(&1 =~ "FOR UPDATE"))

    assert {terminal_guard_offset, _length} =
             :binary.match(await_sql, "v_run.status <> 'started'")

    assert signal_lock_offset < terminal_guard_offset
    assert await_sql =~ "v_task.attempts_count <> p_expected_attempt"
    assert await_sql =~ "v_task.message_id IS DISTINCT FROM p_expected_message_id"

    assert query_source =~ "def await_task_signal("

    assert query_source =~
             ~S|def decode_await_result({:ok, %{rows: [["signal", payload]]}}), do: {:ok, payload}|

    assert query_source =~
             ~S|def decode_await_result({:ok, %{rows: [["stale", nil]]}}), do: :stale|
  end

  test "signal delivery is immutable, typed, and cleaned at terminal boundaries" do
    sql = File.read!(@sql_path)
    query_source = File.read!(@query_path)
    client_source = File.read!(@client_path)
    down = File.read!(@down_path)
    reset = File.read!(@reset_path)

    assert {signal_offset, _length} =
             :binary.match(sql, "CREATE OR REPLACE FUNCTION $SCHEMA$.signal_task(")

    signal_sql =
      sql
      |> binary_part(signal_offset, byte_size(sql) - signal_offset)
      |> String.split("\n--SPLIT--\n", parts: 2)
      |> hd()

    assert signal_sql =~ "RETURNS TABLE(outcome text)"
    assert signal_sql =~ "SECURITY INVOKER"
    assert {task_lock_offset, _length} = :binary.match(signal_sql, "FROM pgflow.step_tasks")
    assert {signal_lock_offset, _length} = :binary.match(signal_sql, "FROM pgflow.task_signals")
    assert {run_lock_offset, _length} = :binary.match(signal_sql, "FROM pgflow.runs")
    assert {step_lock_offset, _length} = :binary.match(signal_sql, "FROM pgflow.step_states")

    assert {claimed_guard_offset, _length} =
             :binary.match(signal_sql, "v_signal.claimed_at IS NOT NULL")

    assert {payload_write_offset, _length} =
             :binary.match(signal_sql, "INSERT INTO pgflow.task_signals")

    assert run_lock_offset < step_lock_offset
    assert step_lock_offset < task_lock_offset
    assert task_lock_offset < signal_lock_offset

    assert signal_sql
           |> binary_part(run_lock_offset, step_lock_offset - run_lock_offset)
           |> then(&(&1 =~ "FOR UPDATE"))

    assert signal_sql
           |> binary_part(step_lock_offset, task_lock_offset - step_lock_offset)
           |> then(&(&1 =~ "FOR UPDATE"))

    assert signal_sql
           |> binary_part(task_lock_offset, signal_lock_offset - task_lock_offset)
           |> then(&(&1 =~ "FOR UPDATE"))

    assert signal_lock_offset < claimed_guard_offset
    assert claimed_guard_offset < payload_write_offset

    for outcome <- ~w(buffered requeued already_delivered expired terminal missing) do
      assert signal_sql =~ "RETURN QUERY SELECT '#{outcome}'::text"
    end

    assert sql =~ "CREATE OR REPLACE FUNCTION $SCHEMA$.cleanup_terminal_step_signals()"
    assert sql =~ "CREATE TRIGGER cleanup_terminal_step_signals"
    assert sql =~ "CREATE OR REPLACE FUNCTION $SCHEMA$.cleanup_terminal_run_signals()"
    assert sql =~ "CREATE TRIGGER cleanup_terminal_run_signals"

    assert {step_cleanup_offset, _length} =
             :binary.match(
               sql,
               "CREATE OR REPLACE FUNCTION $SCHEMA$.cleanup_terminal_step_signals()"
             )

    step_cleanup_sql =
      sql
      |> binary_part(step_cleanup_offset, byte_size(sql) - step_cleanup_offset)
      |> String.split("\n--SPLIT--\n", parts: 2)
      |> hd()

    assert step_cleanup_sql =~ "AND status = 'waiting'"
    refute step_cleanup_sql =~ "status IN ('queued', 'waiting')"
    refute step_cleanup_sql =~ "PERFORM pgmq.archive"

    assert {run_cleanup_offset, _length} =
             :binary.match(
               sql,
               "CREATE OR REPLACE FUNCTION $SCHEMA$.cleanup_terminal_run_signals()"
             )

    run_cleanup_sql =
      sql
      |> binary_part(run_cleanup_offset, byte_size(sql) - run_cleanup_offset)
      |> String.split("\n--SPLIT--\n", parts: 2)
      |> hd()

    assert run_cleanup_sql =~ "UPDATE pgflow.step_states"
    assert run_cleanup_sql =~ "status IN ('created', 'started')"
    assert run_cleanup_sql =~ "status IN ('queued', 'started', 'waiting')"
    assert run_cleanup_sql =~ "ORDER BY step_slug, task_index"
    assert run_cleanup_sql =~ "PERFORM pgmq.archive(NEW.flow_slug, v_message_ids)"
    assert run_cleanup_sql =~ "AND status = 'waiting'"
    assert run_cleanup_sql =~ "message_id = NULL"
    assert run_cleanup_sql =~ "DELETE FROM pgflow.task_signals"

    assert down =~ "DROP TRIGGER IF EXISTS cleanup_terminal_step_signals"
    assert down =~ "DROP FUNCTION IF EXISTS $SCHEMA$.cleanup_terminal_step_signals()"
    assert down =~ "DROP TRIGGER IF EXISTS cleanup_terminal_run_signals"
    assert down =~ "DROP FUNCTION IF EXISTS $SCHEMA$.cleanup_terminal_run_signals()"

    assert {signal_reset_offset, _length} =
             :binary.match(reset, "DELETE FROM pgflow.task_signals;")

    assert {task_reset_offset, _length} = :binary.match(reset, "DELETE FROM pgflow.step_tasks;")
    assert signal_reset_offset < task_reset_offset
    assert query_source =~ "@type signal_outcome ::"

    for outcome <- ~w(buffered requeued already_delivered expired terminal missing) do
      assert query_source =~ "[\"#{outcome}\"]"
    end

    public_source = File.read!(@public_path)

    assert client_source =~ "@type signal_outcome ::"
    assert client_source =~ "with {:ok, _uuid} <- Ecto.UUID.cast(run_id),"

    assert client_source =~
             "Signals.signal_task(repo, run_id, to_string(step_slug), task_index, payload)"

    refute client_source =~ "PgFlow.signal no-op:"
    refute client_source =~ "Fire-and-forget: always returns `:ok`."

    assert public_source =~ "{:ok, signal_outcome()} | {:error, term()}"
    assert public_source =~ "defdelegate signal(run_id, step_slug, payload), to: Client"
  end

  test "direct SQL signal payloads are validated before target lookup and bounded at one MiB" do
    sql = File.read!(@sql_path)

    assert {signal_offset, _length} =
             :binary.match(sql, "CREATE OR REPLACE FUNCTION $SCHEMA$.signal_task(")

    signal_sql =
      sql
      |> binary_part(signal_offset, byte_size(sql) - signal_offset)
      |> String.split("\n--SPLIT--\n", parts: 2)
      |> hd()

    assert {shape_guard_offset, _length} = :binary.match(signal_sql, "p_payload IS NULL")
    assert signal_sql =~ "jsonb_typeof(p_payload) NOT IN ('object', 'array')"
    assert signal_sql =~ "signal payload must be a JSON object or array"

    assert {size_guard_offset, _length} =
             :binary.match(signal_sql, "pg_column_size(p_payload) > 1048576")

    assert signal_sql =~ "signal payload exceeds the 1048576-byte database limit"
    assert {run_lock_offset, _length} = :binary.match(signal_sql, "FROM pgflow.runs")
    assert shape_guard_offset < size_guard_offset
    assert size_guard_offset < run_lock_offset
  end

  test "only the atomic await protocol is installed and callable functions require explicit grants" do
    sql = File.read!(@sql_path)
    query_source = File.read!(@query_path)

    refute sql =~ "CREATE OR REPLACE FUNCTION $SCHEMA$.park_waiting_task("
    refute sql =~ "CREATE OR REPLACE FUNCTION $SCHEMA$.consume_task_signal("
    refute query_source =~ "def park_waiting_task("
    refute query_source =~ "def consume_task_signal("

    assert sql =~ "SECURITY INVOKER"

    assert sql =~
             "REVOKE EXECUTE ON FUNCTION $SCHEMA$.await_task_signal(uuid, text, integer, integer, bigint, bigint, boolean) FROM PUBLIC"

    assert sql =~
             "REVOKE EXECUTE ON FUNCTION $SCHEMA$.signal_task(uuid, text, integer, jsonb) FROM PUBLIC"

    assert sql =~ "REVOKE EXECUTE ON FUNCTION $SCHEMA$.expire_waiting_tasks(integer) FROM PUBLIC"

    assert sql =~
             "REVOKE EXECUTE ON FUNCTION $SCHEMA$.cleanup_terminal_step_signals() FROM PUBLIC"

    assert sql =~ "REVOKE EXECUTE ON FUNCTION $SCHEMA$.cleanup_terminal_run_signals() FROM PUBLIC"
  end

  test "waiting-task discovery exposes only address and timing fields through the public API" do
    query_source = File.read!(@query_path)
    client_source = File.read!(@client_path)
    public_source = File.read!(@public_path)

    assert query_source =~ "@type waiting_task ::"
    assert query_source =~ "def list_waiting_tasks(repo, run_id) do"

    assert query_source =~
             "SELECT st.step_slug, st.task_index, ts.wait_deadline_at, ts.inserted_at"

    refute query_source =~
             "SELECT st.step_slug, st.task_index, ts.wait_deadline_at, ts.inserted_at, ts.payload"

    assert query_source =~ "waiting_since: inserted_at"
    assert query_source =~ "ORDER BY st.step_slug, st.task_index"
    assert client_source =~ "def get_waiting_tasks(run_id) do"
    assert client_source =~ "Signals.list_waiting_tasks(repo, run_id)"
    assert public_source =~ "defdelegate get_waiting_tasks(run_id), to: Client"
  end

  test "public signal example handles accepted and non-delivery outcomes without a race-prone match" do
    public_source = File.read!(@public_path)

    refute public_source =~
             ~s|{:ok, :requeued} = PgFlow.signal(run_id, :approval, %{"decision" => "approved"})|

    assert public_source =~
             ~s|case PgFlow.signal(run_id, :approval, %{"decision" => "approved"}) do|

    assert public_source =~
             "{:ok, outcome} when outcome in [:buffered, :requeued]"

    assert public_source =~ ":already_delivered"

    assert public_source =~
             "{:ok, outcome} when outcome in [:expired, :terminal, :missing]"

    assert public_source =~ "{:error, reason} -> {:error, reason}"
  end

  test "integration callers assert exact typed public signal outcomes" do
    for path <- [@context_integration_path, @await_integration_path] do
      source = File.read!(path)

      refute Regex.match?(~r/assert\s+:ok\s*=\s*PgFlow\.signal/, source)
      refute Regex.match?(~r/assert\s+\{:ok,\s*_[^}]*\}\s*=\s*PgFlow\.signal/, source)
    end
  end

  test "strict deadlines and multi-node recovery are immutable, ordered, and bounded" do
    sql = File.read!(@sql_path)
    query_source = File.read!(@query_path)
    recovery_source = File.read!(@recovery_path)

    assert sql =~ "CREATE INDEX task_signals_unresolved_deadline_idx"
    assert sql =~ "ON $SCHEMA$.task_signals (wait_deadline_at)"
    assert sql =~ "wait_deadline_at IS NOT NULL"
    assert sql =~ "timed_out = false"
    assert sql =~ "payload IS NULL"

    assert {signal_offset, _length} =
             :binary.match(sql, "CREATE OR REPLACE FUNCTION $SCHEMA$.signal_task(")

    signal_sql =
      sql
      |> binary_part(signal_offset, byte_size(sql) - signal_offset)
      |> String.split("\n--SPLIT--\n", parts: 2)
      |> hd()

    assert {deadline_guard_offset, _length} =
             :binary.match(signal_sql, "v_signal.wait_deadline_at <= now()")

    assert {payload_write_offset, _length} =
             :binary.match(signal_sql, "INSERT INTO pgflow.task_signals")

    assert deadline_guard_offset < payload_write_offset
    assert signal_sql =~ "IF v_signal.timed_out"
    assert signal_sql =~ "SET timed_out = true"
    refute signal_sql =~ "timed_out = false"

    assert {expire_offset, _length} =
             :binary.match(
               sql,
               "CREATE OR REPLACE FUNCTION $SCHEMA$.expire_waiting_tasks(p_limit integer)"
             )

    expire_sql =
      sql
      |> binary_part(expire_offset, byte_size(sql) - expire_offset)
      |> String.split("\n--SPLIT--\n", parts: 2)
      |> hd()

    assert expire_sql =~ "SECURITY INVOKER"

    assert {run_table_lock_offset, _length} =
             :binary.match(expire_sql, "LOCK TABLE pgflow.runs IN ROW SHARE MODE")

    assert {step_table_lock_offset, _length} =
             :binary.match(expire_sql, "LOCK TABLE pgflow.step_states IN ROW SHARE MODE")

    assert {task_table_lock_offset, _length} =
             :binary.match(expire_sql, "LOCK TABLE pgflow.step_tasks IN ROW SHARE MODE")

    assert {signal_table_lock_offset, _length} =
             :binary.match(expire_sql, "LOCK TABLE pgflow.task_signals IN ROW SHARE MODE")

    assert run_table_lock_offset < step_table_lock_offset
    assert step_table_lock_offset < task_table_lock_offset
    assert task_table_lock_offset < signal_table_lock_offset

    assert {limit_guard_offset, _length} =
             :binary.match(expire_sql, "IF p_limit IS NULL OR p_limit <= 0 THEN")

    assert {candidate_offset, _length} =
             :binary.match(expire_sql, "FROM pgflow.task_signals candidate")

    assert signal_table_lock_offset < limit_guard_offset
    assert limit_guard_offset < candidate_offset
    assert expire_sql =~ "FROM pgflow.task_signals candidate"
    assert expire_sql =~ "candidate.timed_out = false"
    assert expire_sql =~ "candidate.payload IS NULL"
    assert expire_sql =~ "ORDER BY candidate.wait_deadline_at"
    assert expire_sql =~ "LIMIT p_limit"

    assert {run_lock_offset, _length} = :binary.match(expire_sql, "FROM pgflow.runs")
    assert {step_lock_offset, _length} = :binary.match(expire_sql, "FROM pgflow.step_states")
    assert {task_lock_offset, _length} = :binary.match(expire_sql, "FROM pgflow.step_tasks")

    assert {signal_lock_offset, _length} =
             :binary.match(expire_sql, "FROM pgflow.task_signals ts")

    assert run_lock_offset < step_lock_offset
    assert step_lock_offset < task_lock_offset
    assert task_lock_offset < signal_lock_offset
    assert Enum.count_until(:binary.matches(expire_sql, "FOR UPDATE SKIP LOCKED"), 4) == 4
    assert expire_sql =~ "v_run.status <> 'started'"
    assert expire_sql =~ "v_step.status <> 'started'"
    assert expire_sql =~ "v_task.status <> 'waiting'"
    assert expire_sql =~ "v_signal.timed_out"
    assert expire_sql =~ "v_signal.payload IS NOT NULL"
    assert expire_sql =~ "v_signal.wait_deadline_at > now()"
    assert expire_sql =~ "GET DIAGNOSTICS v_updated = ROW_COUNT"
    assert expire_sql =~ "IF v_updated <> 1 THEN"
    assert expire_sql =~ "ts.timed_out = false"
    assert expire_sql =~ "ts.payload IS NULL"

    assert query_source =~
             "def expire_waiting_tasks(repo, limit) when is_integer(limit) and limit > 0"

    assert query_source =~ "SELECT pgflow.expire_waiting_tasks($1)"
    assert recovery_source =~ "waiting_recovery_batch_size"
    assert recovery_source =~ "Signals.expire_waiting_tasks(state.repo, state.batch_size)"
  end

  test "late duplicates cannot turn an accepted unclaimed payload into a timeout" do
    sql = File.read!(@sql_path)

    assert {signal_offset, _length} =
             :binary.match(sql, "CREATE OR REPLACE FUNCTION $SCHEMA$.signal_task(")

    signal_sql =
      sql
      |> binary_part(signal_offset, byte_size(sql) - signal_offset)
      |> String.split("\n--SPLIT--\n", parts: 2)
      |> hd()

    assert {deadline_guard_offset, _length} =
             :binary.match(signal_sql, "v_signal.wait_deadline_at <= now()")

    assert {payload_guard_offset, _length} =
             :binary.match(signal_sql, "IF v_signal.payload IS NULL THEN")

    assert {timeout_write_offset, _length} = :binary.match(signal_sql, "SET timed_out = true")

    assert {requeue_offset, _length} =
             :binary.match(signal_sql, "IF v_has_task AND v_task.status = 'waiting'")

    assert {expired_return_offset, _length} =
             :binary.match(signal_sql, "RETURN QUERY SELECT 'expired'::text")

    assert deadline_guard_offset < payload_guard_offset
    assert payload_guard_offset < timeout_write_offset
    assert timeout_write_offset < requeue_offset
    assert requeue_offset < expired_return_offset
  end
end
