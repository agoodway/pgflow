defmodule PgFlow.RunDeleteConcurrencyTest do
  @moduledoc """
  Two-connection lock-order regressions for `PgFlow.Runs.delete/2`.

  Each test parks one session on a row lock the other already holds, then
  drives the real claim, recovery, or prune SQL into the locks the first
  session would take next. Run deletion locks the run and every task row
  before touching a queue, and pruning locks its runs before their tasks, so
  neither interleaving may produce a deadlock (`40P01`): the blocked side
  waits and both sides complete.
  """
  use ExUnit.Case
  use PgFlow.Test.WaitHelpers

  import PgFlow.IntegrationCase, only: [add_step: 2, create_flow: 1, start_flow_run: 2]

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Queries.Flows
  alias PgFlow.Queries.Workers, as: WorkerQueries
  alias PgFlow.Runs
  alias PgFlow.Test.DatabaseHelpers
  alias PgFlow.TestRepo

  @moduletag :integration
  @moduletag timeout: 60_000

  # Longer than the server's one-second default deadlock_timeout so a lock
  # cycle is reported as 40P01 instead of being masked by a statement timeout.
  @statement_timeout "20s"
  @await_timeout 30_000

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    on_exit(fn -> Sandbox.mode(TestRepo, :manual) end)

    :ok
  end

  describe "delete/2 against stalled-task recovery" do
    test "requeue branch waits for the task lock instead of deadlocking" do
      fixture = started_task!("delete_vs_requeue")

      assert {:ok, %{rows: [[1, 1]]}} =
               delete_while_task_locked(fixture, fn conn ->
                 Postgrex.query(conn, "SELECT * FROM pgflow.recover_stalled_tasks(0)", [])
               end)

      assert_run_gone(fixture)
    end

    test "archive branch waits for the task lock instead of deadlocking" do
      fixture = started_task!("delete_vs_archive")

      TestRepo.query!("UPDATE pgflow.step_tasks SET requeued_count = 3 WHERE run_id = $1", [
        fixture.run_id_bin
      ])

      assert {:ok, %{rows: [[0, 0]]}} =
               delete_while_task_locked(fixture, fn conn ->
                 Postgrex.query(conn, "SELECT * FROM pgflow.recover_stalled_tasks(0)", [])
               end)

      assert_run_gone(fixture)
    end
  end

  describe "delete/2 against task claims" do
    test "start_tasks waits for the task lock instead of deadlocking" do
      fixture = queued_task!("delete_vs_claim")

      assert {:ok, %{rows: [[1]]}} =
               delete_while_task_locked(fixture, fn conn ->
                 Postgrex.query(
                   conn,
                   """
                   SELECT count(*)
                   FROM pgflow.start_tasks($1::text, $2::bigint[], $3::uuid, $4::text, NULL::text)
                   """,
                   [fixture.flow_slug, fixture.msg_ids, fixture.worker_id_bin, fixture.queue_name]
                 )
               end)

      assert_run_gone(fixture)
    end
  end

  describe "delete/2 against pruning" do
    test "prune and delete of the same old terminal run both complete" do
      fixture = completed_old_run!("delete_vs_prune")
      parent = self()

      deleter =
        Task.async(fn ->
          in_transaction(fn ->
            TestRepo.query!("SELECT 1 FROM pgflow.runs WHERE run_id = $1 FOR UPDATE", [
              fixture.run_id_bin
            ])

            send(parent, {:deleter_locked, self(), backend_pid()})

            receive do
              :delete -> Runs.delete(TestRepo, fixture.run_id)
            end
          end)
        end)

      assert_receive {:deleter_locked, deleter_pid, deleter_backend}, 5_000

      pruner =
        Task.async(fn ->
          in_transaction(fn ->
            send(parent, {:pruner_backend, backend_pid()})

            TestRepo.query(
              "SELECT * FROM pgflow.prune_data_older_than(make_interval(days => 7))",
              []
            )
          end)
        end)

      assert_receive {:pruner_backend, pruner_backend}, 5_000
      wait_until(fn -> blocked_by?(pruner_backend, deleter_backend) end, timeout: 10_000)

      send(deleter_pid, :delete)

      assert :ok = Task.await(deleter, @await_timeout)
      assert {:ok, %{rows: [[0, 0, 0, _workers]]}} = Task.await(pruner, @await_timeout)

      assert_run_gone(fixture)
    end
  end

  # Holds every task row of the fixture run on a second connection, parks
  # `Runs.delete/2` on that lock, then runs `operation` in the lock holder's
  # transaction. Returns the operation's result after both sides finish.
  defp delete_while_task_locked(fixture, operation) do
    holder = start_supervised!({Postgrex, connection_opts()})
    parent = self()

    result =
      Postgrex.transaction(
        holder,
        fn conn ->
          Postgrex.query!(conn, "SET LOCAL statement_timeout = '#{@statement_timeout}'", [])
          %{rows: [[holder_backend]]} = Postgrex.query!(conn, "SELECT pg_backend_pid()", [])

          Postgrex.query!(
            conn,
            "SELECT 1 FROM pgflow.step_tasks WHERE run_id = $1 ORDER BY step_slug, task_index FOR UPDATE",
            [fixture.run_id_bin]
          )

          deleter = start_deleter(parent, fixture.run_id)
          assert_receive {:deleter_backend, deleter_backend}, 5_000

          wait_until(
            fn -> holder_blocks?(conn, deleter_backend, holder_backend) end,
            timeout: 10_000
          )

          case operation.(conn) do
            {:error, _reason} = error -> Postgrex.rollback(conn, {error, deleter})
            operation_result -> {operation_result, deleter}
          end
        end,
        timeout: @await_timeout
      )

    {operation_result, deleter} =
      case result do
        {:ok, value} -> value
        {:error, value} -> value
      end

    assert :ok = Task.await(deleter, @await_timeout)
    operation_result
  end

  defp start_deleter(parent, run_id) do
    Task.async(fn ->
      in_transaction(fn ->
        send(parent, {:deleter_backend, backend_pid()})
        Runs.delete(TestRepo, run_id)
      end)
    end)
  end

  # Runs `fun` in a pinned transaction with a bounded statement timeout and
  # returns `fun`'s result. An `{:error, _}` result rolls back and is returned
  # as-is, so a deadlock surfaces as its Postgrex error rather than a generic
  # rollback.
  defp in_transaction(fun) do
    result =
      TestRepo.transaction(
        fn ->
          TestRepo.query!("SET LOCAL statement_timeout = '#{@statement_timeout}'")

          case fun.() do
            {:error, _reason} = error -> TestRepo.rollback(error)
            value -> value
          end
        end,
        timeout: @await_timeout
      )

    case result do
      {:ok, value} -> value
      {:error, {:error, _reason} = error} -> error
      {:error, reason} -> {:error, reason}
    end
  end

  defp holder_blocks?(conn, waiting_backend, holder_backend) do
    %{rows: [[blocked?]]} =
      Postgrex.query!(conn, "SELECT $1::int = ANY(pg_blocking_pids($2::int))", [
        holder_backend,
        waiting_backend
      ])

    blocked?
  end

  defp blocked_by?(waiting_backend, holder_backend) do
    %{rows: [[blocked?]]} =
      TestRepo.query!("SELECT $1::int = ANY(pg_blocking_pids($2::int))", [
        holder_backend,
        waiting_backend
      ])

    blocked?
  end

  defp backend_pid do
    %{rows: [[pid]]} = TestRepo.query!("SELECT pg_backend_pid()")
    pid
  end

  defp connection_opts, do: DatabaseHelpers.connection_opts(TestRepo.config())

  defp queued_task!(flow_slug) do
    create_flow(flow_slug)
    add_step(flow_slug, "work")
    run_id = start_flow_run(flow_slug, %{"value" => 1})
    {:ok, messages} = Flows.read(TestRepo, flow_slug, 30, 10)
    worker_id = Ecto.UUID.generate()
    {:ok, _} = WorkerQueries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")

    %{
      flow_slug: flow_slug,
      queue_name: flow_slug,
      run_id: run_id,
      run_id_bin: Ecto.UUID.dump!(run_id),
      msg_ids: Enum.map(messages, fn [msg_id | _] -> msg_id end),
      worker_id: worker_id,
      worker_id_bin: Ecto.UUID.dump!(worker_id)
    }
  end

  defp started_task!(flow_slug) do
    fixture = queued_task!(flow_slug)

    {:ok, [_task]} =
      Flows.start_tasks(TestRepo, flow_slug, fixture.msg_ids, fixture.worker_id, flow_slug)

    TestRepo.query!(
      """
      UPDATE pgflow.step_tasks
      SET queued_at = NOW() - interval '120 seconds', started_at = NOW() - interval '115 seconds'
      WHERE run_id = $1
      """,
      [fixture.run_id_bin]
    )

    fixture
  end

  defp completed_old_run!(flow_slug) do
    fixture = started_task!(flow_slug)

    TestRepo.query!("SELECT pgflow.complete_task($1, 'work', 0, '{}'::jsonb)", [
      fixture.run_id_bin
    ])

    TestRepo.query!(
      """
      UPDATE pgflow.runs
      SET started_at = NOW() - interval '31 days', completed_at = NOW() - interval '30 days'
      WHERE run_id = $1 AND status = 'completed'
      """,
      [fixture.run_id_bin]
    )

    assert archive_count(fixture) == 1
    fixture
  end

  defp assert_run_gone(fixture) do
    %{rows: [[runs, states, tasks]]} =
      TestRepo.query!(
        """
        SELECT
          (SELECT count(*) FROM pgflow.runs WHERE run_id = $1),
          (SELECT count(*) FROM pgflow.step_states WHERE run_id = $1),
          (SELECT count(*) FROM pgflow.step_tasks WHERE run_id = $1)
        """,
        [fixture.run_id_bin]
      )

    assert {runs, states, tasks} == {0, 0, 0}
    assert queue_count(fixture, "q") == 0
    assert archive_count(fixture) == 0
  end

  defp archive_count(fixture), do: queue_count(fixture, "a")

  defp queue_count(fixture, prefix) do
    %{rows: [[count]]} =
      TestRepo.query!(
        """
        SELECT count(*)
        FROM pgmq.#{prefix}_#{fixture.queue_name}
        WHERE message->>'run_id' = $1
        """,
        [fixture.run_id]
      )

    count
  end
end
