defmodule PgFlow.Upstream.QueueIdentityTest do
  @moduledoc """
  Queue-scoped claiming: `(queue_name, message_id)` identity and `start_tasks/5`.
  """
  use PgFlow.IntegrationCase, async: false

  alias PgFlow.Queries.Flows
  alias PgFlow.Queries.Workers, as: WorkerQueries
  alias PgFlow.TestRepo
  alias PgFlow.Worker.TaskRow

  @moduletag :integration
  @large_msg_id 9_007_199_254_740_993

  setup do
    worker_id = Ecto.UUID.generate()
    {:ok, _} = WorkerQueries.register_worker(TestRepo, worker_id, "queue_a", "elixir:test")
    {:ok, worker_id: worker_id}
  end

  describe "start_tasks/5 queue identity" do
    test "claiming one queue leaves the same message id in another queue untouched", %{
      worker_id: worker_id
    } do
      create_flow("queue_a")
      create_flow("queue_b")
      add_step("queue_a", "work")
      add_step("queue_b", "work")

      run_a = start_flow_run("queue_a", %{})
      run_b = start_flow_run("queue_b", %{})

      msg_a = task_message_id(run_a, "work")
      msg_b = task_message_id(run_b, "work")

      align_shared_message_id!("queue_a", run_a, msg_a, @large_msg_id)
      align_shared_message_id!("queue_b", run_b, msg_b, @large_msg_id)

      assert {:ok, [claimed]} =
               Flows.start_tasks(TestRepo, "queue_a", [@large_msg_id], worker_id, "queue_a")

      row = TaskRow.decode(claimed)
      assert row.msg_id == @large_msg_id
      assert row.attempt == 1

      assert task_snapshot(run_a, "work") == %{
               status: "started",
               attempts_count: 1,
               message_id: @large_msg_id,
               queue_name: "queue_a"
             }

      assert task_snapshot(run_b, "work") == %{
               status: "queued",
               attempts_count: 0,
               message_id: @large_msg_id,
               queue_name: "queue_b"
             }

      assert message_in_live_queue?("queue_b", @large_msg_id)
    end

    test "wrong flow_slug declines a message that belongs to another flow", %{
      worker_id: worker_id
    } do
      create_flow("queue_a")
      create_flow("queue_b")
      add_step("queue_a", "work")
      add_step("queue_b", "work")

      run_a = start_flow_run("queue_a", %{})
      msg_id = task_message_id(run_a, "work")

      assert {:ok, []} =
               Flows.start_tasks(TestRepo, "queue_b", [msg_id], worker_id, "queue_a")

      assert task_snapshot(run_a, "work").status == "queued"
    end

    test "unknown message ids are ignored without affecting matched tasks", %{
      worker_id: worker_id
    } do
      create_flow("queue_a")
      add_step("queue_a", "work")

      run_id = start_flow_run("queue_a", %{})
      msg_id = task_message_id(run_id, "work")

      assert {:ok, [claimed]} =
               Flows.start_tasks(
                 TestRepo,
                 "queue_a",
                 [msg_id, 9_999_999_999_999],
                 worker_id,
                 "queue_a"
               )

      assert TaskRow.decode(claimed).msg_id == msg_id
      assert task_snapshot(run_id, "work").attempts_count == 1
    end

    test "duplicate claim does not increment attempts twice", %{worker_id: worker_id} do
      create_flow("queue_a")
      add_step("queue_a", "work")

      run_id = start_flow_run("queue_a", %{})
      msg_id = task_message_id(run_id, "work")

      assert {:ok, [_first]} =
               Flows.start_tasks(TestRepo, "queue_a", [msg_id], worker_id, "queue_a")

      assert {:ok, []} =
               Flows.start_tasks(TestRepo, "queue_a", [msg_id], worker_id, "queue_a")

      assert task_snapshot(run_id, "work").attempts_count == 1
    end

    test "null queue_name is rejected", %{worker_id: worker_id} do
      create_flow("queue_a")
      add_step("queue_a", "work")
      run_id = start_flow_run("queue_a", %{})
      msg_id = task_message_id(run_id, "work")

      assert {:error, :invalid_queue_name} =
               Flows.start_tasks(TestRepo, "queue_a", [msg_id], worker_id, nil)
    end

    test "mixed-case flow slug uses the canonical lowercase queue route", %{
      worker_id: worker_id
    } do
      flow_slug = "MixedCaseQueue"
      queue_name = canonical_queue_name(flow_slug)

      create_flow(flow_slug)
      add_step(flow_slug, "work")

      run_id = start_flow_run(flow_slug, %{})
      msg_id = task_message_id(run_id, "work")

      assert {:ok, [claimed]} =
               Flows.start_tasks(TestRepo, flow_slug, [msg_id], worker_id, queue_name)

      assert TaskRow.decode(claimed).msg_id == msg_id
      assert task_snapshot(run_id, "work").queue_name == queue_name

      assert {:ok, []} =
               Flows.start_tasks(TestRepo, flow_slug, [msg_id], worker_id, flow_slug)
    end

    test "preserves bigint message id #{@large_msg_id} through claim", %{worker_id: worker_id} do
      create_flow("queue_a")
      add_step("queue_a", "work")

      run_id = start_flow_run("queue_a", %{})
      original = task_message_id(run_id, "work")

      align_shared_message_id!("queue_a", run_id, original, @large_msg_id)

      assert {:ok, [claimed]} =
               Flows.start_tasks(TestRepo, "queue_a", [@large_msg_id], worker_id, "queue_a")

      assert TaskRow.decode(claimed).msg_id == @large_msg_id
      assert is_integer(TaskRow.decode(claimed).msg_id)
    end

    test "incomplete set_vt_batch rolls back the claim via the _vr guard", %{
      worker_id: worker_id
    } do
      create_flow("queue_a")
      add_step("queue_a", "work")

      run_id = start_flow_run("queue_a", %{})
      msg_id = task_message_id(run_id, "work")

      # Controlled failure: step_tasks still references msg_id, but the live pgmq
      # row is gone. The tasks CTE claims and increments attempts_count; then
      # visibility_reset finds zero rows. _vr compares updated_count < claimed_count,
      # format()::int4 fails the statement, and the whole claim rolls back.
      TestRepo.query!("DELETE FROM pgmq.q_queue_a WHERE msg_id = $1", [msg_id])

      assert {:error, %Postgrex.Error{postgres: %{message: message}}} =
               Flows.start_tasks(TestRepo, "queue_a", [msg_id], worker_id, "queue_a")

      assert message =~ "visibility updated 0 of 1 claimed messages"

      assert task_snapshot(run_id, "work") == %{
               status: "queued",
               attempts_count: 0,
               message_id: msg_id,
               queue_name: "queue_a"
             }
    end

    test "late complete_task after terminal step does not mutate run output", %{
      worker_id: worker_id
    } do
      create_flow("queue_a")
      add_step("queue_a", "work")

      run_id = start_flow_run("queue_a", %{})
      msg_id = task_message_id(run_id, "work")

      assert {:ok, [_claimed]} =
               Flows.start_tasks(TestRepo, "queue_a", [msg_id], worker_id, "queue_a")

      assert {:ok, _} =
               Flows.complete_task(TestRepo, run_id, "work", 0, %{"done" => true})

      assert get_run(run_id).status == "completed"
      original_output = get_run(run_id).output

      assert {:ok, _} =
               Flows.complete_task(TestRepo, run_id, "work", 0, %{"late" => true})

      assert get_run(run_id).output == original_output
    end
  end

  defp task_message_id(run_id, step_slug) do
    %{rows: [[message_id]]} =
      TestRepo.query!(
        """
        SELECT message_id FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = $2
        """,
        [ensure_uuid_binary(run_id), step_slug]
      )

    message_id
  end

  defp task_snapshot(run_id, step_slug) do
    %{rows: [[status, attempts_count, message_id, queue_name]]} =
      TestRepo.query!(
        """
        SELECT status, attempts_count, message_id, queue_name
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = $2
        """,
        [ensure_uuid_binary(run_id), step_slug]
      )

    %{
      status: status,
      attempts_count: attempts_count,
      message_id: message_id,
      queue_name: queue_name
    }
  end

  defp align_shared_message_id!(queue_name, run_id, old_id, new_id) do
    TestRepo.query!("DELETE FROM pgmq.q_#{queue_name} WHERE msg_id = $1", [old_id])

    TestRepo.query!(
      """
      INSERT INTO pgmq.q_#{queue_name} (msg_id, vt, message)
      OVERRIDING SYSTEM VALUE
      VALUES ($1, now(), '{}'::jsonb)
      """,
      [new_id]
    )

    TestRepo.query!(
      """
      UPDATE pgflow.step_tasks
      SET message_id = $1
      WHERE run_id = $2 AND message_id = $3
      """,
      [new_id, ensure_uuid_binary(run_id), old_id]
    )
  end

  defp message_in_live_queue?(queue_name, msg_id) do
    %{rows: [[count]]} =
      TestRepo.query!("SELECT count(*) FROM pgmq.q_#{queue_name} WHERE msg_id = $1", [msg_id])

    count == 1
  end
end
