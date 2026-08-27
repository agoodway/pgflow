defmodule PgFlow.Queries.OrphanedMessagesTest do
  @moduledoc """
  Query-layer tests for the archive-invariant tripwire.

  Every SQL skip path archives the skipped step's queued pgmq messages in the
  same transaction (the invariant the worker's dispatch relies on — see the
  no-skip-check note in `PgFlow.Worker.Server.dispatch_task/2`). These
  functions exist for the day a future SQL bundle breaks that invariant:
  they let the worker diagnose messages `pgflow.start_tasks` declined to
  hand out, and archive the ones that would otherwise redeliver forever.
  """
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Queries.Flows
  alias PgFlow.TestRepo

  @moduletag timeout: 30_000
  @moduletag :integration

  @flow_slug "orphan_probe_flow"

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    on_exit(fn ->
      Sandbox.mode(TestRepo, :manual)
    end)

    TestRepo.query!("SELECT pgflow.create_flow($1, 3, 1, 30)", [@flow_slug])

    TestRepo.query!(
      "SELECT pgflow.add_step($1, 'root_step', '{}'::text[], NULL, NULL, NULL, NULL, 'single')",
      [@flow_slug]
    )

    %{rows: [[run_id]]} =
      TestRepo.query!("SELECT run_id FROM pgflow.start_flow($1, '{}'::jsonb)", [@flow_slug])

    %{rows: [[msg_id]]} =
      TestRepo.query!(
        "SELECT message_id FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = 'root_step'",
        [run_id]
      )

    {:ok, run_id: run_id, msg_id: msg_id}
  end

  describe "orphaned_queue_messages/3" do
    test "reports a still-queued message with its step's status", %{msg_id: msg_id} do
      assert {:ok, [orphan]} = Flows.orphaned_queue_messages(TestRepo, @flow_slug, [msg_id])
      assert orphan.msg_id == msg_id
      assert orphan.step_slug == "root_step"
      assert orphan.step_status == "started"
    end

    test "reports the terminal status of a skipped-but-never-archived message", %{
      run_id: run_id,
      msg_id: msg_id
    } do
      # Simulate a broken skip path: the step goes terminal but its queued
      # message is left behind (the invariant says SQL always archives it).
      TestRepo.transaction(fn ->
        TestRepo.query!("SET LOCAL session_replication_role = replica")

        TestRepo.query!(
          "UPDATE pgflow.step_states SET status = 'skipped', skipped_at = now(), remaining_tasks = NULL, skip_reason = 'condition_unmet' WHERE run_id = $1 AND step_slug = 'root_step'",
          [run_id]
        )
      end)

      assert {:ok, [orphan]} = Flows.orphaned_queue_messages(TestRepo, @flow_slug, [msg_id])
      assert orphan.step_status == "skipped"
    end

    test "returns an empty list for messages no longer in the queue (benign race)", %{
      msg_id: msg_id
    } do
      TestRepo.query!("SELECT pgmq.archive($1::text, $2::bigint)", [@flow_slug, msg_id])

      assert {:ok, []} = Flows.orphaned_queue_messages(TestRepo, @flow_slug, [msg_id])
    end
  end

  describe "archive_messages/3" do
    test "moves the message from the live queue to the archive", %{msg_id: msg_id} do
      assert {:ok, [^msg_id]} = Flows.archive_messages(TestRepo, @flow_slug, [msg_id])

      assert %{rows: [[0]]} =
               TestRepo.query!("SELECT count(*) FROM pgmq.q_#{@flow_slug} WHERE msg_id = $1", [
                 msg_id
               ])

      assert %{rows: [[1]]} =
               TestRepo.query!("SELECT count(*) FROM pgmq.a_#{@flow_slug} WHERE msg_id = $1", [
                 msg_id
               ])
    end

    test "archiving an already-archived message reports it as not archived", %{msg_id: msg_id} do
      TestRepo.query!("SELECT pgmq.archive($1::text, $2::bigint)", [@flow_slug, msg_id])

      assert {:ok, []} = Flows.archive_messages(TestRepo, @flow_slug, [msg_id])
    end
  end
end
