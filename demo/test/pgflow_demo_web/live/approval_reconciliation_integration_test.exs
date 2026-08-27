defmodule PgflowDemoWeb.ApprovalReconciliationIntegrationTest do
  use PgflowDemo.DataCase, async: true

  alias PgFlow.Client
  alias PgFlow.Queries.{Flows, Signals, Workers}
  alias PgFlow.Worker.TaskRow
  alias PgflowDemoWeb.FlowDemoLive
  alias Phoenix.{Component, HTML.Safe, LiveView.Socket}

  @flow_slug "approval_flow"
  @moduletag :integration

  test "reconciliation restores a parked approval task and its actions from the database" do
    worker_id = Ecto.UUID.generate()
    {:ok, nil} = Workers.register_worker(Repo, worker_id, @flow_slug, "elixir:test")

    order = %{"order_id" => "ord_reconcile", "amount" => 42}
    {:ok, run_id} = Flows.start_flow(Repo, @flow_slug, order)

    _create_order = claim_task!(run_id, "create_order", worker_id)
    {:ok, _task} = Flows.complete_task(Repo, run_id, "create_order", 0, order)

    await_approval = claim_task!(run_id, "await_approval", worker_id)

    assert {:ok, []} = Client.get_waiting_tasks(run_id)

    assert :parked =
             Signals.await_task_signal(
               Repo,
               run_id,
               "await_approval",
               0,
               await_approval.attempt,
               await_approval.msg_id,
               3_600,
               true
             )

    assert %{rows: [["waiting", nil]]} =
             Repo.query!(
               """
               SELECT status, message_id
               FROM pgflow.step_tasks
               WHERE run_id = $1 AND step_slug = 'await_approval' AND task_index = 0
               """,
               [Ecto.UUID.dump!(run_id)]
             )

    assert {:ok, [%{step_slug: "await_approval", task_index: 0}]} =
             Client.get_waiting_tasks(run_id)

    {:ok, socket} = FlowDemoLive.mount(%{}, %{}, %Socket{})
    {:noreply, socket} = FlowDemoLive.handle_event("select_flow", %{"flow" => "approval"}, socket)

    socket =
      socket
      |> Component.assign(run_id: run_id, run_status: :running)
      |> FlowDemoLive.reconcile_run_state(run_id)

    assert socket.assigns.steps.await_approval == :waiting

    html =
      socket.assigns
      |> FlowDemoLive.render()
      |> Safe.to_iodata()
      |> IO.iodata_to_binary()

    assert html =~ ~s(id="approval-actions")
    assert html =~ ~s(id="approval-approve")
    assert html =~ ~s(id="approval-reject")
  end

  defp claim_task!(run_id, step_slug, worker_id) do
    assert %{rows: [["queued", message_id]]} =
             Repo.query!(
               """
               SELECT status, message_id
               FROM pgflow.step_tasks
               WHERE run_id = $1 AND step_slug = $2 AND task_index = 0
               """,
               [Ecto.UUID.dump!(run_id), step_slug]
             )

    assert is_integer(message_id)
    assert {:ok, [row]} = Flows.start_tasks(Repo, @flow_slug, [message_id], worker_id)

    task = TaskRow.decode(row)
    assert task.step_slug == step_slug
    task
  end
end
