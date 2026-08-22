defmodule PgFlow.Queries.SignalsTest do
  @moduledoc """
  Integration tests for `PgFlow.Queries.Signals` — the Elixir wrappers around
  helpers V05 park/signal/consume/expire. These call the shipped SQL, not a
  reimplementation.
  """
  use PgFlow.IntegrationCase, async: false

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

  test "signal before await buffers; consume returns payload" do
    compile_one_step_flow("signals_buffer_flow", "step")
    run_id = start_flow_run("signals_buffer_flow", %{"n" => 1})

    assert :ok = Signals.signal_task(repo(), run_id, "step", 0, %{"decision" => "approved"})

    assert {:ok, %{"decision" => "approved"}} =
             Signals.consume_task_signal(repo(), run_id, "step", 0)

    assert :empty = Signals.consume_task_signal(repo(), run_id, "step", 0)
  end

  test "park then signal requeues task as queued" do
    compile_one_step_flow("signals_park_flow", "step")
    run_id = start_started_task("signals_park_flow", %{"n" => 1})

    assert :ok = Signals.park_waiting_task(repo(), run_id, "step", 0, nil)
    assert get_task_details(run_id, "step", 0).status == "waiting"

    assert :ok = Signals.signal_task(repo(), run_id, "step", 0, %{"ok" => true})
    assert get_task_details(run_id, "step", 0).status == "queued"
  end

  test "expire marks timeout and requeues" do
    compile_one_step_flow("signals_expire_flow", "step")
    run_id = start_started_task("signals_expire_flow", %{"n" => 1})

    deadline = DateTime.add(DateTime.utc_now(), -60, :second)
    assert :ok = Signals.park_waiting_task(repo(), run_id, "step", 0, deadline)
    assert {:ok, n} = Signals.expire_waiting_tasks(repo())
    assert n >= 1
    assert {:error, :timeout} = Signals.consume_task_signal(repo(), run_id, "step", 0)
  end
end
