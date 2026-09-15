defmodule PgflowDemo.ScenarioControls.ReleaseRegistryTest do
  use ExUnit.Case, async: false

  alias PgflowDemo.ScenarioControls.ReleaseRegistry

  test "an overwritten handler's DOWN cannot disable its live replacement" do
    run_id = Ecto.UUID.generate()

    old_handler =
      Task.async(fn ->
        receive do
          :finish -> :ok
        end
      end)

    on_exit(fn -> send(old_handler.pid, :finish) end)
    :ok = ReleaseRegistry.register(run_id, old_handler.pid)
    :ok = ReleaseRegistry.register(run_id, self())
    send(old_handler.pid, :finish)
    Task.await(old_handler)
    send(ReleaseRegistry, {:DOWN, make_ref(), :process, old_handler.pid, :normal})
    assert ReleaseRegistry.registered?(run_id)
    :ok = ReleaseRegistry.release(run_id)
    assert_receive :release
    refute ReleaseRegistry.registered?(run_id)
  end

  test "readiness is removed and subscribers notified when a handler exits" do
    run_id = Ecto.UUID.generate()
    :ok = Phoenix.PubSub.subscribe(PgflowDemo.PubSub, "scenario:recovery:#{run_id}")

    handler =
      Task.async(fn ->
        receive do
          :finish -> :ok
        end
      end)

    on_exit(fn -> send(handler.pid, :finish) end)
    refute ReleaseRegistry.registered?(run_id)
    :ok = ReleaseRegistry.register(run_id, handler.pid)
    assert ReleaseRegistry.registered?(run_id)
    assert_receive {:recovery_handler_changed, ^run_id}
    send(handler.pid, :finish)
    Task.await(handler)
    assert_receive {:recovery_handler_changed, ^run_id}
    refute ReleaseRegistry.registered?(run_id)
  end
end
