defmodule PgflowDemoWeb.FlowDemoLiveUnitTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias PgflowDemoWeb.FlowDemoLive

  test "signal delivery failures hide internal reasons and log diagnostic detail" do
    log =
      capture_log(fn ->
        assert {:noreply, failed} =
                 FlowDemoLive.apply_signal_delivery_result(
                   signal_socket(),
                   {:error, {:db_connection, "secret-host"}}
                 )

        refute failed.assigns.approval_submitted
        assert failed.assigns.approval_error == "Signal delivery failed. Please try again."
        refute failed.assigns.approval_error =~ "secret-host"
      end)

    assert log =~ "secret-host"
  end

  test "waiting-task lookup retries one transient failure before reconciling" do
    attempts_key = {__MODULE__, :waiting_lookup_attempts}
    Process.put(attempts_key, 0)
    waiting_task = %{step_slug: "await_approval", task_index: 0}

    lookup = fn "run-id" ->
      attempt = Process.get(attempts_key) + 1
      Process.put(attempts_key, attempt)

      if attempt == 1, do: {:error, :connection_closed}, else: {:ok, [waiting_task]}
    end

    assert FlowDemoLive.load_waiting_task_rows("run-id", lookup) == [waiting_task]
    assert Process.get(attempts_key) == 2
  end

  defp signal_socket do
    %Phoenix.LiveView.Socket{
      assigns: %{
        __changed__: %{},
        approval_submitted: false,
        approval_error: nil
      }
    }
  end
end
