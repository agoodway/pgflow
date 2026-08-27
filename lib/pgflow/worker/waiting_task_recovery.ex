defmodule PgFlow.Worker.WaitingTaskRecovery do
  @moduledoc """
  Periodically re-queues `waiting` tasks whose `wait_deadline_at` has passed.

  After requeue, the handler's next `PgFlow.Context.await_signal/2` returns
  `{:error, :timeout}`. This sweep does **not** broaden stalled-task recovery;
  `waiting` tasks are never treated as stalled.
  """

  use GenServer
  require Logger

  alias PgFlow.Queries.Signals

  @default_interval 15_000
  @default_batch_size 100

  @doc """
  Starts the waiting-task recovery GenServer.

  ## Options

    * `:repo` - (required) The Ecto repository module
    * `:waiting_recovery_interval` - Milliseconds between sweeps (default: `15_000`)
    * `:waiting_recovery_batch_size` - Maximum tasks expired per sweep (default: `100`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(config) when is_list(config) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(config) do
    repo = Keyword.fetch!(config, :repo)
    interval = Keyword.get(config, :waiting_recovery_interval, @default_interval)
    batch_size = Keyword.get(config, :waiting_recovery_batch_size, @default_batch_size)

    state = %{repo: repo, interval: interval, batch_size: batch_size}
    schedule_recovery(state)
    {:ok, state}
  end

  @impl true
  def handle_info(:recover, state) do
    case Signals.expire_waiting_tasks(state.repo, state.batch_size) do
      {:ok, 0} ->
        :ok

      {:ok, count} ->
        Logger.info("Expired #{count} waiting task(s)")

      {:error, reason} ->
        Logger.warning("Failed to expire waiting tasks: #{inspect(reason)}")
    end

    schedule_recovery(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.debug("WaitingTaskRecovery received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp schedule_recovery(state) do
    Process.send_after(self(), :recover, state.interval)
  end
end
