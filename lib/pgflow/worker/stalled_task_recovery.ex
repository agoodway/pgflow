defmodule PgFlow.Worker.StalledTaskRecovery do
  @moduledoc """
  Periodically recovers stalled tasks stuck in 'started' status.

  If a worker crashes while processing tasks, those tasks remain in 'started'
  status in the step_tasks table. The pgmq message eventually becomes visible
  again, but `start_tasks` can't re-process it because the step_task record
  is stuck. This GenServer periodically finds and resets those records.

  ## Configuration

    * `:recovery_interval` - Milliseconds between recovery sweeps (default: 15_000)
    * `:stale_threshold` - Seconds after which a started task is considered stalled (default: 60)
  """

  use GenServer
  require Logger

  alias PgFlow.Queries.Flows

  @doc """
  Starts the StalledTaskRecovery GenServer.

  ## Options

    * `:repo` - (required) The Ecto repository module
    * `:recovery_interval` - Milliseconds between sweeps (default: 15_000)
    * `:stale_threshold` - Seconds threshold for stale tasks (default: 60)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(config) when is_list(config) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(config) do
    repo = Keyword.fetch!(config, :repo)
    recovery_interval = Keyword.fetch!(config, :recovery_interval)
    stale_threshold = Keyword.fetch!(config, :stale_threshold)

    state = %{
      repo: repo,
      recovery_interval: recovery_interval,
      stale_threshold: stale_threshold
    }

    schedule_recovery(state)

    {:ok, state}
  end

  @impl true
  def handle_info(:recover, state) do
    case Flows.recover_stalled_tasks(state.repo, state.stale_threshold) do
      {:ok, 0} ->
        :ok

      {:ok, count} ->
        Logger.info("Recovered #{count} stalled task(s)")

      {:error, reason} ->
        Logger.warning("Failed to recover stalled tasks: #{inspect(reason)}")
    end

    schedule_recovery(state)

    {:noreply, state}
  end

  defp schedule_recovery(state) do
    Process.send_after(self(), :recover, state.recovery_interval)
  end
end
