defmodule PgflowDemo.ScenarioControls.ReleaseRegistry do
  @moduledoc """
  Maps demo recovery run ids to blocked handler pids for test-bed release controls.
  """

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.put_new(opts, :name, __MODULE__))
  end

  @spec register(String.t(), pid()) :: :ok
  def register(run_id, handler_pid) when is_binary(run_id) and is_pid(handler_pid) do
    GenServer.call(__MODULE__, {:register, run_id, handler_pid})
  end

  @doc "Returns whether a live handler is registered for release."
  @spec registered?(String.t()) :: boolean()
  def registered?(run_id) when is_binary(run_id) do
    GenServer.call(__MODULE__, {:registered?, run_id})
  end

  @spec release(String.t()) :: :ok | :not_found
  def release(run_id) when is_binary(run_id) do
    GenServer.call(__MODULE__, {:release, run_id})
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:register, run_id, handler_pid}, _from, state) do
    Process.monitor(handler_pid)

    Phoenix.PubSub.broadcast(
      PgflowDemo.PubSub,
      "scenario:recovery:#{run_id}",
      {:handler_started, handler_pid}
    )

    broadcast_change(run_id)

    {:reply, :ok, Map.put(state, run_id, handler_pid)}
  end

  def handle_call({:registered?, run_id}, _from, state) do
    ready? =
      case Map.get(state, run_id) do
        nil -> false
        pid -> Process.alive?(pid)
      end

    {:reply, ready?, state}
  end

  @impl true
  def handle_call({:release, run_id}, _from, state) do
    case Map.pop(state, run_id) do
      {nil, _} ->
        {:reply, :not_found, state}

      {handler_pid, new_state} ->
        send(handler_pid, :release)
        broadcast_change(run_id)
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    for {run_id, handler_pid} <- state, handler_pid == pid do
      broadcast_change(run_id)
    end

    new_state =
      state
      |> Enum.reject(fn {_run_id, handler_pid} -> handler_pid == pid end)
      |> Map.new()

    {:noreply, new_state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp broadcast_change(run_id) do
    Phoenix.PubSub.broadcast(
      PgflowDemo.PubSub,
      "scenario:recovery:#{run_id}",
      {:recovery_handler_changed, run_id}
    )
  end
end
