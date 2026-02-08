defmodule PgFlow.Test.WaitHelpers do
  @moduledoc """
  Polling-based wait helpers for tests.

  Provides utilities to wait for asynchronous conditions without using
  hard-coded `Process.sleep` calls. Uses exponential backoff with
  configurable timeout.

  ## Usage

      use PgFlow.Test.WaitHelpers

      # Wait for a condition
      wait_until(fn -> some_async_condition() end)

      # With custom timeout
      wait_until(fn -> some_condition() end, timeout: 10_000)

      # Wait for a GenServer to be running
      wait_for_genserver(MyServer)

      # Wait for a flow run to complete
      wait_for_run_completion(repo, run_id)

  """

  @default_timeout 5_000
  @initial_poll_interval 10
  @max_poll_interval 100

  defmacro __using__(_opts) do
    quote do
      import PgFlow.Test.WaitHelpers
    end
  end

  @doc """
  Waits for a condition to become true with exponential backoff.

  ## Options

    * `:timeout` - Maximum time to wait in milliseconds (default: 5000)

  ## Examples

      wait_until(fn -> GenServer.whereis(MyServer) != nil end)
      wait_until(fn -> File.exists?(path) end, timeout: 10_000)

  """
  @spec wait_until((-> boolean()), keyword()) :: :ok
  def wait_until(condition_fn, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(condition_fn, deadline, @initial_poll_interval)
  end

  defp do_wait_until(condition_fn, deadline, interval) do
    if condition_fn.() do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        raise "Timeout waiting for condition"
      else
        Process.sleep(interval)
        next_interval = min(interval * 2, @max_poll_interval)
        do_wait_until(condition_fn, deadline, next_interval)
      end
    end
  end

  @doc """
  Waits for a GenServer to be running and alive.

  ## Examples

      wait_for_genserver(MyServer)
      wait_for_genserver({:via, Registry, {MyRegistry, :key}})

  """
  @spec wait_for_genserver(GenServer.name() | pid(), keyword()) :: :ok
  def wait_for_genserver(name_or_pid, opts \\ []) do
    wait_until(
      fn ->
        case GenServer.whereis(name_or_pid) do
          nil -> false
          pid -> Process.alive?(pid)
        end
      end,
      opts
    )
  end

  @doc """
  Waits for a flow run to complete (status: completed or failed).

  ## Examples

      wait_for_run_completion(MyApp.Repo, run_id)
      wait_for_run_completion(MyApp.Repo, run_id, timeout: 10_000)

  """
  @spec wait_for_run_completion(module(), String.t(), keyword()) :: :ok
  def wait_for_run_completion(repo, run_id, opts \\ []) do
    wait_until(
      fn ->
        case PgFlow.Queries.Flows.get_run(repo, run_id) do
          {:ok, %{status: status}} -> status in ["completed", "failed"]
          _ -> false
        end
      end,
      opts
    )
  end

  @doc """
  Waits for a specific number of active tasks in a worker.

  ## Examples

      wait_for_task_count(worker_pid, 0)  # Wait for all tasks to complete
      wait_for_task_count(worker_pid, 5)  # Wait for 5 active tasks

  """
  @spec wait_for_task_count(pid(), non_neg_integer(), keyword()) :: :ok
  def wait_for_task_count(worker_pid, expected_count, opts \\ []) do
    wait_until(
      fn ->
        state = PgFlow.Worker.Server.get_state(worker_pid)
        map_size(state.active_tasks) == expected_count
      end,
      opts
    )
  end

  @doc """
  Waits for a worker to enter a specific lifecycle state.

  ## Examples

      wait_for_lifecycle_state(worker_pid, :running)
      wait_for_lifecycle_state(worker_pid, :stopped)

  """
  @spec wait_for_lifecycle_state(pid(), atom(), keyword()) :: :ok
  def wait_for_lifecycle_state(worker_pid, expected_state, opts \\ []) do
    wait_until(
      fn ->
        state = PgFlow.Worker.Server.get_state(worker_pid)
        state.lifecycle.state == expected_state
      end,
      opts
    )
  end

  @doc """
  Waits until the mailbox of the current process contains a message matching the pattern.

  Note: This doesn't consume the message, just checks if it's present.

  ## Examples

      wait_for_message(:poll_now)
      wait_for_message({:DOWN, _, :process, _, _})

  """
  defmacro wait_for_message(pattern, opts \\ []) do
    quote do
      PgFlow.Test.WaitHelpers.wait_until(
        fn ->
          {:messages, messages} = Process.info(self(), :messages)

          Enum.any?(messages, fn msg ->
            match?(unquote(pattern), msg)
          end)
        end,
        unquote(opts)
      )
    end
  end
end
