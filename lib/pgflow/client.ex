defmodule PgFlow.Client do
  @moduledoc """
  Client API for interacting with PgFlow runs.

  Provides functions to start flows, query run status, and wait for completion.

  ## Usage

      # Start a flow asynchronously
      {:ok, run_id} = PgFlow.Client.start_flow(:my_flow, %{"order_id" => 123})

      # Start a flow and wait for completion
      {:ok, run} = PgFlow.Client.start_flow_sync(:my_flow, %{"order_id" => 123}, timeout: 30_000)

      # Get run details
      {:ok, run} = PgFlow.Client.get_run(run_id)

      # Get run with step states preloaded
      {:ok, run} = PgFlow.Client.get_run_with_states(run_id)

  """

  import Ecto.Query

  alias PgFlow.Queries
  alias PgFlow.Schema.Run

  @doc """
  Starts a flow run with the given input.

  Calls the `pgflow.start_flow` SQL function which handles all initialization:
  - Creates run and step_states records
  - Handles map step initial_tasks
  - Broadcasts run:started event
  - Enqueues ready steps to pgmq
  - Handles empty cascades

  The flow can be specified by module name or slug atom/string.
  Returns `{:ok, run_id}` on success or `{:error, reason}` on failure.

  ## Examples

      {:ok, run_id} = PgFlow.Client.start_flow(:process_order, %{"order_id" => 123})
      {:ok, run_id} = PgFlow.Client.start_flow("process_order", %{"order_id" => 123})
      {:ok, run_id} = PgFlow.Client.start_flow(MyApp.Flows.ProcessOrder, %{"order_id" => 123})

  """
  @spec start_flow(module() | atom() | String.t(), map()) :: {:ok, String.t()} | {:error, term()}
  def start_flow(flow_module_or_slug, input) when is_map(input) do
    with {:ok, repo} <- get_repo(),
         {:ok, flow_slug} <- resolve_slug(flow_module_or_slug) do
      Queries.start_flow(repo, flow_slug, input)
    end
  end

  @doc """
  Starts a flow and waits for completion.

  Blocks until the flow completes or the timeout is reached. Returns the
  completed run on success or error.

  ## Options

    * `:timeout` - Maximum time to wait in milliseconds (default: 60_000)
    * `:poll_interval` - How often to check status in milliseconds (default: 500)

  ## Examples

      {:ok, run} = PgFlow.Client.start_flow_sync(:my_flow, %{"order_id" => 123})
      {:error, run} = PgFlow.Client.start_flow_sync(:failing_flow, %{})
      {:error, :timeout} = PgFlow.Client.start_flow_sync(:slow_flow, %{}, timeout: 1000)

  """
  @spec start_flow_sync(module() | atom() | String.t(), map(), keyword()) ::
          {:ok, Run.t()} | {:error, Run.t()} | {:error, :timeout} | {:error, term()}
  def start_flow_sync(flow_module_or_slug, input, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    poll_interval = Keyword.get(opts, :poll_interval, 500)

    with {:ok, run_id} <- start_flow(flow_module_or_slug, input),
         {:ok, repo} <- get_repo() do
      wait_for_completion(repo, run_id, timeout, poll_interval)
    end
  end

  @doc """
  Gets a run by ID.

  Returns `{:ok, run}` if found, or `{:error, :not_found}` if the run does not exist.

  ## Examples

      {:ok, run} = PgFlow.Client.get_run(run_id)
      {:error, :not_found} = PgFlow.Client.get_run("00000000-0000-0000-0000-000000000000")

  """
  @spec get_run(String.t()) :: {:ok, Run.t()} | {:error, :not_found} | {:error, term()}
  def get_run(run_id) do
    with {:ok, repo} <- get_repo() do
      case repo.get(Run, run_id) do
        nil -> {:error, :not_found}
        run -> {:ok, run}
      end
    end
  end

  @doc """
  Gets a run with all step states preloaded.

  Returns `{:ok, run}` with step_states association loaded, or `{:error, :not_found}`
  if the run does not exist.

  ## Examples

      {:ok, run} = PgFlow.Client.get_run_with_states(run_id)
      Enum.each(run.step_states, fn state ->
        IO.puts("\#{state.step_slug}: \#{state.status}")
      end)

  """
  @spec get_run_with_states(String.t()) ::
          {:ok, Run.t()} | {:error, :not_found} | {:error, term()}
  def get_run_with_states(run_id) do
    with {:ok, repo} <- get_repo() do
      query =
        from(r in Run,
          where: r.run_id == ^run_id,
          preload: [:step_states]
        )

      case repo.one(query) do
        nil -> {:error, :not_found}
        run -> {:ok, run}
      end
    end
  end

  # Private Functions

  defp get_repo do
    # Try persistent_term first (set by supervisor), then application env
    case :persistent_term.get({PgFlow, :repo}, nil) do
      nil ->
        case Application.get_env(:pgflow, :repo) do
          nil -> {:error, "Repo not configured"}
          repo -> {:ok, repo}
        end

      repo ->
        {:ok, repo}
    end
  end

  defp resolve_slug(module) when is_atom(module) do
    if function_exported?(module, :__pgflow_slug__, 0) do
      {:ok, Atom.to_string(module.__pgflow_slug__())}
    else
      {:ok, Atom.to_string(module)}
    end
  end

  defp resolve_slug(slug) when is_binary(slug) do
    {:ok, slug}
  end

  defp wait_for_completion(repo, run_id, timeout, poll_interval) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_loop(repo, run_id, deadline, poll_interval)
  end

  defp wait_loop(repo, run_id, deadline, poll_interval) do
    case repo.get(Run, run_id) do
      nil ->
        {:error, :not_found}

      %Run{status: "completed"} = run ->
        {:ok, run}

      %Run{status: "failed"} = run ->
        {:error, run}

      %Run{} ->
        now = System.monotonic_time(:millisecond)

        if now >= deadline do
          {:error, :timeout}
        else
          Process.sleep(poll_interval)
          wait_loop(repo, run_id, deadline, poll_interval)
        end
    end
  end
end
