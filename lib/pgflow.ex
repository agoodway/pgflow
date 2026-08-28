defmodule PgFlow do
  @pgflow_core_version "0.5.0"

  @moduledoc """
  PgFlow is an Elixir implementation of the pgflow workflow engine.

  It provides a macro-based DSL for defining workflow DAGs that execute
  on PostgreSQL using pgmq for task coordination.

  ## Compatibility

  This Elixir implementation is compatible with pgflow core version #{@pgflow_core_version}.
  It uses the same database schema and SQL functions as the TypeScript/Deno implementation,
  allowing both to run side-by-side against the same database.

  ## Quick Start

      defmodule MyApp.Flows.ProcessOrder do
        use PgFlow.Flow

        @flow slug: :process_order, max_attempts: 3

        step :validate do
          fn input, _ctx ->
            # Validate order data
            %{valid: true, order_id: input["order_id"]}
          end
        end

        step :charge, depends_on: [:validate] do
          fn deps, _ctx ->
            # Charge the customer
            %{charged: true, amount: 100}
          end
        end

        step :fulfill, depends_on: [:charge] do
          fn deps, _ctx ->
            # Fulfill the order
            %{fulfilled: true}
          end
        end
      end

  ## Starting a Flow

      {:ok, run_id} = PgFlow.start_flow(MyApp.Flows.ProcessOrder, %{"order_id" => 123})

  ## Configuration

  Add PgFlow to your supervision tree:

      children = [
        {PgFlow, repo: MyApp.Repo, flows: [MyApp.Flows.ProcessOrder]}
      ]

  See `PgFlow.Config` for configuration options.
  """

  alias PgFlow.{Client, Config, FlowRegistry, WorkerSupervisor}

  @doc """
  Returns a child specification for starting PgFlow under a supervisor.

  ## Options

  See `PgFlow.Config` for available options.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Starts the PgFlow supervision tree.

  ## Options

  See `PgFlow.Config` for available options.

  ## Examples

      PgFlow.start_link(repo: MyApp.Repo, flows: [MyApp.Flows.ProcessOrder])

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    config = Config.validate!(opts)
    PgFlow.Supervisor.start_link(config)
  end

  @doc """
  Starts a flow run with the given input.

  The flow can be specified by module name, slug atom, or slug string.

  ## Examples

      {:ok, run_id} = PgFlow.start_flow(MyApp.Flows.ProcessOrder, %{"order_id" => 123})
      {:ok, run_id} = PgFlow.start_flow(:process_order, %{"order_id" => 123})

  """
  @spec start_flow(module() | atom() | String.t(), map()) :: {:ok, String.t()} | {:error, term()}
  defdelegate start_flow(flow_module_or_slug, input), to: Client

  @doc """
  Starts a flow and waits for completion.

  Blocks until the flow completes or the timeout is reached.

  ## Options

    * `:timeout` - Maximum time to wait in milliseconds (default: 60_000)
    * `:poll_interval` - How often to check status in milliseconds (default: 500)

  ## Examples

      {:ok, run} = PgFlow.start_flow_sync(MyApp.Flows.ProcessOrder, %{"order_id" => 123})
      {:error, run} = PgFlow.start_flow_sync(MyApp.Flows.FailingFlow, %{})

  """
  @spec start_flow_sync(module() | atom() | String.t(), map(), keyword()) ::
          {:ok, PgFlow.Schema.Run.t()}
          | {:error, PgFlow.Schema.Run.t()}
          | {:error, :timeout}
          | {:error, term()}
  defdelegate start_flow_sync(flow_module_or_slug, input, opts \\ []), to: Client

  @doc """
  Gets a run by ID.

  ## Examples

      {:ok, run} = PgFlow.get_run("550e8400-e29b-41d4-a716-446655440000")
      {:error, :invalid_id} = PgFlow.get_run("not-a-uuid")
      {:error, :not_found} = PgFlow.get_run(Ecto.UUID.generate())

  """
  @spec get_run(String.t()) ::
          {:ok, PgFlow.Schema.Run.t()} | {:error, :invalid_id | :not_found | term()}
  defdelegate get_run(run_id), to: Client

  @doc """
  Gets a run with all step states preloaded.

  ## Examples

      {:ok, run} = PgFlow.get_run_with_states("550e8400-e29b-41d4-a716-446655440000")
      run.step_states  # => [%StepState{}, ...]

  """
  @spec get_run_with_states(String.t()) ::
          {:ok, PgFlow.Schema.Run.t()} | {:error, :invalid_id | :not_found | term()}
  defdelegate get_run_with_states(run_id), to: Client

  @doc """
  Recompiles a flow definition at runtime.

  Unlike the compile-time DSL (`use PgFlow.Flow`), this creates flow
  definitions from plain data - for per-tenant automations and dynamic workflows.

  If the flow already exists, this operation is destructive: existing
  definition and historical run/task data for the slug are deleted first.

  ## Examples

       PgFlow.upsert_flow("acct_123_hubspot_sync_v1",
         max_attempts: 3,
         steps: [
           %{slug: "reshape", deps: []},
           %{slug: "create_contact", deps: ["reshape"]}
         ]
       )

  """
  @spec upsert_flow(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate upsert_flow(slug, opts), to: Client

  @doc """
  Deletes a flow and all associated data (runs, tasks, queue).

  ## Examples

      PgFlow.delete_flow("acct_123_hubspot_sync_v1")

  """
  @spec delete_flow(String.t()) :: :ok | {:error, term()}
  defdelegate delete_flow(slug), to: Client

  @doc """
  Checks if a flow exists in the database.

  ## Examples

      PgFlow.flow_exists?("my_flow")
      # => {:ok, true}

  """
  @spec flow_exists?(String.t()) :: {:ok, boolean()} | {:error, term()}
  defdelegate flow_exists?(slug), to: Client

  @doc """
  Enqueues a background job with the given input.

  This is the primary API for dispatching jobs. Under the hood, jobs are
  single-step flows, so this delegates to `start_flow/2`.

  ## Examples

      {:ok, run_id} = PgFlow.enqueue(MyApp.Jobs.SendEmail, %{"to" => "user@example.com"})

  """
  @spec enqueue(module(), map()) :: {:ok, String.t()} | {:error, term()}
  def enqueue(job_module, input) when is_atom(job_module) and is_map(input) do
    Client.enqueue(job_module, input)
  end

  @doc """
  Enqueues a background job with options.

  Supported options:

    * `:delay_seconds` - non-negative integer seconds before the job is available
    * `:scheduled_at` - `DateTime` when the job should become available

  ## Examples

      {:ok, run_id} = PgFlow.enqueue(MyApp.Jobs.SendEmail, %{"to" => "user@example.com"}, delay_seconds: 60)
      {:ok, run_id} = PgFlow.enqueue(MyApp.Jobs.SendEmail, %{"to" => "user@example.com"}, scheduled_at: ~U[2026-05-08 12:00:00Z])

  """
  @spec enqueue(module(), map(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def enqueue(job_module, input, opts) when is_atom(job_module) and is_map(input) do
    Client.enqueue(job_module, input, opts)
  end

  @doc """
  Enqueues a background job that becomes available after `delay_seconds`.

  PgFlow persists the run immediately, then delays the initial pgmq task
  visibility so workers cannot execute it until the delay elapses. Values of
  `0` enqueue the job for immediate execution.

  ## Examples

      {:ok, run_id} = PgFlow.enqueue_in(MyApp.Jobs.SendEmail, %{"to" => "user@example.com"}, 60)

  """
  @spec enqueue_in(module(), map(), non_neg_integer()) :: {:ok, String.t()} | {:error, term()}
  def enqueue_in(job_module, input, delay_seconds)
      when is_atom(job_module) and is_map(input) do
    Client.enqueue_in(job_module, input, delay_seconds)
  end

  @doc """
  Enqueues a background job that becomes available at `scheduled_at`.

  Timestamps in the past enqueue the job for immediate execution. Any `DateTime`
  time zone is accepted; PgFlow compares the scheduled timestamp as an instant.

  ## Examples

      {:ok, run_id} = PgFlow.enqueue_at(MyApp.Jobs.SendEmail, %{"to" => "user@example.com"}, ~U[2026-05-08 12:00:00Z])

  """
  @spec enqueue_at(module(), map(), DateTime.t()) :: {:ok, String.t()} | {:error, term()}
  def enqueue_at(job_module, input, scheduled_at)
      when is_atom(job_module) and is_map(input) do
    Client.enqueue_at(job_module, input, scheduled_at)
  end

  @doc """
  Starts a worker for the given flow.

  ## Options

    * `:poll_interval` - How often to poll for messages (default: 1000ms)
    * `:visibility_timeout` - How long to hold messages (default: 30s)

  ## Examples

      {:ok, pid} = PgFlow.start_worker(MyApp.Flows.ProcessOrder)

  """
  @spec start_worker(module(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_worker(flow_module, opts \\ []) do
    WorkerSupervisor.start_worker(flow_module, opts)
  end

  @doc """
  Stops a worker for the given flow.

  ## Examples

      :ok = PgFlow.stop_worker(MyApp.Flows.ProcessOrder)

  """
  @spec stop_worker(module()) :: :ok | {:error, :not_found}
  def stop_worker(flow_module) do
    WorkerSupervisor.stop_worker(flow_module)
  end

  @doc """
  Lists all registered flows.

  ## Examples

      flows = PgFlow.list_flows()
      #=> [%{module: MyApp.Flows.ProcessOrder, slug: :process_order, ...}, ...]

  """
  @spec list_flows() :: [map()]
  def list_flows do
    FlowRegistry.list()
  end

  @doc """
  Gets the definition for a flow by module or slug.

  ## Examples

      {:ok, flow_def} = PgFlow.get_flow(MyApp.Flows.ProcessOrder)
      {:ok, flow_def} = PgFlow.get_flow(:process_order)
      {:error, :not_found} = PgFlow.get_flow(:unknown)

  """
  @spec get_flow(module() | atom()) :: {:ok, map()} | {:error, :not_found}
  def get_flow(flow_module_or_slug) do
    FlowRegistry.get(flow_module_or_slug)
  end

  @doc """
  Returns health check information.

  ## Examples

      PgFlow.health_check()
      #=> %{status: :ok, workers: [...], flows: [...]}

  """
  @spec health_check() :: %{status: :ok, workers: [map()], flows: [map()]}
  def health_check do
    %{
      status: :ok,
      workers: WorkerSupervisor.list_workers(),
      flows: FlowRegistry.list()
    }
  end

  @doc """
  Returns the compatible pgflow core version.

  This version indicates which pgflow database schema and SQL functions
  this Elixir implementation is compatible with.

  ## Examples

      PgFlow.core_version()
      #=> "0.5.0"

  """
  @spec core_version() :: String.t()
  def core_version, do: @pgflow_core_version
end
