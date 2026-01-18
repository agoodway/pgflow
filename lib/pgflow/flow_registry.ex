defmodule PgFlow.FlowRegistry do
  @moduledoc """
  Registry for flow definitions using an ETS table.

  The registry stores flow modules and their metadata, allowing flows to be
  looked up by module name or slug. This is a GenServer that manages an ETS
  table for fast concurrent reads.

  ## Usage

      # Register a flow
      :ok = PgFlow.FlowRegistry.register(MyApp.Flows.ProcessOrder)

      # Get a flow definition
      {:ok, flow_def} = PgFlow.FlowRegistry.get(MyApp.Flows.ProcessOrder)
      {:ok, flow_def} = PgFlow.FlowRegistry.get(:process_order)

      # List all flows
      flows = PgFlow.FlowRegistry.list()

      # Unregister a flow
      :ok = PgFlow.FlowRegistry.unregister(MyApp.Flows.ProcessOrder)

  """

  use GenServer
  require Logger

  @table_name :pgflow_flows

  # Client API

  @doc """
  Starts the FlowRegistry GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers a flow module in the registry.

  The flow module must implement the PgFlow.Flow behaviour (via `use PgFlow.Flow`)
  and expose flow metadata via the `__pgflow_definition__/0` callback.

  ## Examples

      :ok = PgFlow.FlowRegistry.register(MyApp.Flows.ProcessOrder)

  Returns `:ok` on success, or raises if the flow module is invalid.
  """
  @spec register(module()) :: :ok
  def register(flow_module) when is_atom(flow_module) do
    validate_flow_module!(flow_module)
    flow_def = extract_flow_definition(flow_module)
    GenServer.call(__MODULE__, {:register, flow_module, flow_def})
  end

  @doc """
  Gets a flow definition by module or slug.

  ## Examples

      {:ok, flow_def} = PgFlow.FlowRegistry.get(MyApp.Flows.ProcessOrder)
      {:ok, flow_def} = PgFlow.FlowRegistry.get(:process_order)
      {:error, :not_found} = PgFlow.FlowRegistry.get(:unknown_flow)

  """
  @spec get(module() | atom()) :: {:ok, map()} | {:error, :not_found}
  def get(flow_module_or_slug) when is_atom(flow_module_or_slug) do
    case :ets.lookup(@table_name, flow_module_or_slug) do
      [{^flow_module_or_slug, flow_def}] -> {:ok, flow_def}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Gets a flow definition by module or slug, raising if not found.

  ## Examples

      flow_def = PgFlow.FlowRegistry.get!(MyApp.Flows.ProcessOrder)

  """
  @spec get!(module() | atom()) :: map()
  def get!(flow_module_or_slug) do
    case get(flow_module_or_slug) do
      {:ok, flow_def} ->
        flow_def

      {:error, :not_found} ->
        raise ArgumentError, "flow #{inspect(flow_module_or_slug)} not found in registry"
    end
  end

  @doc """
  Lists all registered flows.

  Returns a list of flow definitions.

  ## Examples

      flows = PgFlow.FlowRegistry.list()
      #=> [%{module: MyApp.Flows.ProcessOrder, slug: :process_order, ...}, ...]

  """
  @spec list() :: [map()]
  def list do
    @table_name
    |> :ets.tab2list()
    |> Enum.filter(fn {key, _value} -> is_atom(key) and function_exported?(key, :__info__, 1) end)
    |> Enum.map(fn {_module, flow_def} -> flow_def end)
  end

  @doc """
  Unregisters a flow module from the registry.

  ## Examples

      :ok = PgFlow.FlowRegistry.unregister(MyApp.Flows.ProcessOrder)

  """
  @spec unregister(module()) :: :ok
  def unregister(flow_module) when is_atom(flow_module) do
    GenServer.call(__MODULE__, {:unregister, flow_module})
  end

  # Server Callbacks

  @impl true
  def init(_opts) do
    table = :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    Logger.debug("FlowRegistry initialized with ETS table #{inspect(table)}")
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:register, flow_module, flow_def}, _from, state) do
    slug = flow_def.slug

    # Store by both module and slug for fast lookups
    :ets.insert(@table_name, {flow_module, flow_def})
    :ets.insert(@table_name, {slug, flow_def})

    Logger.info("Registered flow: #{inspect(flow_module)} with slug #{inspect(slug)}")
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:unregister, flow_module}, _from, state) do
    case :ets.lookup(@table_name, flow_module) do
      [{^flow_module, flow_def}] ->
        slug = flow_def.slug
        :ets.delete(@table_name, flow_module)
        :ets.delete(@table_name, slug)
        Logger.info("Unregistered flow: #{inspect(flow_module)}")
        {:reply, :ok, state}

      [] ->
        {:reply, :ok, state}
    end
  end

  # Private Functions

  defp validate_flow_module!(flow_module) do
    unless Code.ensure_loaded?(flow_module) do
      raise ArgumentError, "flow module #{inspect(flow_module)} is not loaded"
    end

    unless function_exported?(flow_module, :__pgflow_definition__, 0) do
      raise ArgumentError,
            "flow module #{inspect(flow_module)} does not implement PgFlow.Flow behaviour"
    end
  end

  defp extract_flow_definition(flow_module) do
    flow_def = flow_module.__pgflow_definition__()

    %{
      module: flow_module,
      slug: flow_def.slug,
      max_attempts: flow_def.opts[:max_attempts],
      base_delay: flow_def.opts[:base_delay],
      timeout: flow_def.opts[:timeout],
      steps: flow_def.steps
    }
  end
end
