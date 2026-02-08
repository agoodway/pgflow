defmodule PgFlowDashboard.Cache.MetricsCache do
  @moduledoc """
  ETS-based cache for expensive dashboard aggregations.

  Provides TTL-based caching with automatic expiration to reduce
  database load for frequently accessed metrics.
  """

  use GenServer

  @table_name :pgflow_dashboard_cache
  @default_ttl 5_000
  @telemetry_handler_id "pgflow-dashboard-cache-invalidation"

  # Client API

  @doc """
  Starts the metrics cache.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Gets a cached value, or computes and caches it if not present or expired.

  Uses a "single-flight" pattern to prevent cache stampede: when a cache miss
  occurs, the computation is serialized through the GenServer so only one
  caller computes the value while others wait for the result.

  ## Options

    * `:ttl` - Time-to-live in milliseconds. Default: 5000ms.

  ## Examples

      MetricsCache.fetch(:overview_metrics, fn -> Queries.get_overview_metrics(repo) end)

  """
  @spec fetch(term(), (-> term()), keyword()) :: term()
  def fetch(key, compute_fn, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, @default_ttl)

    case get(key) do
      {:ok, value} ->
        # Fast path: cache hit, no GenServer call needed
        value

      :miss ->
        # Slow path: serialize through GenServer to prevent stampede
        GenServer.call(__MODULE__, {:compute_and_cache, key, compute_fn, ttl})
    end
  end

  defp get(key) do
    case :ets.lookup(@table_name, key) do
      [{^key, value, expires_at}] ->
        if System.monotonic_time(:millisecond) < expires_at do
          {:ok, value}
        else
          # Expired, delete it
          :ets.delete(@table_name, key)
          :miss
        end

      [] ->
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  defp put(key, value, ttl) do
    expires_at = System.monotonic_time(:millisecond) + ttl
    :ets.insert(@table_name, {key, value, expires_at})
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp invalidate(key) do
    :ets.delete(@table_name, key)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Invalidates all cache entries matching a pattern.

  ## Examples

      MetricsCache.invalidate_pattern(:overview_metrics)
      MetricsCache.invalidate_pattern({:flow_stats, _})

  """
  @spec invalidate_pattern(term()) :: :ok
  def invalidate_pattern(pattern) do
    match_spec = [{{pattern, :_, :_}, [], [true]}]

    :ets.select_delete(@table_name, match_spec)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    table = :ets.new(@table_name, [:named_table, :public, :set, read_concurrency: true])
    attach_telemetry()
    schedule_cleanup()
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:compute_and_cache, key, compute_fn, ttl}, _from, state) do
    # Double-check pattern: another caller may have computed while we waited in the queue
    result =
      case get(key) do
        {:ok, value} ->
          value

        :miss ->
          value = compute_fn.()
          put(key, value, ttl)
          value
      end

    {:reply, result, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_expired()
    schedule_cleanup()
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :telemetry.detach(@telemetry_handler_id)
    :ok
  end

  # Telemetry-based cache invalidation
  # Listens directly to run lifecycle events and invalidates relevant cache entries.

  defp attach_telemetry do
    events = [
      [:pgflow, :run, :started],
      [:pgflow, :run, :completed],
      [:pgflow, :run, :failed]
    ]

    :telemetry.attach_many(
      @telemetry_handler_id,
      events,
      &__MODULE__.handle_telemetry_event/4,
      nil
    )
  end

  @doc false
  def handle_telemetry_event([:pgflow, :run, :started], _measurements, _metadata, _config) do
    invalidate(:overview_metrics)
  end

  def handle_telemetry_event([:pgflow, :run, :completed], _measurements, metadata, _config) do
    invalidate(:overview_metrics)
    invalidate({:flow_stats, metadata[:flow_slug]})
  end

  def handle_telemetry_event([:pgflow, :run, :failed], _measurements, metadata, _config) do
    invalidate(:overview_metrics)
    invalidate({:flow_stats, metadata[:flow_slug]})
  end

  # Private

  defp schedule_cleanup do
    # Run cleanup every 60 seconds
    Process.send_after(self(), :cleanup, 60_000)
  end

  defp cleanup_expired do
    now = System.monotonic_time(:millisecond)

    # Select and delete all expired entries
    match_spec = [{{:"$1", :"$2", :"$3"}, [{:<, :"$3", now}], [:"$1"]}]

    keys = :ets.select(@table_name, match_spec)

    for key <- keys do
      :ets.delete(@table_name, key)
    end
  rescue
    ArgumentError -> :ok
  end
end
