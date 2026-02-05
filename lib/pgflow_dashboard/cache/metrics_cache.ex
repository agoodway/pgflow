defmodule PgFlowDashboard.Cache.MetricsCache do
  @moduledoc """
  ETS-based cache for expensive dashboard aggregations.

  Provides TTL-based caching with automatic expiration to reduce
  database load for frequently accessed metrics.
  """

  use GenServer

  @table_name :pgflow_dashboard_cache
  @default_ttl 5_000

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

  @doc """
  Gets a value from the cache.

  Returns `{:ok, value}` if found and not expired, `:miss` otherwise.
  """
  @spec get(term()) :: {:ok, term()} | :miss
  def get(key) do
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

  @doc """
  Puts a value in the cache with a TTL.
  """
  @spec put(term(), term(), pos_integer()) :: :ok
  def put(key, value, ttl \\ @default_ttl) do
    expires_at = System.monotonic_time(:millisecond) + ttl
    :ets.insert(@table_name, {key, value, expires_at})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Invalidates a specific cache key.
  """
  @spec invalidate(term()) :: :ok
  def invalidate(key) do
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

  @doc """
  Clears all cached entries.
  """
  @spec clear() :: :ok
  def clear do
    :ets.delete_all_objects(@table_name)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    table = :ets.new(@table_name, [:named_table, :public, :set, read_concurrency: true])
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
