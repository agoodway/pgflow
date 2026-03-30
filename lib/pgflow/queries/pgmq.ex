defmodule PgFlow.Queries.Pgmq do
  @moduledoc """
  SQL query interface for pgmq-specific operations.

  Provides functions for managing pgmq NOTIFY triggers and querying
  pgmq extension metadata.
  """

  alias Ecto.Adapters.SQL

  @doc """
  Enables NOTIFY triggers for a pgmq queue.

  Calls `pgmq.enable_notify_insert/2` to add an INSERT trigger
  that fires NOTIFY events when messages are added to the queue.
  The `throttle_ms` parameter controls how frequently notifications fire.
  """
  @spec enable_notify_insert(module(), String.t(), non_neg_integer()) :: :ok | {:error, term()}
  def enable_notify_insert(repo, queue_name, throttle_ms) do
    case SQL.query(
           repo,
           "SELECT pgmq.enable_notify_insert($1::text, $2::integer)",
           [queue_name, throttle_ms]
         ) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Disables NOTIFY triggers for a pgmq queue.

  Calls `pgmq.disable_notify_insert/1` to remove the INSERT trigger
  that fires NOTIFY events when messages are added to the queue.
  """
  @spec disable_notify_insert(module(), String.t()) :: :ok | {:error, term()}
  def disable_notify_insert(repo, queue_name) do
    case SQL.query(repo, "SELECT pgmq.disable_notify_insert($1::text)", [queue_name]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Gets the installed pgmq version.

  Checks two sources in order:
  1. `pg_extension` catalog — works when pgmq is installed via `CREATE EXTENSION pgmq`
  2. Feature detection — checks for the `enable_notify_insert` function in the `pgmq` schema,
     which indicates pgmq >= 1.8.0 installed from a SQL dump (e.g., on Neon or other managed Postgres)

  Returns `{:ok, version}` if pgmq is detected, or `{:error, :not_installed}` if not.
  """
  @spec get_pgmq_version(module()) :: {:ok, String.t()} | {:error, :not_installed | term()}
  def get_pgmq_version(repo) do
    case get_extension_version(repo) do
      {:ok, version} -> {:ok, version}
      {:error, :not_installed} -> detect_version_by_features(repo)
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_extension_version(repo) do
    case SQL.query(repo, "SELECT extversion FROM pg_extension WHERE extname = 'pgmq'", []) do
      {:ok, %{rows: [[version]]}} -> {:ok, version}
      {:ok, %{rows: []}} -> {:error, :not_installed}
      {:error, reason} -> {:error, reason}
    end
  end

  defp detect_version_by_features(repo) do
    case SQL.query(
           repo,
           """
           SELECT EXISTS(
             SELECT 1 FROM pg_proc
             WHERE proname = 'enable_notify_insert'
             AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgmq')
           )
           """,
           []
         ) do
      {:ok, %{rows: [[true]]}} -> {:ok, "1.8.0"}
      {:ok, %{rows: [[false]]}} -> {:error, :not_installed}
      {:error, reason} -> {:error, reason}
    end
  end
end
