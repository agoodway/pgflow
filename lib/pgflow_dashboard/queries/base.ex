defmodule PgFlowDashboard.Queries.Base do
  @moduledoc """
  Common query helpers for RPC-style database calls.

  Provides utilities for executing PostgreSQL functions and handling
  common result patterns in a consistent way.
  """

  @doc """
  Executes a PostgreSQL function call and handles common result patterns.

  ## Options

    * `:mode` - Result handling mode:
      * `:single` - Expect single row, return `{:ok, map}` or `{:error, :not_found}`
      * `:list` - Expect multiple rows, return list of maps (default)
      * `:count` - Expect single value, return integer
      * `:raw` - Return the raw `{:ok, result}` or `{:error, reason}`

  ## Examples

      # List query (returns list of maps)
      execute_rpc(repo, "list_runs", [time_start, nil, nil, 50, nil])

      # Single item query (returns {:ok, map} or {:error, :not_found})
      execute_rpc(repo, "get_run", [run_id], mode: :single)

      # Count query (returns integer)
      execute_rpc(repo, "count_runs", [time_start, nil, nil], mode: :count)

  """
  @spec execute_rpc(module(), String.t(), list(), keyword()) ::
          {:ok, map()} | {:error, :not_found | term()} | list(map()) | integer()
  def execute_rpc(repo, function_name, params, opts \\ []) do
    mode = Keyword.get(opts, :mode, :list)
    placeholders = build_placeholders(params)
    query = "SELECT * FROM pgflow_dashboard.#{function_name}(#{placeholders})"

    case repo.query(query, params) do
      {:ok, %{rows: rows, columns: columns}} ->
        handle_result(rows, columns, mode)

      {:error, reason} ->
        error_result(mode, reason)
    end
  end

  defp build_placeholders([]), do: ""

  defp build_placeholders(params) do
    1..length(params)
    |> Enum.map_join(", ", &"$#{&1}")
  end

  defp handle_result([], _columns, :single), do: {:error, :not_found}
  defp handle_result([], _columns, :list), do: []
  defp handle_result([[nil]], _columns, :single), do: {:error, :not_found}
  defp handle_result([[count]], _columns, :count) when is_integer(count), do: count
  defp handle_result([[nil]], _columns, :count), do: 0
  defp handle_result([row], columns, :single), do: {:ok, row_to_map(row, columns)}
  defp handle_result(rows, columns, :list), do: rows_to_maps(rows, columns)
  defp handle_result(rows, columns, :raw), do: {:ok, rows_to_maps(rows, columns)}

  defp error_result(:single, _reason), do: {:error, :not_found}
  defp error_result(:list, _reason), do: []
  defp error_result(:count, _reason), do: 0
  defp error_result(:raw, reason), do: {:error, reason}

  # ===================
  # Row Conversion
  # ===================

  @doc """
  Converts a list of database rows to a list of maps.
  """
  @spec rows_to_maps(list(), list(String.t())) :: list(map())
  def rows_to_maps(rows, columns) do
    Enum.map(rows, &row_to_map(&1, columns))
  end

  @doc """
  Converts a single database row to a map.
  """
  @spec row_to_map(list(), list(String.t())) :: map()
  def row_to_map(row, columns) do
    columns
    |> Enum.zip(row)
    |> Map.new(fn {col, val} -> {String.to_atom(col), maybe_format_uuid(col, val)} end)
  end

  # Format UUID columns to string representation
  defp maybe_format_uuid(col, val)
       when col in ["run_id", "worker_id"] and is_binary(val) and byte_size(val) == 16 do
    format_uuid(val)
  end

  defp maybe_format_uuid(_col, val), do: val

  # ===================
  # UUID Helpers
  # ===================

  @doc """
  Parses a UUID string to binary format for database queries.
  """
  @spec parse_uuid(nil | String.t() | binary()) :: nil | binary()
  def parse_uuid(nil), do: nil

  def parse_uuid(uuid) when is_binary(uuid) do
    case Ecto.UUID.dump(uuid) do
      {:ok, binary} -> binary
      :error -> uuid
    end
  end

  def parse_uuid(uuid), do: uuid

  @doc """
  Formats a binary UUID to string representation.
  """
  @spec format_uuid(nil | binary()) :: nil | String.t()
  def format_uuid(nil), do: nil

  def format_uuid(uuid) when is_binary(uuid) and byte_size(uuid) == 16 do
    case Ecto.UUID.load(uuid) do
      {:ok, str} -> str
      :error -> Base.encode16(uuid, case: :lower)
    end
  end

  def format_uuid(uuid), do: uuid

  # ===================
  # Time Range Helpers
  # ===================

  @doc """
  Converts a time range atom to a DateTime.
  """
  @spec time_range_start(atom()) :: DateTime.t()
  def time_range_start(:last_hour), do: DateTime.add(DateTime.utc_now(), -1, :hour)
  def time_range_start(:last_24h), do: DateTime.add(DateTime.utc_now(), -24, :hour)
  def time_range_start(:last_7d), do: DateTime.add(DateTime.utc_now(), -7, :day)
  def time_range_start(:last_30d), do: DateTime.add(DateTime.utc_now(), -30, :day)
  def time_range_start(_), do: DateTime.add(DateTime.utc_now(), -24, :hour)

  # ===================
  # Status Helpers
  # ===================

  @doc """
  Converts a status atom or string to string for database queries.
  """
  @spec status_to_string(nil | atom() | String.t()) :: nil | String.t()
  def status_to_string(nil), do: nil
  def status_to_string(status) when is_atom(status), do: Atom.to_string(status)
  def status_to_string(status) when is_binary(status), do: status

  @doc """
  Converts a health status atom or string to string for database queries.
  """
  @spec health_status_to_string(nil | atom() | String.t()) :: nil | String.t()
  def health_status_to_string(nil), do: nil
  def health_status_to_string(status) when is_atom(status), do: Atom.to_string(status)
  def health_status_to_string(status) when is_binary(status), do: status

  @doc """
  Converts a direction atom to string for database queries.
  """
  @spec direction_to_string(:next | :prev | String.t()) :: String.t()
  def direction_to_string(:next), do: "next"
  def direction_to_string(:prev), do: "prev"
  def direction_to_string(dir) when is_binary(dir), do: dir
end
