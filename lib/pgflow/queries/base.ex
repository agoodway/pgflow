defmodule PgFlow.Queries.Base do
  @moduledoc """
  Common query helpers for RPC-style database calls.

  Provides utilities for executing PostgreSQL functions and handling
  common result patterns in a consistent way.

  ## Options

    * `:schema` - (required) The PostgreSQL schema to call functions in
    * `:mode` - Result handling mode:
      * `:single` - Expect single row, return `{:ok, map}` or `{:error, :not_found}`
      * `:list` - Expect multiple rows, return list of maps (default)
      * `:count` - Expect single value, return integer
      * `:raw` - Return the raw `{:ok, result}` or `{:error, reason}`
      * `:void` - For write functions returning void, return `{:ok, nil}`

  """

  @spec execute_rpc(module(), String.t(), list(), keyword()) ::
          {:ok, map()} | {:error, :not_found | term()} | list(map()) | integer()
  def execute_rpc(repo, function_name, params, opts \\ []) do
    schema = Keyword.fetch!(opts, :schema)
    mode = Keyword.get(opts, :mode, :list)
    placeholders = build_placeholders(params)
    query = "SELECT * FROM #{schema}.#{function_name}(#{placeholders})"

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

  defp handle_result(_rows, _columns, :void), do: {:ok, nil}
  defp handle_result([], _columns, :single), do: {:error, :not_found}
  defp handle_result([], _columns, :list), do: []
  defp handle_result([[nil]], _columns, :single), do: {:error, :not_found}
  defp handle_result([[count]], _columns, :count) when is_integer(count), do: count
  defp handle_result([[nil]], _columns, :count), do: 0
  defp handle_result([row], columns, :single), do: {:ok, row_to_map(row, columns)}
  defp handle_result(rows, columns, :list), do: rows_to_maps(rows, columns)
  defp handle_result(rows, columns, :raw), do: {:ok, rows_to_maps(rows, columns)}

  defp error_result(:void, reason), do: {:error, reason}
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
  Converts a time range atom to a DateTime using Timex.shift.
  """
  @spec time_range_start(atom()) :: DateTime.t()
  def time_range_start(:last_hour), do: Timex.shift(DateTime.utc_now(), hours: -1)
  def time_range_start(:last_24h), do: Timex.shift(DateTime.utc_now(), hours: -24)
  def time_range_start(:last_7d), do: Timex.shift(DateTime.utc_now(), days: -7)
  def time_range_start(:last_30d), do: Timex.shift(DateTime.utc_now(), days: -30)
  def time_range_start(_), do: Timex.shift(DateTime.utc_now(), hours: -24)

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
