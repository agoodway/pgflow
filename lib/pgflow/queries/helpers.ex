defmodule PgFlow.Queries.Helpers do
  @moduledoc """
  Shared utilities for database queries.

  Provides RPC-style PostgreSQL function calls, UUID handling, row-to-map
  conversion, time range calculations, and status conversions.

  Used by both PgFlow core query modules and the PgFlow Dashboard.
  """

  # ===================
  # RPC Helpers
  # ===================

  @doc """
  Executes an RPC-style call to a PostgreSQL function.

  ## Parameters

    * `repo` - The Ecto repository
    * `function_name` - Name of the PostgreSQL function
    * `params` - List of parameters to pass
    * `opts` - Options keyword list

  ## Options

    * `:schema` - (required) The PostgreSQL schema containing the function
    * `:mode` - Result handling mode: `:list`, `:single`, `:count`, `:void`, `:raw`

  ## Returns

  Depends on mode:
    * `:list` - Returns list of maps
    * `:single` - Returns `{:ok, map}` or `{:error, :not_found}`
    * `:count` - Returns integer count
    * `:void` - Returns `{:ok, nil}`
    * `:raw` - Returns `{:ok, list of maps}` or error
  """
  @spec execute_rpc(module(), String.t(), list(), keyword()) ::
          {:ok, nil | map() | [map()]} | {:error, :not_found | term()} | [map()] | integer()
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

  defp rows_to_maps(rows, columns) do
    Enum.map(rows, &row_to_map(&1, columns))
  end

  defp row_to_map(row, columns) do
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
  Casts a UUID to its canonical string form and fails closed for invalid IDs.
  """
  @spec cast_uuid(term()) :: {:ok, Ecto.UUID.t()} | {:error, :invalid_id}
  def cast_uuid(uuid) do
    case Ecto.UUID.cast(uuid) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> {:error, :invalid_id}
    end
  end

  @doc """
  Casts an optional UUID while preserving `nil`.
  """
  @spec optional_uuid(term()) :: {:ok, Ecto.UUID.t() | nil} | {:error, :invalid_id}
  def optional_uuid(nil), do: {:ok, nil}
  def optional_uuid(uuid), do: cast_uuid(uuid)

  @doc """
  Normalizes an atom or string flow slug and rejects unsupported values.

  This only normalizes the Elixir value. Callers that interpolate a slug into
  SQL must additionally validate it with PgFlow's database slug rules.
  """
  @spec cast_flow_slug(term()) :: {:ok, String.t()} | {:error, :invalid_flow_slug}
  def cast_flow_slug(nil), do: {:error, :invalid_flow_slug}
  def cast_flow_slug(true), do: {:error, :invalid_flow_slug}
  def cast_flow_slug(false), do: {:error, :invalid_flow_slug}
  def cast_flow_slug(flow_slug) when is_atom(flow_slug), do: {:ok, Atom.to_string(flow_slug)}
  def cast_flow_slug(flow_slug) when is_binary(flow_slug), do: {:ok, flow_slug}
  def cast_flow_slug(_flow_slug), do: {:error, :invalid_flow_slug}

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

  @doc """
  Returns a positive `:limit` option or the supplied positive default.
  """
  @spec positive_limit(keyword(), pos_integer()) :: pos_integer()
  def positive_limit(opts, default) when is_integer(default) and default > 0 do
    case Keyword.get(opts, :limit, default) do
      limit when is_integer(limit) and limit > 0 -> limit
      _invalid -> default
    end
  end

  # ===================
  # Time Range Helpers
  # ===================

  @doc """
  Converts a time range atom to a start DateTime.
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
