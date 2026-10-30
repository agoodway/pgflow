defmodule PgFlow.Slug do
  @moduledoc """
  Upstream pgflow 0.17.2 slug rules (`pgflow.is_valid_slug/1`), mirrored for
  compile-time and pre-flight checks. The database remains authoritative.
  """

  @max_slug_length 128
  @max_queue_name_length 47
  @reserved ~w(run)

  @type reason ::
          :empty | :too_long | :bad_chars | :reserved | :bad_start | :bad_end | :double_underscore

  @doc "Maximum flow or step slug length accepted by pgflow 0.17.2."
  @spec max_slug_length() :: pos_integer()
  def max_slug_length, do: @max_slug_length

  @doc "Maximum PGMQ queue name length. Flow slugs are lowercased into this budget."
  @spec max_queue_name_length() :: pos_integer()
  def max_queue_name_length, do: @max_queue_name_length

  @doc "Returns true when `slug` satisfies every upstream slug rule."
  @spec valid?(String.t() | atom()) :: boolean()
  def valid?(slug), do: validate(slug) == :ok

  @doc "Validates a flow or step slug and returns the first broken rule."
  @spec validate(String.t() | atom()) :: :ok | {:error, reason()}
  def validate(slug) when is_atom(slug), do: validate(Atom.to_string(slug))
  def validate(""), do: {:error, :empty}

  def validate(slug) when is_binary(slug) do
    cond do
      String.length(slug) > @max_slug_length -> {:error, :too_long}
      not Regex.match?(~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/, slug) -> {:error, :bad_chars}
      slug in @reserved -> {:error, :reserved}
      not Regex.match?(~r/\A[a-zA-Z]/, slug) -> {:error, :bad_start}
      not Regex.match?(~r/[a-zA-Z0-9]\z/, slug) -> {:error, :bad_end}
      String.contains?(slug, "__") -> {:error, :double_underscore}
      true -> :ok
    end
  end

  @doc "Returns the flow-mode queue name, which is the lowercased slug."
  @spec queue_name(String.t() | atom()) :: String.t()
  def queue_name(slug) when is_atom(slug), do: queue_name(Atom.to_string(slug))
  def queue_name(slug) when is_binary(slug), do: String.downcase(slug)

  @doc "Rejects a queue name longer than the PGMQ limit."
  @spec validate_queue_name(String.t()) :: :ok | {:error, :queue_name_too_long}
  def validate_queue_name(name) when byte_size(name) <= @max_queue_name_length, do: :ok
  def validate_queue_name(_name), do: {:error, :queue_name_too_long}

  @doc "Turns a validation reason into the message shown by the DSL and slug audit."
  @spec describe(reason() | :queue_name_too_long) :: String.t()
  def describe(:empty), do: "must not be empty"
  def describe(:too_long), do: "must be at most #{@max_slug_length} characters"
  def describe(:bad_chars), do: "must match ^[a-zA-Z_][a-zA-Z0-9_]*$"
  def describe(:reserved), do: "\"run\" is reserved"
  def describe(:bad_start), do: "must start with a letter"
  def describe(:bad_end), do: "must end with a letter or digit"

  def describe(:double_underscore),
    do: "must not contain \"__\" (reserved for pgflow queue names)"

  def describe(:queue_name_too_long),
    do:
      "lower(flow_slug) is the PGMQ queue name and must be at most #{@max_queue_name_length} characters"
end
