defmodule PgFlow.Type.JSON do
  @moduledoc """
  Ecto type for values that PostgreSQL stores in `jsonb` columns.
  """
  use Ecto.Type

  @type value ::
          nil
          | boolean()
          | number()
          | String.t()
          | [value()]
          | %{optional(String.t()) => value()}

  @impl Ecto.Type
  def type, do: :map

  @impl Ecto.Type
  def cast(value), do: cast_json(value)

  @impl Ecto.Type
  def load(value), do: cast_json(value)

  @impl Ecto.Type
  def dump(value), do: cast_json(value)

  defp cast_json(value) do
    if json_value?(value), do: {:ok, value}, else: :error
  end

  defp json_value?(nil), do: true
  defp json_value?(value) when is_boolean(value), do: true
  defp json_value?(value) when is_number(value), do: true
  defp json_value?(value) when is_binary(value), do: String.valid?(value)
  defp json_value?(value) when is_list(value), do: json_list?(value)

  defp json_value?(value) when is_map(value) do
    Enum.all?(value, fn {key, nested_value} ->
      is_binary(key) and String.valid?(key) and json_value?(nested_value)
    end)
  end

  defp json_value?(_value), do: false

  defp json_list?([]), do: true
  defp json_list?([head | tail]), do: json_value?(head) and json_list?(tail)
  defp json_list?(_improper_tail), do: false
end
