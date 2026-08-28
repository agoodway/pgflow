defmodule PgFlow.RunHistoryCell do
  @moduledoc """
  Typed result for one step in a run history projection.
  """

  @fields [:run_id, :started_at, :step_slug, :status, :duration_ms]

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          run_id: Ecto.UUID.t(),
          started_at: DateTime.t(),
          step_slug: String.t() | nil,
          status: String.t() | nil,
          duration_ms: Decimal.t() | nil
        }

  @doc """
  Builds a run history cell from a complete query projection.
  """
  @spec new(map()) :: t()
  def new(attributes) when is_map(attributes), do: struct!(__MODULE__, attributes)
end
