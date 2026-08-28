defmodule PgFlow.RunSummary do
  @moduledoc """
  Typed operational summary for a PgFlow run.
  """

  alias PgFlow.Type.JSON

  @fields [
    :run_id,
    :flow_slug,
    :flow_type,
    :status,
    :input,
    :output,
    :started_at,
    :completed_at,
    :duration_ms,
    :total_steps,
    :completed_steps,
    :failed_steps,
    :skipped_steps,
    :progress_percent
  ]

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          run_id: Ecto.UUID.t(),
          flow_slug: String.t(),
          flow_type: String.t(),
          status: String.t(),
          input: JSON.value(),
          output: JSON.value(),
          started_at: DateTime.t(),
          completed_at: DateTime.t() | nil,
          duration_ms: Decimal.t(),
          total_steps: non_neg_integer(),
          completed_steps: non_neg_integer(),
          failed_steps: non_neg_integer(),
          skipped_steps: non_neg_integer(),
          progress_percent: Decimal.t()
        }

  @doc """
  Builds a run summary from a complete query projection.
  """
  @spec new(map()) :: t()
  def new(attributes) when is_map(attributes), do: struct!(__MODULE__, attributes)
end
