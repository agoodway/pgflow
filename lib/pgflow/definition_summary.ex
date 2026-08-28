defmodule PgFlow.DefinitionSummary do
  @moduledoc """
  Typed operational statistics for a stored PgFlow definition.
  """

  @fields [
    :flow_slug,
    :flow_type,
    :opt_max_attempts,
    :opt_base_delay,
    :opt_timeout,
    :total_runs_24h,
    :completed_runs_24h,
    :failed_runs_24h,
    :success_rate_24h,
    :avg_duration_ms,
    :p95_duration_ms,
    :step_count
  ]

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          flow_slug: String.t(),
          flow_type: String.t(),
          opt_max_attempts: non_neg_integer(),
          opt_base_delay: non_neg_integer(),
          opt_timeout: pos_integer(),
          total_runs_24h: non_neg_integer(),
          completed_runs_24h: non_neg_integer(),
          failed_runs_24h: non_neg_integer(),
          success_rate_24h: Decimal.t(),
          avg_duration_ms: Decimal.t(),
          p95_duration_ms: float(),
          step_count: non_neg_integer()
        }

  @doc """
  Builds definition statistics from a complete query projection.
  """
  @spec new(map()) :: t()
  def new(attributes) when is_map(attributes), do: struct!(__MODULE__, attributes)
end
