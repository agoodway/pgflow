defmodule PgFlow.CronSummary do
  @moduledoc """
  Typed operational statistics and schedule data for a PgFlow cron definition.

  `last_run_at` is the completion time of the definition's most recent PgFlow run.
  `next_run_at` is the next scheduled instant in UTC.
  """

  @fields [
    :flow_slug,
    :flow_type,
    :cron_expression,
    :is_active,
    :opt_max_attempts,
    :opt_base_delay,
    :opt_timeout,
    :total_runs_24h,
    :completed_runs_24h,
    :failed_runs_24h,
    :success_rate_24h,
    :avg_duration_ms,
    :p95_duration_ms,
    :last_run_at,
    :last_run_status,
    :next_run_at
  ]

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          flow_slug: String.t(),
          flow_type: String.t(),
          cron_expression: String.t(),
          is_active: boolean(),
          opt_max_attempts: non_neg_integer(),
          opt_base_delay: non_neg_integer(),
          opt_timeout: pos_integer(),
          total_runs_24h: non_neg_integer(),
          completed_runs_24h: non_neg_integer(),
          failed_runs_24h: non_neg_integer(),
          success_rate_24h: Decimal.t(),
          avg_duration_ms: Decimal.t(),
          p95_duration_ms: float(),
          last_run_at: DateTime.t() | nil,
          last_run_status: String.t() | nil,
          next_run_at: DateTime.t() | nil
        }

  @doc """
  Builds a cron summary from a complete query projection.
  """
  @spec new(map()) :: t()
  def new(attributes) when is_map(attributes), do: struct!(__MODULE__, attributes)
end
