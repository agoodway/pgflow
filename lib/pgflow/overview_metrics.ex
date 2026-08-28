defmodule PgFlow.OverviewMetrics do
  @moduledoc """
  Typed real-time metrics for PgFlow's operational overview.
  """

  @fields [
    :active_workers,
    :healthy_workers,
    :stale_workers,
    :total_runs_24h,
    :completed_runs_24h,
    :failed_runs_24h,
    :running_runs,
    :avg_duration_ms,
    :queue_depth
  ]

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          active_workers: non_neg_integer(),
          healthy_workers: non_neg_integer(),
          stale_workers: non_neg_integer(),
          total_runs_24h: non_neg_integer(),
          completed_runs_24h: non_neg_integer(),
          failed_runs_24h: non_neg_integer(),
          running_runs: non_neg_integer(),
          avg_duration_ms: Decimal.t(),
          queue_depth: non_neg_integer()
        }

  @doc """
  Builds overview metrics from a complete query projection.
  """
  @spec new(map()) :: t()
  def new(attributes) when is_map(attributes), do: struct!(__MODULE__, attributes)
end
