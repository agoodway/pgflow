defmodule PgFlow.WorkerSummary do
  @moduledoc """
  Typed operational summary for a persisted PgFlow worker.
  """

  @fields [
    :worker_id,
    :flow_slug,
    :flow_type,
    :last_heartbeat_at,
    :health_status,
    :active_tasks,
    :completed_tasks_24h
  ]

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{
          worker_id: Ecto.UUID.t(),
          flow_slug: String.t(),
          flow_type: String.t(),
          last_heartbeat_at: DateTime.t(),
          health_status: String.t(),
          active_tasks: non_neg_integer(),
          completed_tasks_24h: non_neg_integer()
        }

  @doc """
  Builds a worker summary from a complete query projection.
  """
  @spec new(map()) :: t()
  def new(attributes) when is_map(attributes), do: struct!(__MODULE__, attributes)
end
