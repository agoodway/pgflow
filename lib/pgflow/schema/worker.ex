defmodule PgFlow.Schema.Worker do
  @moduledoc """
  Schema for the pgflow.workers table.

  Represents a worker process that can execute flow steps.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  @schema_prefix "pgflow"
  @primary_key {:worker_id, :binary_id, autogenerate: false}

  schema "workers" do
    field(:queue_name, :string)
    field(:function_name, :string)
    field(:started_at, :utc_datetime_usec)
    field(:deprecated_at, :utc_datetime_usec)
    field(:last_heartbeat_at, :utc_datetime_usec)
    field(:stopped_at, :utc_datetime_usec)
  end

  @doc false
  def changeset(worker, attrs) do
    worker
    |> cast(attrs, [
      :worker_id,
      :queue_name,
      :function_name,
      :started_at,
      :deprecated_at,
      :last_heartbeat_at,
      :stopped_at
    ])
    |> validate_required([
      :worker_id,
      :queue_name,
      :function_name,
      :started_at,
      :last_heartbeat_at
    ])
  end
end
