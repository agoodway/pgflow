defmodule PgFlow.Schema.Worker do
  @moduledoc """
  Schema for the pgflow.workers table.

  Represents a worker process that can execute flow steps.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @schema_prefix "pgflow"
  @primary_key {:worker_id, :binary_id, autogenerate: false}

  schema "workers" do
    field(:flow_slugs, {:array, :string})
    field(:status, :string)
    field(:deprecation_reason, :string)
    field(:expires_at, :utc_datetime_usec)

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(worker, attrs) do
    worker
    |> cast(attrs, [:worker_id, :flow_slugs, :status, :deprecation_reason, :expires_at])
    |> validate_required([:worker_id, :flow_slugs, :status])
    |> validate_inclusion(:status, ["active", "inactive", "deprecated"])
  end
end
