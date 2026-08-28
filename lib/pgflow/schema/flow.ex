defmodule PgFlow.Schema.Flow do
  @moduledoc """
  Schema for the pgflow.flows table.

  Represents a flow definition with its configuration options.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  @schema_prefix "pgflow"
  @primary_key {:flow_slug, :string, autogenerate: false}

  schema "flows" do
    field(:opt_max_attempts, :integer)
    field(:opt_base_delay, :integer)
    field(:opt_timeout, :integer)
    field(:created_at, :utc_datetime_usec)
    field(:flow_type, :string)

    has_many(:steps, PgFlow.Schema.Step, foreign_key: :flow_slug, references: :flow_slug)
    has_many(:runs, PgFlow.Schema.Run, foreign_key: :flow_slug, references: :flow_slug)
  end

  @doc false
  def changeset(flow, attrs) do
    flow
    |> cast(attrs, [:flow_slug, :opt_max_attempts, :opt_base_delay, :opt_timeout, :flow_type])
    |> validate_required([
      :flow_slug,
      :opt_max_attempts,
      :opt_base_delay,
      :opt_timeout
    ])
    |> validate_number(:opt_max_attempts, greater_than: 0)
    |> validate_number(:opt_base_delay, greater_than_or_equal_to: 0)
    |> validate_number(:opt_timeout, greater_than: 0)
    |> validate_inclusion(:flow_type, ["flow", "job"])
  end
end
