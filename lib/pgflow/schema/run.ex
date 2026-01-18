defmodule PgFlow.Schema.Run do
  @moduledoc """
  Schema for the pgflow.runs table.

  Represents a single execution instance of a flow.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @schema_prefix "pgflow"
  @primary_key {:run_id, :binary_id, autogenerate: false}

  schema "runs" do
    field(:flow_slug, :string)
    field(:status, :string)
    field(:input, :map)
    field(:output, :map)
    field(:remaining_steps, :integer)

    belongs_to(:flow, PgFlow.Schema.Flow,
      foreign_key: :flow_slug,
      references: :slug,
      define_field: false
    )

    has_many(:step_states, PgFlow.Schema.StepState, foreign_key: :run_id, references: :run_id)

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(run, attrs) do
    run
    |> cast(attrs, [:run_id, :flow_slug, :status, :input, :output, :remaining_steps])
    |> validate_required([:run_id, :flow_slug, :status, :input, :remaining_steps])
    |> validate_inclusion(:status, ["started", "completed", "failed"])
    |> validate_number(:remaining_steps, greater_than_or_equal_to: 0)
    |> foreign_key_constraint(:flow_slug)
  end
end
