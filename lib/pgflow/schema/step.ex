defmodule PgFlow.Schema.Step do
  @moduledoc """
  Schema for the pgflow.steps table.

  Represents a step within a flow with composite primary key (flow_slug, step_slug).
  """
  use Ecto.Schema
  import Ecto.Changeset

  @schema_prefix "pgflow"
  @primary_key false

  schema "steps" do
    field(:flow_slug, :string, primary_key: true)
    field(:step_slug, :string, primary_key: true)
    field(:step_type, :string)
    field(:deps_count, :integer)
    field(:opt_max_attempts, :integer)
    field(:opt_base_delay, :integer)
    field(:opt_timeout, :integer)

    belongs_to(:flow, PgFlow.Schema.Flow,
      foreign_key: :flow_slug,
      references: :slug,
      define_field: false
    )

    has_many(:deps, PgFlow.Schema.Dep,
      foreign_key: :flow_slug,
      references: :flow_slug
    )

    has_many(:step_states, PgFlow.Schema.StepState,
      foreign_key: :step_slug,
      references: :step_slug
    )
  end

  @doc false
  def changeset(step, attrs) do
    step
    |> cast(attrs, [
      :flow_slug,
      :step_slug,
      :step_type,
      :deps_count,
      :opt_max_attempts,
      :opt_base_delay,
      :opt_timeout
    ])
    |> validate_required([:flow_slug, :step_slug, :step_type, :deps_count])
    |> validate_inclusion(:step_type, ["single", "map"])
    |> validate_number(:deps_count, greater_than_or_equal_to: 0)
    |> validate_number(:opt_max_attempts, greater_than: 0)
    |> validate_number(:opt_base_delay, greater_than_or_equal_to: 0)
    |> validate_number(:opt_timeout, greater_than: 0)
    |> foreign_key_constraint(:flow_slug)
  end
end
