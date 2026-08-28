defmodule PgFlow.Schema.Step do
  @moduledoc """
  Schema for the pgflow.steps table.

  Represents a step within a flow with composite primary key (flow_slug, step_slug).
  """
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  @schema_prefix "pgflow"
  @primary_key false

  schema "steps" do
    field(:flow_slug, :string, primary_key: true)
    field(:step_slug, :string, primary_key: true)
    field(:step_type, :string)
    field(:step_index, :integer)
    field(:deps_count, :integer)
    field(:opt_max_attempts, :integer)
    field(:opt_base_delay, :integer)
    field(:opt_timeout, :integer)
    field(:created_at, :utc_datetime_usec)
    field(:opt_start_delay, :integer)
    field(:required_input_pattern, :map)
    field(:forbidden_input_pattern, :map)
    field(:when_unmet, :string)
    field(:when_exhausted, :string)

    belongs_to(:flow, PgFlow.Schema.Flow,
      foreign_key: :flow_slug,
      references: :flow_slug,
      define_field: false
    )
  end

  @doc false
  def changeset(step, attrs) do
    step
    |> cast(attrs, [
      :flow_slug,
      :step_slug,
      :step_type,
      :step_index,
      :deps_count,
      :opt_max_attempts,
      :opt_base_delay,
      :opt_timeout,
      :opt_start_delay,
      :required_input_pattern,
      :forbidden_input_pattern,
      :when_unmet,
      :when_exhausted
    ])
    |> validate_required([:flow_slug, :step_slug, :step_type, :deps_count])
    |> validate_inclusion(:step_type, ["single", "map"])
    |> validate_number(:step_index, greater_than_or_equal_to: 0)
    |> validate_number(:deps_count, greater_than_or_equal_to: 0)
    |> validate_number(:opt_max_attempts, greater_than: 0)
    |> validate_number(:opt_base_delay, greater_than_or_equal_to: 0)
    |> validate_number(:opt_timeout, greater_than: 0)
    |> validate_number(:opt_start_delay, greater_than_or_equal_to: 0)
    |> foreign_key_constraint(:flow_slug)
  end
end
