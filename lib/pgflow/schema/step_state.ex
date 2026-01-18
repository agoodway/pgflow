defmodule PgFlow.Schema.StepState do
  @moduledoc """
  Schema for the pgflow.step_states table.

  Represents the state of a step within a specific run with composite primary key
  (run_id, step_slug).
  """
  use Ecto.Schema
  import Ecto.Changeset

  @schema_prefix "pgflow"
  @primary_key false

  schema "step_states" do
    field(:run_id, :binary_id, primary_key: true)
    field(:step_slug, :string, primary_key: true)
    field(:status, :string)
    field(:remaining_tasks, :integer)
    field(:output, :map)
    field(:error_message, :string)
    field(:attempts_count, :integer)

    belongs_to(:run, PgFlow.Schema.Run,
      foreign_key: :run_id,
      references: :run_id,
      type: :binary_id,
      define_field: false
    )

    belongs_to(:step, PgFlow.Schema.Step,
      foreign_key: :step_slug,
      references: :step_slug,
      define_field: false
    )

    has_many(:step_tasks, PgFlow.Schema.StepTask,
      foreign_key: :run_id,
      references: :run_id
    )
  end

  @doc false
  def changeset(step_state, attrs) do
    step_state
    |> cast(attrs, [
      :run_id,
      :step_slug,
      :status,
      :remaining_tasks,
      :output,
      :error_message,
      :attempts_count
    ])
    |> validate_required([:run_id, :step_slug, :status, :remaining_tasks, :attempts_count])
    |> validate_number(:remaining_tasks, greater_than_or_equal_to: 0)
    |> validate_number(:attempts_count, greater_than_or_equal_to: 0)
    |> foreign_key_constraint(:run_id)
    |> foreign_key_constraint(:step_slug)
  end
end
