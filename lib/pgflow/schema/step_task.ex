defmodule PgFlow.Schema.StepTask do
  @moduledoc """
  Schema for the pgflow.step_tasks table.

  Represents an individual task within a step (for map-type steps) with composite
  primary key (run_id, step_slug, task_index).
  """
  use Ecto.Schema
  import Ecto.Changeset

  @schema_prefix "pgflow"
  @primary_key false

  schema "step_tasks" do
    field(:run_id, :binary_id, primary_key: true)
    field(:step_slug, :string, primary_key: true)
    field(:task_index, :integer, primary_key: true)
    field(:status, :string)
    field(:input, :map)
    field(:output, :map)
    field(:error_message, :string)
    field(:message_id, :integer)

    belongs_to(:step_state, PgFlow.Schema.StepState,
      foreign_key: :run_id,
      references: :run_id,
      type: :binary_id,
      define_field: false
    )
  end

  @doc false
  def changeset(step_task, attrs) do
    step_task
    |> cast(attrs, [
      :run_id,
      :step_slug,
      :task_index,
      :status,
      :input,
      :output,
      :error_message,
      :message_id
    ])
    |> validate_required([:run_id, :step_slug, :task_index, :status, :input])
    |> validate_number(:task_index, greater_than_or_equal_to: 0)
    |> validate_number(:message_id, greater_than_or_equal_to: 0)
    |> foreign_key_constraint(:run_id)
  end
end
