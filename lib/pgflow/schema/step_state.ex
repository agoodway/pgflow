defmodule PgFlow.Schema.StepState do
  @moduledoc """
  Schema for the pgflow.step_states table.

  Represents the state of a step within a specific run with composite primary key
  (run_id, step_slug).
  """
  use Ecto.Schema
  import Ecto.Changeset

  alias PgFlow.Type.JSON

  @type t :: %__MODULE__{}

  @schema_prefix "pgflow"
  @primary_key false

  schema "step_states" do
    field(:flow_slug, :string)
    field(:run_id, :binary_id, primary_key: true)
    field(:step_slug, :string, primary_key: true)
    field(:status, :string)
    field(:remaining_tasks, :integer)
    field(:remaining_deps, :integer)
    field(:error_message, :string)
    field(:initial_tasks, :integer)
    field(:created_at, :utc_datetime_usec)
    field(:started_at, :utc_datetime_usec)
    field(:completed_at, :utc_datetime_usec)
    field(:failed_at, :utc_datetime_usec)
    field(:output, JSON)
    field(:skip_reason, :string)
    field(:skipped_at, :utc_datetime_usec)

    belongs_to(:run, PgFlow.Schema.Run,
      foreign_key: :run_id,
      references: :run_id,
      type: :binary_id,
      define_field: false
    )
  end

  @doc false
  def changeset(step_state, attrs) do
    step_state
    |> cast(attrs, [
      :run_id,
      :flow_slug,
      :step_slug,
      :status,
      :remaining_tasks,
      :remaining_deps,
      :output,
      :error_message,
      :initial_tasks,
      :skip_reason,
      :skipped_at
    ])
    |> validate_required([:run_id, :flow_slug, :step_slug, :status])
    |> validate_number(:remaining_tasks, greater_than_or_equal_to: 0)
    |> foreign_key_constraint(:run_id)
    |> foreign_key_constraint(:step_slug, name: :step_states_flow_slug_step_slug_fkey)
  end
end
