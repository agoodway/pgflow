defmodule PgFlow.Schema.StepTask do
  @moduledoc """
  Schema for the pgflow.step_tasks table.

  Represents an individual task within a step (for map-type steps) with composite
  primary key (run_id, step_slug, task_index).
  """
  use Ecto.Schema
  import Ecto.Changeset

  alias PgFlow.Type.JSON

  @type t :: %__MODULE__{}

  @schema_prefix "pgflow"
  @primary_key false

  schema "step_tasks" do
    field(:flow_slug, :string)
    field(:run_id, :binary_id, primary_key: true)
    field(:step_slug, :string, primary_key: true)
    field(:message_id, :integer)
    field(:task_index, :integer, primary_key: true)
    field(:status, :string)
    field(:attempts_count, :integer)
    field(:error_message, :string)
    field(:output, JSON)
    field(:queued_at, :utc_datetime_usec)
    field(:completed_at, :utc_datetime_usec)
    field(:failed_at, :utc_datetime_usec)
    field(:started_at, :utc_datetime_usec)
    field(:last_worker_id, :binary_id)
    field(:requeued_count, :integer)
    field(:last_requeued_at, :utc_datetime_usec)
    field(:permanently_stalled_at, :utc_datetime_usec)
  end

  @doc false
  def changeset(step_task, attrs) do
    step_task
    |> cast(attrs, [
      :flow_slug,
      :run_id,
      :step_slug,
      :task_index,
      :status,
      :output,
      :error_message,
      :message_id,
      :attempts_count,
      :last_worker_id,
      :requeued_count,
      :last_requeued_at,
      :permanently_stalled_at
    ])
    |> validate_required([:flow_slug, :run_id, :step_slug, :task_index, :status])
    |> validate_number(:task_index, greater_than_or_equal_to: 0)
    |> validate_number(:message_id, greater_than_or_equal_to: 0)
    |> validate_number(:attempts_count, greater_than_or_equal_to: 0)
    |> validate_number(:requeued_count, greater_than_or_equal_to: 0)
    |> foreign_key_constraint(:run_id)
    |> foreign_key_constraint(:step_slug, name: :step_tasks_run_id_step_slug_fkey)
  end
end
