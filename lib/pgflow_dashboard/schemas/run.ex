defmodule PgFlowDashboard.Schemas.Run do
  @moduledoc """
  Schema for the pgflow_dashboard.runs_view view.

  Used with LiveFilter's QueryBuilder for filtered run queries.
  """
  use Ecto.Schema

  @primary_key {:run_id, :binary_id, autogenerate: false}
  @schema_prefix "pgflow_dashboard"

  schema "runs_view" do
    field(:flow_slug, :string)
    field(:flow_type, :string)
    field(:status, :string)
    field(:input, :map)
    field(:output, :map)
    field(:started_at, :utc_datetime_usec)
    field(:completed_at, :utc_datetime_usec)
    field(:duration_ms, :decimal)
    field(:total_steps, :integer)
    field(:completed_steps, :integer)
    field(:failed_steps, :integer)
    field(:skipped_steps, :integer)
    field(:progress_percent, :decimal)
  end
end
