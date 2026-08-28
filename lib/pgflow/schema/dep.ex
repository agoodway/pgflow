defmodule PgFlow.Schema.Dep do
  @moduledoc """
  Schema for the pgflow.deps table.

  Represents a dependency relationship between steps with composite primary key
  (flow_slug, step_slug, dep_slug).
  """
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  @schema_prefix "pgflow"
  @primary_key false

  schema "deps" do
    field(:flow_slug, :string, primary_key: true)
    field(:dep_slug, :string, primary_key: true)
    field(:step_slug, :string, primary_key: true)
    field(:created_at, :utc_datetime_usec)
  end

  @doc false
  def changeset(dep, attrs) do
    dep
    |> cast(attrs, [:flow_slug, :step_slug, :dep_slug])
    |> validate_required([:flow_slug, :step_slug, :dep_slug])
    |> foreign_key_constraint(:dep_slug, name: :deps_flow_slug_dep_slug_fkey)
    |> foreign_key_constraint(:step_slug, name: :deps_flow_slug_step_slug_fkey)
  end
end
