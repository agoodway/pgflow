defmodule PgFlow.Schema.Flow do
  @moduledoc """
  Schema for the pgflow.flows table.

  Represents a flow definition with its configuration options.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @schema_prefix "pgflow"
  @primary_key {:slug, :string, autogenerate: false}

  schema "flows" do
    field(:opt_max_attempts, :integer)
    field(:opt_base_delay, :integer)
    field(:opt_timeout, :integer)
    field(:opt_retry_backoff, :string)

    has_many(:steps, PgFlow.Schema.Step, foreign_key: :flow_slug, references: :slug)
    has_many(:runs, PgFlow.Schema.Run, foreign_key: :flow_slug, references: :slug)

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(flow, attrs) do
    flow
    |> cast(attrs, [:slug, :opt_max_attempts, :opt_base_delay, :opt_timeout, :opt_retry_backoff])
    |> validate_required([
      :slug,
      :opt_max_attempts,
      :opt_base_delay,
      :opt_timeout,
      :opt_retry_backoff
    ])
    |> validate_number(:opt_max_attempts, greater_than: 0)
    |> validate_number(:opt_base_delay, greater_than_or_equal_to: 0)
    |> validate_number(:opt_timeout, greater_than: 0)
    |> validate_inclusion(:opt_retry_backoff, ["exponential", "linear", "constant"])
  end
end
