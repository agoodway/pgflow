defmodule PgflowDemo.Flows do
  @moduledoc """
  Context for querying PgFlow run and step data.
  """

  alias PgFlow.Queries.Flows, as: FlowQueries
  alias PgflowDemo.Repo

  @doc """
  Fetches the output for a specific step in a flow run.

  Returns the output map or nil if not found.
  """
  @spec get_step_output(String.t(), String.t()) :: map() | nil
  def get_step_output(run_id, step_slug) do
    case FlowQueries.get_step_output(Repo, run_id, step_slug) do
      {:ok, output} -> output
      {:error, _} -> nil
    end
  end
end
