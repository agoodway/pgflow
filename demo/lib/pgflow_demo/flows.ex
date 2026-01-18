defmodule PgflowDemo.Flows do
  @moduledoc """
  Context for querying PgFlow run and step data.
  """

  alias PgflowDemo.Repo

  @doc """
  Fetches the output for a specific step in a flow run.

  Returns the output map or nil if not found.
  """
  @spec get_step_output(String.t(), String.t()) :: map() | nil
  def get_step_output(run_id, step_slug) do
    case Ecto.UUID.dump(run_id) do
      {:ok, run_id_bin} ->
        sql = """
        SELECT output FROM pgflow.step_states
        WHERE run_id = $1 AND step_slug = $2
        """

        case Ecto.Adapters.SQL.query(Repo, sql, [run_id_bin, step_slug]) do
          {:ok, %{rows: [[output]]}} when not is_nil(output) -> output
          _ -> nil
        end

      :error ->
        nil
    end
  end
end
