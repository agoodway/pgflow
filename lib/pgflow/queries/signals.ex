defmodule PgFlow.Queries.Signals do
  @moduledoc """
  SQL wrappers for awaiting-signals helpers (`park_waiting_task`, `signal_task`,
  `consume_task_signal`, `expire_waiting_tasks`).
  """

  alias Ecto.Adapters.SQL

  import PgFlow.Queries.Helpers, only: [parse_uuid: 1]

  @spec park_waiting_task(module(), String.t(), String.t(), non_neg_integer(), DateTime.t() | nil) ::
          :ok | {:error, term()}
  def park_waiting_task(repo, run_id, step_slug, task_index, wait_deadline_at) do
    sql = "SELECT pgflow.park_waiting_task($1, $2, $3, $4)"

    case SQL.query(repo, sql, [parse_uuid(run_id), step_slug, task_index, wait_deadline_at]) do
      {:ok, _} -> :ok
      {:error, err} -> {:error, err}
    end
  end

  @spec signal_task(module(), String.t(), String.t(), non_neg_integer(), map() | list()) ::
          :ok | {:error, term()}
  def signal_task(repo, run_id, step_slug, task_index, payload)
      when is_map(payload) or is_list(payload) do
    sql = "SELECT pgflow.signal_task($1, $2, $3, $4::jsonb)"

    case SQL.query(repo, sql, [parse_uuid(run_id), step_slug, task_index, payload]) do
      {:ok, _} -> :ok
      {:error, err} -> {:error, err}
    end
  end

  @spec consume_task_signal(module(), String.t(), String.t(), non_neg_integer()) ::
          {:ok, map() | list()} | {:error, :timeout} | :empty | {:error, term()}
  def consume_task_signal(repo, run_id, step_slug, task_index) do
    sql = "SELECT payload, timed_out FROM pgflow.consume_task_signal($1, $2, $3)"

    case SQL.query(repo, sql, [parse_uuid(run_id), step_slug, task_index]) do
      {:ok, %{rows: []}} ->
        :empty

      {:ok, %{rows: [[_payload, true]]}} ->
        {:error, :timeout}

      {:ok, %{rows: [[payload, false]]}} when not is_nil(payload) ->
        {:ok, payload}

      {:ok, %{rows: [[nil, false]]}} ->
        :empty

      {:error, err} ->
        {:error, err}
    end
  end

  @spec expire_waiting_tasks(module()) :: {:ok, non_neg_integer()} | {:error, term()}
  def expire_waiting_tasks(repo) do
    sql = "SELECT pgflow.expire_waiting_tasks()"

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: [[count]]}} -> {:ok, count}
      {:error, err} -> {:error, err}
    end
  end
end
