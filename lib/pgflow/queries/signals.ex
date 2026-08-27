defmodule PgFlow.Queries.Signals do
  @moduledoc """
  SQL wrappers for awaiting-signals helpers.
  """

  alias Ecto.Adapters.SQL

  import PgFlow.Queries.Helpers, only: [parse_uuid: 1]

  @type await_outcome ::
          {:ok, map() | list()}
          | :empty
          | :parked
          | :timeout
          | :stale
          | :terminal
          | :missing
          | {:error, term()}

  @type signal_outcome ::
          :buffered | :requeued | :already_delivered | :expired | :terminal | :missing

  @type waiting_task :: %{
          step_slug: String.t(),
          task_index: non_neg_integer(),
          wait_deadline_at: DateTime.t() | nil,
          waiting_since: DateTime.t()
        }

  @spec await_task_signal(
          module(),
          String.t(),
          String.t(),
          non_neg_integer(),
          pos_integer(),
          integer(),
          pos_integer() | nil,
          boolean()
        ) :: await_outcome()
  @doc """
  Atomically reads, replays, or parks the addressed task's signal state.

  `run_id`, `step_slug`, `task_index`, `expected_attempt`, and
  `expected_message_id` fence ownership to the current task claim; stale,
  terminal, and missing targets are returned as typed outcomes. When a signal
  is available, returns `{:ok, payload}` with the JSON map or list; this is the
  only Signals API that returns signal payload data. `wait_for_seconds` is the
  durable deadline budget and `park?` selects read-only or park-if-empty
  behavior.
  """
  def await_task_signal(
        repo,
        run_id,
        step_slug,
        task_index,
        expected_attempt,
        expected_message_id,
        wait_for_seconds,
        park?
      ) do
    sql = """
    SELECT outcome, payload
    FROM pgflow.await_task_signal($1, $2, $3, $4, $5, $6, $7)
    """

    params = [
      parse_uuid(run_id),
      step_slug,
      task_index,
      expected_attempt,
      expected_message_id,
      wait_for_seconds,
      park?
    ]

    repo
    |> SQL.query(sql, params)
    |> decode_await_result()
  end

  @doc false
  @spec decode_await_result({:ok, map()} | {:error, term()}) :: await_outcome()
  def decode_await_result({:ok, %{rows: [["signal", payload]]}}), do: {:ok, payload}
  def decode_await_result({:ok, %{rows: [["empty", nil]]}}), do: :empty
  def decode_await_result({:ok, %{rows: [["parked", nil]]}}), do: :parked
  def decode_await_result({:ok, %{rows: [["timeout", nil]]}}), do: :timeout
  def decode_await_result({:ok, %{rows: [["stale", nil]]}}), do: :stale
  def decode_await_result({:ok, %{rows: [["terminal", nil]]}}), do: :terminal
  def decode_await_result({:ok, %{rows: [["missing", nil]]}}), do: :missing

  def decode_await_result({:ok, %{rows: rows}}),
    do: {:error, {:unexpected_await_outcome, rows}}

  def decode_await_result({:error, reason}), do: {:error, reason}

  @spec signal_task(module(), String.t(), String.t(), non_neg_integer(), map() | list()) ::
          {:ok, signal_outcome()} | {:error, term()}
  @doc """
  Delivers JSON `payload` to the task addressed by `run_id`, `step_slug`, and
  `task_index`.

  The database fences delivery to that exact task address and returns a typed
  outcome: `:buffered`, `:requeued`, `:already_delivered`, `:expired`,
  `:terminal`, or `:missing`. A successful result reports delivery state, not
  the payload; the payload is returned only when the task awaits it. Direct SQL
  calls reject SQL `NULL`, JSON `null`, and scalar JSON before looking up the
  target, and reject payloads whose `pg_column_size` exceeds 1,048,576 bytes.
  """
  def signal_task(repo, run_id, step_slug, task_index, payload)
      when is_map(payload) or is_list(payload) do
    sql = "SELECT outcome FROM pgflow.signal_task($1, $2, $3, $4::jsonb)"

    case SQL.query(repo, sql, [parse_uuid(run_id), step_slug, task_index, payload]) do
      {:ok, %{rows: [["buffered"]]}} -> {:ok, :buffered}
      {:ok, %{rows: [["requeued"]]}} -> {:ok, :requeued}
      {:ok, %{rows: [["already_delivered"]]}} -> {:ok, :already_delivered}
      {:ok, %{rows: [["expired"]]}} -> {:ok, :expired}
      {:ok, %{rows: [["terminal"]]}} -> {:ok, :terminal}
      {:ok, %{rows: [["missing"]]}} -> {:ok, :missing}
      {:ok, %{rows: rows}} -> {:error, {:unexpected_signal_outcome, rows}}
      {:error, err} -> {:error, err}
    end
  end

  @spec list_waiting_tasks(module(), String.t()) ::
          {:ok, [waiting_task()]} | {:error, term()}
  @doc """
  Lists the signal-waiting tasks owned by `run_id`.

  Results contain each fenced task address and waiting timestamps, but never
  its signal payload or claim state. An unknown run simply yields an empty
  list when the underlying query has no matching waiting tasks.
  """
  def list_waiting_tasks(repo, run_id) do
    sql = """
    SELECT st.step_slug, st.task_index, ts.wait_deadline_at, ts.inserted_at
    FROM pgflow.step_tasks st
    JOIN pgflow.task_signals ts
      ON ts.run_id = st.run_id
     AND ts.step_slug = st.step_slug
     AND ts.task_index = st.task_index
    WHERE st.run_id = $1 AND st.status = 'waiting'
    ORDER BY st.step_slug, st.task_index
    """

    case SQL.query(repo, sql, [parse_uuid(run_id)]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [step_slug, task_index, deadline, inserted_at] ->
           %{
             step_slug: step_slug,
             task_index: task_index,
             wait_deadline_at: deadline,
             waiting_since: inserted_at
           }
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec expire_waiting_tasks(module(), pos_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  @doc """
  Requeues at most `limit` waiting tasks whose PostgreSQL deadlines have
  elapsed.

  Returns the number of tasks requeued. Deadline and ownership checks are
  performed by the SQL helper; no signal payload data is returned.
  """
  def expire_waiting_tasks(repo, limit) when is_integer(limit) and limit > 0 do
    sql = "SELECT pgflow.expire_waiting_tasks($1)"

    case SQL.query(repo, sql, [limit]) do
      {:ok, %{rows: [[count]]}} -> {:ok, count}
      {:error, err} -> {:error, err}
    end
  end
end
