defmodule PgFlow.Queries.Flows do
  @moduledoc """
  SQL query interface for pgflow flow operations.

  Provides functions for starting flows, completing/failing tasks, reading
  messages, and managing flow lifecycle. All functions that accept JSON data
  expect Elixir terms that will be encoded with Jason.
  """

  alias Ecto.Adapters.SQL

  import PgFlow.Queries.Helpers, only: [execute_rpc: 4, parse_uuid: 1, format_uuid: 1]

  @pgflow_schema "pgflow"

  @doc """
  Starts a new flow execution.

  ## Parameters

    * `repo` - The Ecto repository
    * `flow_slug` - The flow identifier slug
    * `input` - Input data as an Elixir term (will be encoded as JSONB)

  ## Returns

    * `{:ok, run_id}` - The UUID of the created flow run
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec start_flow(Ecto.Repo.t(), String.t(), map() | list()) ::
          {:ok, String.t()} | {:error, term()}
  def start_flow(repo, flow_slug, input) do
    with {:ok, run_id, _run_snapshot} <- start_flow_with_run(repo, flow_slug, input) do
      {:ok, run_id}
    end
  end

  @doc """
  Starts a new flow execution and returns the run row `pgflow.start_flow`
  itself produced, alongside the run id.

  `pgflow.start_flow` runs run creation, condition evaluation
  (`cascade_resolve_conditions`), taskless-step completion, and initial task
  enqueuing all inside the one implicit transaction backing this statement.
  The row this function returns is read back inside that same transaction,
  so it is the authoritative snapshot of whatever the statement decided —
  no external worker can have touched the run yet, because nothing about it
  is visible to another session until this statement commits. Callers that
  need to know whether `start_flow` itself completed or failed the run
  synchronously (e.g. a root condition with `when_unmet: :fail`) should use
  this snapshot instead of issuing a second query, which would race a fast
  worker and risk mislabeling a genuine handler failure.

  This snapshot only covers the run's own `status`/`output` — it does not
  extend to skipped steps. The private `emit_post_start` client helper still calls
  `PgFlow.Telemetry.emit_skipped_steps/3` as a separate, post-commit query,
  which can race a worker sweeping the same run; see the delivery contract on
  `PgFlow.Telemetry.emit_skipped_steps/4` for what that guarantees.

  ## Returns

    * `{:ok, run_id, run}` - The run id and a map of the returned run row
      (`:run_id, :flow_slug, :status, :input, :output, :remaining_steps,
      :started_at, :completed_at, :failed_at`)
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec start_flow_with_run(Ecto.Repo.t(), String.t(), map() | list()) ::
          {:ok, String.t(), map()} | {:error, term()}
  def start_flow_with_run(repo, flow_slug, input) do
    sql = "SELECT * FROM pgflow.start_flow($1, $2::jsonb)"

    case SQL.query(repo, sql, [flow_slug, input]) do
      {:ok, %{rows: [row], columns: columns}} ->
        run = row_to_run(columns, row)
        {:ok, run.run_id, run}

      {:ok, %{rows: []}} ->
        {:error, :no_result}

      {:error, error} ->
        {:error, error}
    end
  end

  # The columns pgflow.start_flow's run row is documented (and consumed) as.
  # Mapped through a fixed table instead of String.to_atom/1 so column names
  # arriving from the database can never mint new atoms; a column a newer SQL
  # bundle adds is simply dropped from the snapshot until it is added here.
  @run_row_columns Map.new(
                     ~w(run_id flow_slug status input output remaining_steps
                        started_at completed_at failed_at)a,
                     &{Atom.to_string(&1), &1}
                   )

  defp row_to_run(columns, row) do
    columns
    |> Enum.zip(row)
    |> Enum.reduce(%{}, fn {column, value}, acc ->
      case Map.fetch(@run_row_columns, column) do
        {:ok, key} -> Map.put(acc, key, value)
        :error -> acc
      end
    end)
    |> Map.update!(:run_id, &format_uuid/1)
  end

  @doc """
  Delays the first queued task for a flow run by moving its pgmq visibility time.

  This is a lower-level helper for public APIs such as `PgFlow.enqueue_in/3`
  and `PgFlow.enqueue_at/3`. Call it in the same repository transaction as
  `start_flow/3` when callers need to ensure workers cannot see the task before
  the delay is applied.
  """
  @spec delay_run(Ecto.Repo.t(), String.t(), String.t(), non_neg_integer()) ::
          :ok | {:error, term()}
  def delay_run(_repo, _flow_slug, _run_id, 0), do: :ok

  def delay_run(repo, flow_slug, run_id, delay_seconds)
      when is_integer(delay_seconds) and delay_seconds > 0 do
    sql = """
    WITH task AS (
      SELECT step_tasks.message_id
      FROM pgflow.step_tasks AS step_tasks
      WHERE step_tasks.flow_slug = $1::text
        AND step_tasks.run_id = $2::uuid
      ORDER BY step_tasks.queued_at ASC
      LIMIT 1
    ),
    delayed AS (
      SELECT pgflow.set_vt_batch(
        $1::text,
        ARRAY[task.message_id]::bigint[],
        ARRAY[$3::integer]::integer[]
      )
      FROM task
    )
    SELECT count(*) FROM delayed
    """

    case SQL.query(repo, sql, [flow_slug, parse_uuid(run_id), delay_seconds]) do
      {:ok, %{rows: [[1]]}} -> :ok
      {:ok, %{rows: [[0]]}} -> {:error, :task_not_found}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Retrieves a flow run's current state.

  ## Parameters

    * `repo` - The Ecto repository
    * `run_id` - The flow run UUID

  ## Returns

    * `{:ok, %{status: String.t(), output: term()}}` - Run state
    * `{:error, :not_found}` - Run does not exist
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec get_run(Ecto.Repo.t(), String.t()) ::
          {:ok, %{status: String.t(), output: term()}} | {:error, :not_found | term()}
  def get_run(repo, run_id) do
    sql = "SELECT status, output FROM pgflow.runs WHERE run_id = $1"

    case SQL.query(repo, sql, [parse_uuid(run_id)]) do
      {:ok, %{rows: [[status, output]]}} -> {:ok, %{status: status, output: output}}
      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Lists skipped steps for a flow run in dependency order.

  Ordered by skip time, ties broken by `pgflow.steps.step_index`. A cascade
  stamps every step in the chain with the same `skipped_at = now()`, so ties
  are the rule rather than the exception; `step_index` is assigned in
  `add_step` order, which is topological (a step's deps must already exist),
  and is what core SQL itself uses to walk a cascade. Breaking ties by slug
  would announce a child before its parent.

  ## Parameters

    * `repo` - The Ecto repository
    * `run_id` - The flow run UUID

  ## Returns

    * `{:ok, [%{step_slug: String.t(), skip_reason: String.t()}]}` - Skipped steps
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec list_skipped_steps(Ecto.Repo.t(), String.t()) ::
          {:ok, [%{step_slug: String.t(), skip_reason: String.t()}]} | {:error, term()}
  def list_skipped_steps(repo, run_id) do
    sql = """
    SELECT step_states.step_slug, step_states.skip_reason
    FROM pgflow.step_states AS step_states
    JOIN pgflow.steps AS steps
      ON steps.flow_slug = step_states.flow_slug
     AND steps.step_slug = step_states.step_slug
    WHERE step_states.run_id = $1 AND step_states.status = 'skipped'
    ORDER BY step_states.skipped_at ASC NULLS LAST, steps.step_index ASC
    """

    case SQL.query(repo, sql, [parse_uuid(run_id)]) do
      {:ok, %{rows: rows}} ->
        {:ok, Enum.map(rows, fn [slug, reason] -> %{step_slug: slug, skip_reason: reason} end)}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Marks a task as completed with output data.

  ## Parameters

    * `repo` - The Ecto repository
    * `run_id` - The flow run UUID
    * `step_slug` - The step identifier slug
    * `task_index` - The task index (0-based)
    * `output` - Output data as an Elixir term (will be encoded as JSONB)

  ## Returns

    * `{:ok, result}` - Success result from the database
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec complete_task(Ecto.Repo.t(), String.t(), String.t(), non_neg_integer(), map() | list()) ::
          {:ok, term()} | {:error, term()}
  def complete_task(repo, run_id, step_slug, task_index, output) do
    sql = "SELECT * FROM pgflow.complete_task($1, $2, $3, $4::jsonb)"
    query_single_row(repo, sql, [parse_uuid(run_id), step_slug, task_index, output])
  end

  @doc """
  Marks a task as failed with an error message.

  ## Parameters

    * `repo` - The Ecto repository
    * `run_id` - The flow run UUID
    * `step_slug` - The step identifier slug
    * `task_index` - The task index (0-based)
    * `error_message` - Error description string

  ## Returns

    * `{:ok, result}` - Success result from the database
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec fail_task(Ecto.Repo.t(), String.t(), String.t(), non_neg_integer(), String.t()) ::
          {:ok, term()} | {:error, term()}
  def fail_task(repo, run_id, step_slug, task_index, error_message) do
    sql = "SELECT * FROM pgflow.fail_task($1, $2, $3, $4)"
    query_single_row(repo, sql, [parse_uuid(run_id), step_slug, task_index, error_message])
  end

  @doc """
  Reads messages from a queue without blocking (non-blocking read).

  Uses pgmq.read() to fetch available messages. Returns immediately whether or
  not messages are available. Messages are made invisible for the visibility
  timeout period to prevent duplicate processing.

  Queue poll SQL logging is disabled by default because workers call this
  frequently. Set `config :pgflow, :log_queue_polls, true` to enable Ecto query
  logging for these reads while debugging.

  ## Parameters

    * `repo` - The Ecto repository
    * `queue_name` - The name of the queue to read from (matches flow_slug)
    * `visibility_timeout` - Time in seconds messages remain invisible
    * `batch_size` - Maximum number of messages to retrieve

  ## Returns

    * `{:ok, messages}` - List of message records from pgmq (may be empty)
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec read(Ecto.Repo.t(), String.t(), pos_integer(), pos_integer()) ::
          {:ok, list(list())} | {:error, term()}
  def read(repo, queue_name, visibility_timeout, batch_size) do
    sql = """
    SELECT msg_id, read_ct, enqueued_at, vt, message
    FROM pgmq.read(
      queue_name => $1::text,
      vt => $2::integer,
      qty => $3::integer
    )
    """

    case SQL.query(repo, sql, [queue_name, visibility_timeout, batch_size],
           log: log_queue_polls?()
         ) do
      {:ok, %{rows: rows}} -> {:ok, rows}
      {:error, error} -> {:error, error}
    end
  end

  defp log_queue_polls? do
    Application.get_env(:pgflow, :log_queue_polls, false)
  end

  defp query_single_row(repo, sql, params) do
    case SQL.query(repo, sql, params) do
      {:ok, %{rows: [row | _]}} -> {:ok, row}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Starts multiple tasks by marking messages as in-progress.

  ## Parameters

    * `repo` - The Ecto repository
    * `flow_slug` - The flow identifier slug
    * `msg_ids` - List of message IDs from pgmq
    * `worker_id` - The worker UUID string

  ## Returns

    * `{:ok, task_details}` - List of task detail records
    * `{:error, reason}` - Error details if the operation fails
  """
  @spec start_tasks(Ecto.Repo.t(), String.t(), list(pos_integer()), String.t()) ::
          {:ok, list(list())} | {:error, term()}
  def start_tasks(repo, flow_slug, msg_ids, worker_id) do
    sql = "SELECT * FROM pgflow.start_tasks($1, $2, $3)"

    case SQL.query(repo, sql, [flow_slug, msg_ids, parse_uuid(worker_id)]) do
      {:ok, %{rows: rows}} -> {:ok, rows}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Diagnoses messages `pgflow.start_tasks` declined to convert into tasks.

  Under the archive invariant (every SQL path that ends a step — skip,
  completion, permanent failure — archives that step's queued pgmq messages
  in the same transaction), a declined message has normally already left the
  queue. This probe returns the declined messages still present in the live
  queue, each with the status of the step it belongs to, so the worker can
  tell a benign race (message already archived) from a broken invariant
  (message still queued for a terminal step, doomed to redeliver forever).

  Each returned orphan is `%{msg_id:, step_slug:, step_status:}`; the step
  fields are `nil` when the message has no matching `step_tasks` row.
  """
  @spec orphaned_queue_messages(Ecto.Repo.t(), String.t(), [pos_integer()]) ::
          {:ok,
           [%{msg_id: integer(), step_slug: String.t() | nil, step_status: String.t() | nil}]}
          | {:error, term()}
  def orphaned_queue_messages(repo, flow_slug, msg_ids) do
    # The queue table name derives from the flow slug, which pgflow validates
    # as an identifier — same interpolation precedent as ensure_queue_dropped/2.
    sql = """
    SELECT q.msg_id, st.step_slug, ss.status
    FROM pgmq.q_#{flow_slug} AS q
    LEFT JOIN pgflow.step_tasks AS st
      ON st.flow_slug = $1 AND st.message_id = q.msg_id
    LEFT JOIN pgflow.step_states AS ss
      ON ss.run_id = st.run_id AND ss.step_slug = st.step_slug
    WHERE q.msg_id = ANY($2::bigint[])
    """

    case SQL.query(repo, sql, [flow_slug, msg_ids]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [msg_id, step_slug, status] ->
           %{msg_id: msg_id, step_slug: step_slug, step_status: status}
         end)}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Archives messages out of a flow's live queue.

  Returns `{:ok, archived_msg_ids}` — pgmq reports only the ids it actually
  archived, so ids already absent from the live queue are missing from the
  result rather than raising.
  """
  @spec archive_messages(Ecto.Repo.t(), String.t(), [pos_integer()]) ::
          {:ok, [integer()]} | {:error, term()}
  def archive_messages(repo, flow_slug, msg_ids) do
    sql = "SELECT pgmq.archive($1::text, $2::bigint[])"

    case SQL.query(repo, sql, [flow_slug, msg_ids]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [msg_id] -> msg_id end)}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Compiles and upserts a flow definition.

  ## Parameters

    * `repo` - The Ecto repository
    * `slug` - The flow identifier slug
    * `opts` - Flow options map (e.g., `%{max_retries: 3}`)
    * `steps` - List of step definitions as maps
  """
  @spec compile_flow(Ecto.Repo.t(), String.t(), map(), list(map())) ::
          {:ok, term()} | {:error, term()}
  def compile_flow(repo, slug, opts, steps) do
    sql = "SELECT * FROM pgflow.analyze_and_create_flow($1, $2::jsonb, $3::jsonb)"

    case SQL.query(repo, sql, [slug, opts, steps]) do
      {:ok, %{rows: [[result]]}} -> {:ok, result}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Recompiles a flow definition from runtime options.

  Uses `create_flow` + `add_step` (the proven low-level SQL functions) to
  register a flow. If the flow already exists, it is dropped and re-created
  to ensure the definition matches.

  This operation is destructive for existing flows: all historical run and
  task data for the slug is deleted before recompiling.

  ## Parameters

    * `repo` - The Ecto repository
    * `slug` - The flow identifier slug
    * `opts` - Flow-level options map with keys: `"max_attempts"`, `"base_delay"`, `"timeout"`
    * `steps` - List of step definition maps with keys: `"slug"`, `"deps"`, `"step_type"`,
      and optional `"max_attempts"`, `"base_delay"`, `"timeout"`, `"start_delay"`,
      `"if"`, `"if_not"`, `"when_unmet"`, `"when_exhausted"` (string or atom keys)

  ## Returns

    * `{:ok, %{"status" => status}}` where status is `"compiled"` or `"recompiled"`
    * `{:error, term()}` on failure
  """
  @spec upsert_flow(Ecto.Repo.t(), String.t(), map(), list(map())) ::
          {:ok, map()} | {:error, term()}
  def upsert_flow(repo, slug, opts, steps) do
    max_attempts = Map.get(opts, "max_attempts", 3)
    base_delay = Map.get(opts, "base_delay", 1)
    timeout = Map.get(opts, "timeout", 60)

    tx_result =
      repo.transaction(fn ->
        upsert_flow_transaction(repo, slug, max_attempts, base_delay, timeout, steps)
      end)

    tx_result
  end

  defp upsert_flow_transaction(repo, slug, max_attempts, base_delay, timeout, steps) do
    case upsert_flow_definition(repo, slug, max_attempts, base_delay, timeout, steps) do
      {:ok, exists} ->
        status = if exists, do: "recompiled", else: "compiled"
        %{"status" => status, "differences" => []}

      {:error, reason} ->
        repo.rollback(reason)
    end
  end

  defp upsert_flow_definition(repo, slug, max_attempts, base_delay, timeout, steps) do
    with {:ok, _} <- advisory_lock_slug(repo, slug),
         {:ok, exists} <- flow_exists?(repo, slug),
         :ok <- maybe_delete_existing_flow(repo, slug, exists),
         :ok <- create_flow_definition(repo, slug, max_attempts, base_delay, timeout),
         :ok <- add_flow_steps(repo, slug, steps) do
      {:ok, exists}
    end
  end

  @doc """
  Deletes a flow and all associated data (runs, step states, step tasks, queue).

  ## Parameters

    * `repo` - The Ecto repository
    * `slug` - The flow identifier slug

  ## Returns

    * `:ok` on success (including when flow doesn't exist)
    * `{:error, term()}` on failure
  """
  @spec delete_flow(Ecto.Repo.t(), String.t()) :: :ok | {:error, term()}
  def delete_flow(repo, slug) do
    tx_result =
      repo.transaction(fn ->
        with {:ok, _} <- advisory_lock_slug(repo, slug),
             :ok <- delete_flow_rows(repo, slug) do
          :ok
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end)

    case tx_result do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # Deletes all flow data in dependency order:
  # tasks -> states -> runs -> deps -> steps -> flow -> queue
  defp delete_flow_rows(repo, slug) do
    with {:ok, _} <- delete_step_tasks(repo, slug),
         {:ok, _} <- delete_step_states(repo, slug),
         {:ok, _} <- delete_runs(repo, slug),
         {:ok, _} <- delete_deps(repo, slug),
         {:ok, _} <- delete_steps(repo, slug),
         {:ok, _} <- delete_flow_record(repo, slug) do
      drop_queue(repo, slug)
    end
  end

  defp delete_step_tasks(repo, slug) do
    SQL.query(repo, "DELETE FROM pgflow.step_tasks WHERE flow_slug = $1", [slug])
  end

  defp delete_step_states(repo, slug) do
    SQL.query(
      repo,
      """
      DELETE FROM pgflow.step_states WHERE run_id IN (
        SELECT run_id FROM pgflow.runs WHERE flow_slug = $1
      )
      """,
      [slug]
    )
  end

  defp delete_runs(repo, slug) do
    SQL.query(repo, "DELETE FROM pgflow.runs WHERE flow_slug = $1", [slug])
  end

  defp delete_deps(repo, slug) do
    SQL.query(repo, "DELETE FROM pgflow.deps WHERE flow_slug = $1", [slug])
  end

  defp delete_steps(repo, slug) do
    SQL.query(repo, "DELETE FROM pgflow.steps WHERE flow_slug = $1", [slug])
  end

  defp delete_flow_record(repo, slug) do
    SQL.query(repo, "DELETE FROM pgflow.flows WHERE flow_slug = $1", [slug])
  end

  # pgmq.drop_queue raises if the queue doesn't exist, which poisons the
  # enclosing transaction. Check existence first via to_regclass so a
  # missing queue (flow had rows but queue was already dropped or was
  # never created) resolves cleanly.
  defp drop_queue(repo, slug) do
    queue_table = "pgmq.q_" <> slug

    case SQL.query(repo, "SELECT to_regclass($1::text) IS NOT NULL", [queue_table]) do
      {:ok, %{rows: [[true]]}} ->
        case SQL.query(repo, "SELECT pgmq.drop_queue($1::text)", [slug]) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, reason}
        end

      {:ok, _} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp advisory_lock_slug(repo, slug) do
    SQL.query(repo, "SELECT pg_advisory_xact_lock(hashtext($1))", [slug])
  end

  defp maybe_delete_existing_flow(_repo, _slug, false), do: :ok

  defp maybe_delete_existing_flow(repo, slug, true) do
    case delete_flow_rows(repo, slug) do
      :ok -> :ok
      {:error, reason} -> {:error, {:delete_failed, reason}}
    end
  end

  defp create_flow_definition(repo, slug, max_attempts, base_delay, timeout) do
    create_sql = "SELECT pgflow.create_flow($1, $2, $3, $4)"

    case SQL.query(repo, create_sql, [slug, max_attempts, base_delay, timeout]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, {:create_failed, reason}}
    end
  end

  defp add_flow_steps(repo, slug, steps) do
    Enum.reduce_while(steps, :ok, fn step, :ok ->
      step_slug = step_value(step, :slug) || Map.fetch!(step, "slug")
      deps = step_value(step, :deps, [])
      step_type = step_value(step, :step_type, "single")
      step_max = step_value(step, :max_attempts)
      step_delay = step_value(step, :base_delay)
      step_timeout = step_value(step, :timeout)
      start_delay = step_value(step, :start_delay)

      args = [slug, step_slug, deps, step_max, step_delay, step_timeout, start_delay, step_type]

      {add_sql, args} =
        if condition_opts?(step) do
          {
            "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12)",
            args ++
              [
                step_value(step, :if),
                step_value(step, :if_not),
                mode_or_default(step, :when_unmet, "skip"),
                mode_or_default(step, :when_exhausted, "fail")
              ]
          }
        else
          {"SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)", args}
        end

      case SQL.query(repo, add_sql, args) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:add_step_failed, step_slug, reason}}}
      end
    end)
  end

  defp condition_opts?(step) do
    Enum.any?([:if, :if_not, :when_unmet, :when_exhausted], &step_has_key?(step, &1))
  end

  defp step_has_key?(step, key) do
    Map.has_key?(step, to_string(key)) or Map.has_key?(step, key)
  end

  defp step_value(step, key, default \\ nil) do
    case Map.fetch(step, to_string(key)) do
      {:ok, value} -> value
      :error -> Map.get(step, key, default)
    end
  end

  # Mode columns are NOT NULL; never bind nil (overrides add_step DEFAULTs).
  defp mode_or_default(step, key, default) do
    case step_value(step, key, default) do
      nil -> default
      mode -> normalize_mode(mode)
    end
  end

  defp normalize_mode(:skip_cascade), do: "skip-cascade"
  defp normalize_mode("skip_cascade"), do: "skip-cascade"
  defp normalize_mode(mode) when is_atom(mode), do: Atom.to_string(mode)
  defp normalize_mode(mode) when is_binary(mode), do: mode

  @doc """
  Retrieves the input data for a flow run.
  """
  @spec get_flow_input(Ecto.Repo.t(), String.t()) ::
          {:ok, map() | list()} | {:error, term()}
  def get_flow_input(repo, run_id) do
    case execute_rpc(repo, "get_flow_input", [parse_uuid(run_id)],
           schema: @pgflow_schema,
           mode: :raw
         ) do
      {:ok, [%{get_flow_input: nil}]} -> {:error, :not_found}
      {:ok, [%{get_flow_input: input}]} -> {:ok, input}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Checks if a flow exists in the database.
  """
  @spec flow_exists?(Ecto.Repo.t(), String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def flow_exists?(repo, flow_slug) do
    case execute_rpc(repo, "flow_exists", [flow_slug], schema: @pgflow_schema, mode: :raw) do
      {:ok, [%{flow_exists: result}]} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Checks if a slug is valid according to core pgflow rules.
  """
  @spec valid_slug?(Ecto.Repo.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def valid_slug?(repo, slug) do
    sql = "SELECT pgflow.is_valid_slug($1)"

    case SQL.query(repo, sql, [slug]) do
      {:ok, %{rows: [[result]]}} -> {:ok, result}
      {:ok, %{rows: []}} -> {:ok, false}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Deletes a message from a PGMQ queue.
  """
  @spec delete_message(Ecto.Repo.t(), String.t(), pos_integer()) ::
          {:ok, boolean()} | {:error, term()}
  def delete_message(repo, queue_name, msg_id) do
    sql = "SELECT pgmq.delete($1::text, $2::bigint)"

    case SQL.query(repo, sql, [queue_name, msg_id]) do
      {:ok, %{rows: [[deleted]]}} -> {:ok, deleted}
      {:ok, %{rows: []}} -> {:ok, false}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Retrieves the output for a specific step in a flow run.
  """
  @spec get_step_output(Ecto.Repo.t(), String.t(), String.t()) ::
          {:ok, map() | nil} | {:error, term()}
  def get_step_output(repo, run_id, step_slug) do
    case execute_rpc(repo, "get_step_output", [parse_uuid(run_id), step_slug],
           schema: @pgflow_schema,
           mode: :raw
         ) do
      {:ok, [%{get_step_output: output}]} -> {:ok, output}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @typedoc """
  Result of pruning old run data.
  """
  @type prune_result :: %{
          deleted_runs: non_neg_integer(),
          deleted_step_states: non_neg_integer(),
          deleted_step_tasks: non_neg_integer(),
          deleted_workers: non_neg_integer()
        }

  @doc """
  Prunes old flow run data older than the specified retention period.

  ## Options

    * `:flow_slugs` - List of flow slugs to prune (default: all flows)
  """
  @spec prune_data(Ecto.Repo.t(), pos_integer(), keyword()) ::
          {:ok, prune_result()} | {:error, term()}
  def prune_data(repo, retention_hours, opts \\ []) do
    flow_slugs = Keyword.get(opts, :flow_slugs)

    sql = "SELECT * FROM pgflow.prune_data_older_than(make_interval(hours => $1), $2)"

    case SQL.query(repo, sql, [retention_hours, flow_slugs]) do
      {:ok, %{rows: [[deleted_runs, deleted_states, deleted_tasks, deleted_workers]]}} ->
        {:ok,
         %{
           deleted_runs: deleted_runs,
           deleted_step_states: deleted_states,
           deleted_step_tasks: deleted_tasks,
           deleted_workers: deleted_workers
         }}

      {:ok, %{rows: []}} ->
        {:ok,
         %{
           deleted_runs: 0,
           deleted_step_states: 0,
           deleted_step_tasks: 0,
           deleted_workers: 0
         }}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Recovers stalled tasks via the `pgflow.recover_stalled_tasks` helper, returning
  the number requeued.

  A task is stalled once it has been `started` longer than its effective timeout
  — `coalesce(step.opt_timeout, flow.opt_timeout)` — plus `stale_threshold`
  seconds of buffer. Stalled tasks are reset to `queued`; past a requeue cap they
  are archived and marked `permanently_stalled_at`.

  The deadline is step-aware on purpose: upstream pgflow's
  `requeue_stalled_tasks()` deadlines on the flow timeout alone, which would
  reclaim a healthy long step (e.g. a step with `timeout: 120` under a 30s flow
  default) mid-flight. Step-awareness matches how `start_tasks` sets each
  message's pgmq visibility timeout.
  """
  @spec recover_stalled_tasks(Ecto.Repo.t(), pos_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def recover_stalled_tasks(repo, stale_threshold) do
    case execute_rpc(repo, "recover_stalled_tasks", [stale_threshold],
           schema: @pgflow_schema,
           mode: :single
         ) do
      {:ok, %{recovered_count: count}} -> {:ok, count}
      {:error, :not_found} -> {:ok, 0}
      {:error, reason} -> {:error, reason}
    end
  end
end
