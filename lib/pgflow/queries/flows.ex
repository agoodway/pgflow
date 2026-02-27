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
    sql = "SELECT * FROM pgflow.start_flow($1, $2::jsonb)"

    case SQL.query(repo, sql, [flow_slug, input]) do
      {:ok, %{rows: [[run_id_bin | _rest]]}} -> {:ok, format_uuid(run_id_bin)}
      {:ok, %{rows: []}} -> {:error, :no_result}
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

    case SQL.query(repo, sql, [parse_uuid(run_id), step_slug, task_index, output]) do
      {:ok, %{rows: [row | _]}} -> {:ok, row}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
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

    case SQL.query(repo, sql, [parse_uuid(run_id), step_slug, task_index, error_message]) do
      {:ok, %{rows: [row | _]}} -> {:ok, row}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Reads messages from a queue without blocking (non-blocking read).

  Uses pgmq.read() to fetch available messages. Returns immediately whether or
  not messages are available. Messages are made invisible for the visibility
  timeout period to prevent duplicate processing.

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

    case SQL.query(repo, sql, [queue_name, visibility_timeout, batch_size]) do
      {:ok, %{rows: rows}} -> {:ok, rows}
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
      and optional `"max_attempts"`, `"base_delay"`, `"timeout"`, `"start_delay"`

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
        with {:ok, _} <- advisory_lock_slug(repo, slug),
             {:ok, exists} <- flow_exists?(repo, slug),
             :ok <- maybe_delete_existing_flow(repo, slug, exists),
             :ok <- create_flow_definition(repo, slug, max_attempts, base_delay, timeout),
             :ok <- add_flow_steps(repo, slug, steps) do
          status = if exists, do: "recompiled", else: "compiled"
          %{"status" => status, "differences" => []}
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end)

    case tx_result do
      {:ok, status_map} -> {:ok, status_map}
      {:error, reason} -> {:error, reason}
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

  defp delete_flow_rows(repo, slug) do
    # Delete in dependency order: tasks -> states -> runs -> deps -> steps -> flow -> queue
    delete_tasks_sql = "DELETE FROM pgflow.step_tasks WHERE flow_slug = $1"

    delete_states_sql = """
    DELETE FROM pgflow.step_states WHERE run_id IN (
      SELECT run_id FROM pgflow.runs WHERE flow_slug = $1
    )
    """

    delete_runs_sql = "DELETE FROM pgflow.runs WHERE flow_slug = $1"
    delete_deps_sql = "DELETE FROM pgflow.deps WHERE flow_slug = $1"
    delete_steps_sql = "DELETE FROM pgflow.steps WHERE flow_slug = $1"
    delete_flow_sql = "DELETE FROM pgflow.flows WHERE flow_slug = $1"

    with {:ok, _} <- SQL.query(repo, delete_tasks_sql, [slug]),
         {:ok, _} <- SQL.query(repo, delete_states_sql, [slug]),
         {:ok, _} <- SQL.query(repo, delete_runs_sql, [slug]),
         {:ok, _} <- SQL.query(repo, delete_deps_sql, [slug]),
         {:ok, _} <- SQL.query(repo, delete_steps_sql, [slug]),
         {:ok, _} <- SQL.query(repo, delete_flow_sql, [slug]) do
      # Try to drop the pgmq queue, ignore errors
      _ = SQL.query(repo, "SELECT pgmq.drop_queue($1::text)", [slug])
      :ok
    else
      {:error, reason} -> {:error, reason}
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
      step_slug = Map.fetch!(step, "slug")
      deps = Map.get(step, "deps", [])
      step_type = Map.get(step, "step_type", "single")
      step_max = Map.get(step, "max_attempts")
      step_delay = Map.get(step, "base_delay")
      step_timeout = Map.get(step, "timeout")
      start_delay = Map.get(step, "start_delay")

      add_sql = "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)"

      case SQL.query(repo, add_sql, [
             slug,
             step_slug,
             deps,
             step_max,
             step_delay,
             step_timeout,
             start_delay,
             step_type
           ]) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:add_step_failed, step_slug, reason}}}
      end
    end)
  end

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
  Recovers stalled tasks by resetting step_tasks stuck in 'started' status.

  Tasks that have been in 'started' status longer than the stale threshold
  are reset to 'queued' so they can be re-processed by workers.
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
