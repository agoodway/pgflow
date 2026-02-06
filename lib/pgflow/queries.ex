defmodule PgFlow.Queries do
  @moduledoc """
  SQL query interface for calling pgflow PostgreSQL functions.

  This module provides functions for interacting with the pgflow schema
  using raw SQL queries via `SQL.query/3`.

  All functions that accept JSON data expect Elixir terms that will be
  encoded with Jason. Results are returned as tuples following the
  `{:ok, result}` or `{:error, reason}` convention.
  """

  alias Ecto.Adapters.SQL

  @doc """
  Starts a new flow execution.

  ## Parameters

    * `repo` - The Ecto repository
    * `flow_slug` - The flow identifier slug
    * `input` - Input data as an Elixir term (will be encoded as JSONB)

  ## Returns

    * `{:ok, run_id}` - The UUID of the created flow run
    * `{:error, reason}` - Error details if the operation fails

  ## Examples

      iex> start_flow(MyApp.Repo, "my_flow", %{key: "value"})
      {:ok, "550e8400-e29b-41d4-a716-446655440000"}
  """
  @spec start_flow(Ecto.Repo.t(), String.t(), map() | list()) ::
          {:ok, String.t()} | {:error, term()}
  def start_flow(repo, flow_slug, input) do
    # Pass map directly - Postgrex handles JSONB encoding automatically
    sql = "SELECT * FROM pgflow.start_flow($1, $2::jsonb)"

    case SQL.query(repo, sql, [flow_slug, input]) do
      {:ok, %{rows: [[run_id_bin | _rest]]}} ->
        # Convert binary UUID to string format
        {:ok, run_id} = Ecto.UUID.load(run_id_bin)
        {:ok, run_id}

      {:ok, %{rows: []}} ->
        {:error, :no_result}

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

  ## Examples

      iex> complete_task(MyApp.Repo, run_id, "process_data", 0, %{result: "success"})
      {:ok, nil}
  """
  @spec complete_task(Ecto.Repo.t(), String.t(), String.t(), non_neg_integer(), map() | list()) ::
          {:ok, term()} | {:error, term()}
  def complete_task(repo, run_id, step_slug, task_index, output) do
    # Pass map directly - Postgrex handles JSONB encoding automatically
    sql = "SELECT * FROM pgflow.complete_task($1, $2, $3, $4::jsonb)"

    # Convert run_id string to binary UUID for Postgrex
    {:ok, run_id_bin} = Ecto.UUID.dump(run_id)

    case SQL.query(repo, sql, [run_id_bin, step_slug, task_index, output]) do
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

  ## Examples

      iex> fail_task(MyApp.Repo, run_id, "process_data", 0, "Network timeout")
      {:ok, nil}
  """
  @spec fail_task(Ecto.Repo.t(), String.t(), String.t(), non_neg_integer(), String.t()) ::
          {:ok, term()} | {:error, term()}
  def fail_task(repo, run_id, step_slug, task_index, error_message) do
    sql = "SELECT * FROM pgflow.fail_task($1, $2, $3, $4)"

    # Convert run_id string to binary UUID for Postgrex
    {:ok, run_id_bin} = Ecto.UUID.dump(run_id)

    case SQL.query(repo, sql, [run_id_bin, step_slug, task_index, error_message]) do
      {:ok, %{rows: [row | _]}} -> {:ok, row}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Reads messages from a queue with polling and visibility timeout.

  Uses pgmq.read_with_poll to poll for available messages. Messages are made
  invisible for the visibility timeout period to prevent duplicate processing.

  ## Parameters

    * `repo` - The Ecto repository
    * `queue_name` - The name of the queue to poll (matches flow_slug)
    * `visibility_timeout` - Time in seconds messages remain invisible
    * `batch_size` - Maximum number of messages to retrieve
    * `opts` - Optional keyword list

  ## Options (required)

    * `:max_poll_seconds` - Maximum seconds to poll
    * `:poll_interval_ms` - Milliseconds between poll attempts

  ## Returns

    * `{:ok, messages}` - List of message records from pgmq
    * `{:error, reason}` - Error details if the operation fails

  Each message is a list containing: `[msg_id, read_ct, enqueued_at, vt, message_json]`
  where `message_json` is a JSON string with flow_slug, run_id, step_slug, task_index.

  ## Examples

      iex> read_with_poll(MyApp.Repo, "my_flow", 30, 10)
      {:ok, [[1, 1, ~U[2026-01-17 00:00:00Z], ~U[2026-01-17 00:00:30Z], "{\"flow_slug\":\"my_flow\",...}"]]}
  """
  @spec read_with_poll(Ecto.Repo.t(), String.t(), pos_integer(), pos_integer(), keyword()) ::
          {:ok, list(list())} | {:error, term()}
  def read_with_poll(repo, queue_name, visibility_timeout, batch_size, opts \\ []) do
    max_poll_seconds = Keyword.fetch!(opts, :max_poll_seconds)
    poll_interval_ms = Keyword.fetch!(opts, :poll_interval_ms)

    sql = """
    SELECT msg_id, read_ct, enqueued_at, vt, message
    FROM pgmq.read_with_poll(
      queue_name => $1::text,
      vt => $2::integer,
      qty => $3::integer,
      max_poll_seconds => $4::integer,
      poll_interval_ms => $5::integer
    )
    """

    case SQL.query(repo, sql, [
           queue_name,
           visibility_timeout,
           batch_size,
           max_poll_seconds,
           poll_interval_ms
         ]) do
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

  Each task detail record contains:
  `[flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input]`

  ## Examples

      iex> start_tasks(MyApp.Repo, "my_flow", [1, 2, 3], worker_id)
      {:ok, [["my_flow", "run-uuid", "step1", "{}", 1, 0, "{}"]]}
  """
  @spec start_tasks(Ecto.Repo.t(), String.t(), list(pos_integer()), String.t()) ::
          {:ok, list(list())} | {:error, term()}
  def start_tasks(repo, flow_slug, msg_ids, worker_id) do
    sql = "SELECT * FROM pgflow.start_tasks($1, $2, $3)"

    # Convert worker_id string to binary UUID for Postgrex
    {:ok, worker_id_bin} = Ecto.UUID.dump(worker_id)

    case SQL.query(repo, sql, [flow_slug, msg_ids, worker_id_bin]) do
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

  ## Returns

    * `{:ok, result}` - Analysis and creation result
    * `{:error, reason}` - Error details if the operation fails

  Each step should contain keys like `slug`, `queue`, `fan_in`, `fan_out`, `next`, etc.

  ## Examples

      iex> steps = [%{slug: "step1", queue: "default", fan_in: "none", fan_out: "none"}]
      iex> compile_flow(MyApp.Repo, "my_flow", %{}, steps)
      {:ok, result}
  """
  @spec compile_flow(Ecto.Repo.t(), String.t(), map(), list(map())) ::
          {:ok, term()} | {:error, term()}
  def compile_flow(repo, slug, opts, steps) do
    # Pass maps/lists directly - Postgrex handles JSONB encoding automatically
    sql = "SELECT * FROM pgflow.analyze_and_create_flow($1, $2::jsonb, $3::jsonb)"

    case SQL.query(repo, sql, [slug, opts, steps]) do
      {:ok, %{rows: [[result]]}} -> {:ok, result}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Retrieves the input data for a flow run.

  ## Parameters

    * `repo` - The Ecto repository
    * `run_id` - The flow run UUID

  ## Returns

    * `{:ok, input}` - The input data as a decoded JSON term
    * `{:error, reason}` - Error details if the operation fails

  ## Examples

      iex> get_flow_input(MyApp.Repo, "550e8400-e29b-41d4-a716-446655440000")
      {:ok, %{"key" => "value"}}
  """
  @spec get_flow_input(Ecto.Repo.t(), String.t()) ::
          {:ok, map() | list()} | {:error, term()}
  def get_flow_input(repo, run_id) do
    sql = "SELECT input FROM pgflow.runs WHERE run_id = $1"

    case SQL.query(repo, sql, [run_id]) do
      {:ok, %{rows: [[input_json]]}} ->
        case Jason.decode(input_json) do
          {:ok, input} -> {:ok, input}
          {:error, error} -> {:error, error}
        end

      {:ok, %{rows: []}} ->
        {:error, :not_found}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Checks if a flow exists in the database.

  ## Parameters

    * `repo` - The Ecto repository
    * `flow_slug` - The flow identifier slug

  ## Returns

    * `{:ok, true}` - Flow exists
    * `{:ok, false}` - Flow does not exist
    * `{:error, reason}` - Error details if the operation fails

  ## Examples

      iex> flow_exists?(MyApp.Repo, "my_flow")
      {:ok, true}

      iex> flow_exists?(MyApp.Repo, "nonexistent")
      {:ok, false}
  """
  @spec flow_exists?(Ecto.Repo.t(), String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def flow_exists?(repo, flow_slug) do
    sql = "SELECT 1 FROM pgflow.flows WHERE flow_slug = $1"

    case SQL.query(repo, sql, [flow_slug]) do
      {:ok, %{num_rows: 1}} -> {:ok, true}
      {:ok, %{num_rows: 0}} -> {:ok, false}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Registers a worker in the database.

  Creates a new worker record or updates the heartbeat if the worker already exists.

  ## Parameters

    * `repo` - The Ecto repository
    * `worker_id` - The worker identifier (UUID string)
    * `queue_name` - The queue name (flow_slug)
    * `function_name` - The function name (e.g., "elixir:MyApp.Flows.MyFlow")

  ## Returns

    * `{:ok, nil}` - Success
    * `{:error, reason}` - Error details if the operation fails

  ## Examples

      iex> register_worker(MyApp.Repo, "550e8400-e29b-41d4-a716-446655440000", "my_flow", "elixir:MyApp.Flows.MyFlow")
      {:ok, nil}
  """
  @spec register_worker(Ecto.Repo.t(), String.t(), String.t(), String.t()) ::
          {:ok, nil} | {:error, term()}
  def register_worker(repo, worker_id, queue_name, function_name) do
    sql = """
    INSERT INTO pgflow.workers (worker_id, queue_name, function_name, started_at, last_heartbeat_at)
    VALUES ($1, $2, $3, NOW(), NOW())
    ON CONFLICT (worker_id) DO UPDATE SET
      last_heartbeat_at = NOW()
    """

    {:ok, worker_id_bin} = Ecto.UUID.dump(worker_id)

    case SQL.query(repo, sql, [worker_id_bin, queue_name, function_name]) do
      {:ok, _} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Deletes a message from a PGMQ queue.

  ## Parameters

    * `repo` - The Ecto repository
    * `queue_name` - The queue name (flow_slug)
    * `msg_id` - The message ID to delete

  ## Returns

    * `{:ok, deleted}` - Whether the message was deleted
    * `{:error, reason}` - Error details if the operation fails

  ## Examples

      iex> delete_message(MyApp.Repo, "my_flow", 123)
      {:ok, true}
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
  Marks a worker as stopped.

  Sets the `stopped_at` timestamp for graceful shutdown signaling.

  ## Parameters

    * `repo` - The Ecto repository
    * `worker_id` - The worker identifier (UUID string)

  ## Returns

    * `{:ok, nil}` - Success
    * `{:error, reason}` - Error details if the operation fails

  ## Examples

      iex> mark_worker_stopped(MyApp.Repo, "550e8400-e29b-41d4-a716-446655440000")
      {:ok, nil}
  """
  @spec mark_worker_stopped(Ecto.Repo.t(), String.t()) ::
          {:ok, nil} | {:error, term()}
  def mark_worker_stopped(repo, worker_id) do
    sql = """
    UPDATE pgflow.workers
    SET stopped_at = clock_timestamp()
    WHERE worker_id = $1
    """

    {:ok, worker_id_bin} = Ecto.UUID.dump(worker_id)

    case SQL.query(repo, sql, [worker_id_bin]) do
      {:ok, _} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Recovers stalled tasks by resetting step_tasks stuck in 'started' status.

  Tasks that have been in 'started' status longer than the stale threshold
  are reset to 'queued' so they can be re-processed by workers.

  ## Parameters

    * `repo` - The Ecto repository
    * `stale_threshold` - Seconds after which a started task is considered stalled

  ## Returns

    * `{:ok, count}` - Number of tasks recovered
    * `{:error, reason}` - Error details if the operation fails

  ## Examples

      iex> recover_stalled_tasks(MyApp.Repo, 60)
      {:ok, 3}
  """
  @spec recover_stalled_tasks(Ecto.Repo.t(), pos_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def recover_stalled_tasks(repo, stale_threshold) do
    sql = """
    WITH stalled AS (
      UPDATE pgflow.step_tasks
      SET status = 'queued', started_at = NULL, last_worker_id = NULL
      WHERE status = 'started'
        AND started_at < NOW() - make_interval(secs => $1::double precision)
      RETURNING flow_slug, message_id
    ),
    vt_reset AS (
      SELECT pgflow.set_vt_batch(
        s.flow_slug,
        array_agg(s.message_id),
        array_agg(0::integer)
      )
      FROM stalled s
      WHERE s.message_id IS NOT NULL
      GROUP BY s.flow_slug
    )
    SELECT
      (SELECT count(*) FROM stalled) AS recovered_count,
      (SELECT count(*) FROM vt_reset) AS vt_batches
    """

    case SQL.query(repo, sql, [stale_threshold]) do
      {:ok, %{rows: [[count, _vt_batches]]}} -> {:ok, count}
      {:ok, %{rows: []}} -> {:ok, 0}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Retrieves the output for a specific step in a flow run.

  ## Parameters

    * `repo` - The Ecto repository
    * `run_id` - The flow run UUID (string)
    * `step_slug` - The step identifier slug

  ## Returns

    * `{:ok, output}` - The step output as a decoded map/list
    * `{:ok, nil}` - Step has no output yet
    * `{:error, reason}` - Error details if the operation fails

  ## Examples

      iex> get_step_output(MyApp.Repo, "550e8400-...", "validate")
      {:ok, %{"valid" => true}}
  """
  @spec get_step_output(Ecto.Repo.t(), String.t(), String.t()) ::
          {:ok, map() | nil} | {:error, term()}
  def get_step_output(repo, run_id, step_slug) do
    sql = "SELECT output FROM pgflow.step_states WHERE run_id = $1 AND step_slug = $2"

    {:ok, run_id_bin} = Ecto.UUID.dump(run_id)

    case SQL.query(repo, sql, [run_id_bin, step_slug]) do
      {:ok, %{rows: [[output]]}} -> {:ok, output}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, error} -> {:error, error}
    end
  end
end
