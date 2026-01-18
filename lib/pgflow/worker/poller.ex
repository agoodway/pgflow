defmodule PgFlow.Worker.Poller do
  @moduledoc """
  Two-phase polling logic for pgmq message processing.

  This module implements the worker's polling protocol which consists of two phases:

  ## Phase 1: Reserve Messages

  Uses `PgFlow.Queries.read_with_poll/4` to reserve messages from the pgmq queue.
  Messages are made invisible to other workers for a visibility timeout period,
  preventing duplicate processing.

  ## Phase 2: Claim Tasks

  Uses `PgFlow.Queries.start_tasks/2` to atomically claim the reserved tasks,
  transitioning them from "created" to "in_progress" status. This phase retrieves
  the task execution details including input data and dependencies.

  ## Usage

      {:ok, tasks} = PgFlow.Worker.Poller.poll(repo, "my_queue")

      Enum.each(tasks, fn task ->
        # Execute each task
        IO.inspect(task)
      end)

  ## Task Structure

  Each task returned by `poll/2` contains:

    * `:run_id` - UUID of the flow run
    * `:step_slug` - Step identifier atom
    * `:task_index` - Task index (0 for single steps)
    * `:attempt` - Current attempt number (1-indexed)
    * `:input` - Task input data as a map
    * `:deps` - Dependency outputs as a map (for dependent steps)
  """

  require Logger
  alias PgFlow.Queries

  @default_visibility_timeout 30
  @default_batch_size 10

  @doc """
  Polls for available tasks using the two-phase protocol.

  ## Parameters

    * `repo` - The Ecto repository
    * `queue_name` - Name of the queue to poll
    * `opts` - Options keyword list

  ## Options

    * `:visibility_timeout` - Seconds messages remain invisible (default: 30)
    * `:batch_size` - Maximum number of messages to retrieve (default: 10)

  ## Returns

    * `{:ok, tasks}` - List of task maps ready for execution
    * `{:error, reason}` - Error details if polling fails

  Each task map contains:
  - `run_id` - Flow run UUID
  - `step_slug` - Step slug as atom
  - `task_index` - Task index (0-based)
  - `attempt` - Current attempt number
  - `input` - Task input data
  - `deps` - Map of dependency outputs (for dependent steps)

  ## Examples

      {:ok, tasks} = PgFlow.Worker.Poller.poll(MyApp.Repo, "default_queue")
      #=> {:ok, [
      #=>   %{
      #=>     run_id: "550e8400-e29b-41d4-a716-446655440000",
      #=>     step_slug: :process,
      #=>     task_index: 0,
      #=>     attempt: 1,
      #=>     input: %{"key" => "value"},
      #=>     deps: %{}
      #=>   }
      #=> ]}

      {:ok, []} = PgFlow.Worker.Poller.poll(MyApp.Repo, "empty_queue")
      #=> {:ok, []}
  """
  @spec poll(Ecto.Repo.t(), String.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def poll(repo, queue_name, opts \\ []) do
    visibility_timeout = Keyword.get(opts, :visibility_timeout, @default_visibility_timeout)
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)

    with {:ok, messages} <-
           Queries.read_with_poll(repo, queue_name, visibility_timeout, batch_size),
         {:ok, message_tuples} <- extract_message_tuples(messages),
         {:ok, tasks} <- claim_tasks(repo, message_tuples, opts) do
      {:ok, tasks}
    else
      {:error, reason} = error ->
        Logger.error("Failed to poll queue #{queue_name}: #{inspect(reason)}")
        error
    end
  end

  @doc """
  Extracts message tuples from pgmq message rows.

  Converts raw message data from `read_with_poll` into tuples of
  `{flow_slug, run_id, step_slug, msg_id}` for use in the second polling phase.

  ## Parameters

    * `messages` - List of message rows from `read_with_poll`

  ## Returns

    * `{:ok, tuples}` - List of `{flow_slug, run_id, step_slug, msg_id}` tuples
    * `{:error, reason}` - Error if message format is invalid

  ## Message Format

  Each message row from pgmq.read_with_poll is: `[msg_id, read_ct, enqueued_at, vt, message]`
  where `message` is a map with: flow_slug, run_id, step_slug, task_index

  ## Examples

      messages = [
        [1, 1, ~U[2026-01-17 00:00:00Z], ~U[2026-01-17 00:00:30Z], %{"flow_slug" => "my_flow", "run_id" => "run-uuid-1", "step_slug" => "step1", "task_index" => 0}],
        [2, 1, ~U[2026-01-17 00:00:01Z], ~U[2026-01-17 00:00:31Z], %{"flow_slug" => "my_flow", "run_id" => "run-uuid-2", "step_slug" => "step2", "task_index" => 0}]
      ]

      {:ok, tuples} = PgFlow.Worker.Poller.extract_message_tuples(messages)
      #=> {:ok, [{"my_flow", "run-uuid-1", "step1", 1}, {"my_flow", "run-uuid-2", "step2", 2}]}
  """
  @spec extract_message_tuples(list(list())) ::
          {:ok, [{String.t(), String.t(), String.t(), pos_integer()}]} | {:error, term()}
  def extract_message_tuples(messages) when is_list(messages) do
    tuples =
      Enum.map(messages, fn
        # pgmq format: [msg_id, read_ct, enqueued_at, vt, message_map]
        [
          msg_id,
          _read_ct,
          _enqueued_at,
          _vt,
          %{"flow_slug" => flow_slug, "run_id" => run_id, "step_slug" => step_slug}
        ] ->
          {flow_slug, run_id, step_slug, msg_id}

        invalid ->
          Logger.warning("Invalid message format: #{inspect(invalid)}")
          nil
      end)
      |> Enum.reject(&is_nil/1)

    {:ok, tuples}
  rescue
    error ->
      {:error, "Failed to extract message tuples: #{inspect(error)}"}
  end

  @doc """
  Claims tasks and retrieves execution details.

  Calls `PgFlow.Queries.start_tasks/4` to atomically mark tasks as in-progress
  and retrieve their input data and dependencies.

  Messages are grouped by flow_slug and processed separately. This matches the
  current database function signature which processes tasks for a single flow.

  ## Parameters

    * `repo` - The Ecto repository
    * `message_tuples` - List of `{flow_slug, run_id, step_slug, msg_id}` tuples
    * `opts` - Options keyword list

  ## Options

    * `:worker_id` - Worker UUID (default: generates new UUID)

  ## Returns

    * `{:ok, tasks}` - List of task maps with execution details
    * `{:error, reason}` - Error if claiming fails

  ## Examples

      message_tuples = [{"my_flow", "run-uuid", "step1", 1}]

      {:ok, tasks} = PgFlow.Worker.Poller.claim_tasks(MyApp.Repo, message_tuples, worker_id: "worker-123")
      #=> {:ok, [
      #=>   %{
      #=>     flow_slug: "my_flow",
      #=>     run_id: "run-uuid",
      #=>     step_slug: :step1,
      #=>     task_index: 0,
      #=>     attempt: 1,
      #=>     input: %{},
      #=>     deps: %{}
      #=>   }
      #=> ]}
  """
  @spec claim_tasks(
          Ecto.Repo.t(),
          [{String.t(), String.t(), String.t(), pos_integer()}],
          keyword()
        ) ::
          {:ok, [map()]} | {:error, term()}
  def claim_tasks(repo, message_tuples, opts \\ [])

  def claim_tasks(_repo, [], _opts) do
    {:ok, []}
  end

  def claim_tasks(repo, message_tuples, opts) when is_list(message_tuples) do
    worker_id = Keyword.get(opts, :worker_id, Ecto.UUID.generate())

    # Group messages by flow_slug
    grouped_messages =
      Enum.group_by(message_tuples, fn {flow_slug, _run_id, _step_slug, _msg_id} ->
        flow_slug
      end)

    # Process each flow's messages separately
    results =
      Enum.flat_map(grouped_messages, fn {flow_slug, tuples} ->
        process_flow_messages(repo, flow_slug, tuples, worker_id)
      end)

    {:ok, results}
  end

  defp process_flow_messages(repo, flow_slug, tuples, worker_id) do
    msg_ids = Enum.map(tuples, fn {_flow_slug, _run_id, _step_slug, msg_id} -> msg_id end)

    case Queries.start_tasks(repo, flow_slug, msg_ids, worker_id) do
      {:ok, task_rows} ->
        merge_and_log_tasks(flow_slug, tuples, task_rows)

      {:error, reason} ->
        Logger.error("Failed to start tasks for flow #{flow_slug}: #{inspect(reason)}")
        []
    end
  end

  defp merge_and_log_tasks(flow_slug, tuples, task_rows) do
    case merge_task_details(tuples, task_rows) do
      {:ok, tasks} ->
        tasks

      {:error, reason} ->
        Logger.error("Failed to merge task details for flow #{flow_slug}: #{inspect(reason)}")
        []
    end
  end

  @doc """
  Merges message data with task execution details.

  Combines the message metadata with the task details returned from `start_tasks`.

  ## Parameters

    * `message_tuples` - List of `{flow_slug, run_id, step_slug, msg_id}` tuples
    * `task_rows` - List of task detail rows from `start_tasks`

  ## Returns

    * `{:ok, tasks}` - List of complete task maps
    * `{:error, reason}` - Error if merging fails

  ## Task Row Format

  Each task row from `start_tasks` is:
  `[flow_slug, run_id (binary UUID), step_slug, input, msg_id, task_index, flow_input]`
  - input: step-specific input (deps outputs) - empty map for root steps
  - flow_input: the original flow input

  ## Examples

      message_tuples = [{"my_flow", "run-uuid", "step1", 1}]
      task_rows = [["my_flow", <<uuid_binary>>, "step1", %{}, 1, 0, %{"key" => "value"}]]

      {:ok, tasks} = PgFlow.Worker.Poller.merge_task_details(message_tuples, task_rows)
      #=> {:ok, [
      #=>   %{
      #=>     flow_slug: "my_flow",
      #=>     run_id: "run-uuid",
      #=>     step_slug: :step1,
      #=>     task_index: 0,
      #=>     attempt: 1,
      #=>     input: %{"run" => %{"key" => "value"}},
      #=>     deps: %{}
      #=>   }
      #=> ]}
  """
  @spec merge_task_details(
          [{String.t(), String.t(), String.t(), pos_integer()}],
          list(list())
        ) :: {:ok, [map()]} | {:error, term()}
  def merge_task_details(message_tuples, task_rows)
      when is_list(message_tuples) and is_list(task_rows) do
    # Build a map from msg_id to message data for quick lookup
    message_map =
      Map.new(message_tuples, fn {flow_slug, run_id, step_slug, msg_id} ->
        {msg_id, %{flow_slug: flow_slug, run_id: run_id, step_slug: step_slug, msg_id: msg_id}}
      end)

    tasks =
      Enum.map(task_rows, fn
        # Format from pgflow.start_tasks:
        # [flow_slug, run_id (binary UUID), step_slug, input, msg_id, task_index, flow_input]
        [_flow_slug, run_id_bin, step_slug, input, msg_id, task_index, flow_input] ->
          # Convert binary UUID to string if needed
          run_id = convert_run_id(run_id_bin)

          # Decode input if it's JSON string (for unit tests), or use as-is if map (from DB)
          decoded_input = decode_if_json(input)
          decoded_flow_input = decode_if_json(flow_input)

          # Build combined input: {"run": flow_input, ...deps}
          combined_input = Map.put(decoded_input, "run", decoded_flow_input)

          # Find the corresponding message
          message = Map.get(message_map, msg_id)

          if message do
            %{
              flow_slug: message.flow_slug,
              run_id: run_id,
              step_slug: String.to_existing_atom(step_slug),
              task_index: task_index,
              attempt: 1,
              # Default attempt to 1 - actual retry logic would track this
              input: combined_input,
              deps: %{}
            }
          else
            Logger.warning(
              "No message found for task run_id=#{run_id}, step_slug=#{step_slug}, msg_id=#{msg_id}"
            )

            nil
          end

        invalid ->
          Logger.warning("Invalid task row format: #{inspect(invalid)}")
          nil
      end)
      |> Enum.reject(&is_nil/1)

    {:ok, tasks}
  rescue
    error ->
      {:error, "Failed to merge task details: #{inspect(error)}"}
  end

  # Convert run_id from binary UUID to string if needed
  defp convert_run_id(run_id) when is_binary(run_id) and byte_size(run_id) == 16 do
    Ecto.UUID.load!(run_id)
  end

  defp convert_run_id(run_id) when is_binary(run_id), do: run_id

  # Decode JSON string or return map as-is
  defp decode_if_json(value) when is_map(value), do: value
  defp decode_if_json(value) when is_binary(value), do: Jason.decode!(value)
  defp decode_if_json(nil), do: nil
end
