defmodule PgFlow.Runs do
  @moduledoc """
  Typed operational reads for PgFlow runs, states, and tasks.

  Every function receives the consumer repository explicitly so callers can
  use their configured repository and tests can use an isolated repository.
  """

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias PgFlow.Queries.{Flows, Helpers}
  alias PgFlow.{RunHistoryCell, RunSummary}
  alias PgFlow.Schema.{Flow, Run, StepState, StepTask}
  alias PgFlow.Type.JSON

  @default_limit 50

  @type queue_location :: :all | :live | :archive

  @doc """
  Gets a persisted run by UUID.
  """
  @spec get(module(), Ecto.UUID.t()) :: {:ok, Run.t()} | {:error, :invalid_id | :not_found}
  def get(repo, run_id) do
    with {:ok, run_id} <- Helpers.cast_uuid(run_id),
         %Run{} = run <- repo.get(Run, run_id) do
      {:ok, run}
    else
      nil -> {:error, :not_found}
      {:error, :invalid_id} = error -> error
    end
  end

  @doc """
  Gets a persisted run with its step states preloaded in deterministic order.
  """
  @spec get_with_states(module(), Ecto.UUID.t()) ::
          {:ok, Run.t()} | {:error, :invalid_id | :not_found}
  def get_with_states(repo, run_id) do
    with {:ok, run_id} <- Helpers.cast_uuid(run_id),
         %Run{} = run <- repo.one(run_with_states_query(run_id)) do
      {:ok, run}
    else
      nil -> {:error, :not_found}
      {:error, :invalid_id} = error -> error
    end
  end

  @doc """
  Lists run summaries in newest-first order.

  Supported options are `:flow_slug`, `:status`, `:flow_type`, `:time_range`,
  `:started_after`, `:started_before`, `:input_contains`, `:cursor`, and `:limit`.
  Explicit `:started_after` and `:started_before` bounds are inclusive. No time
  filter is applied unless a time option is supplied.
  """
  @spec list(module(), keyword()) :: {:ok, [RunSummary.t()]} | {:error, :invalid_id}
  def list(repo, opts \\ []) do
    with {:ok, cursor} <- Helpers.optional_uuid(Keyword.get(opts, :cursor)) do
      summaries =
        opts
        |> run_summary_query(cursor)
        |> repo.all()
        |> Enum.map(&RunSummary.new/1)

      {:ok, summaries}
    end
  end

  @doc """
  Counts runs matching the supplied filters.

  Count ignores pagination options and accepts the same filter options as
  `list/2`.
  """
  @spec count(module(), keyword()) :: {:ok, non_neg_integer()}
  def count(repo, opts \\ []) do
    count =
      Run
      |> join(:inner, [run], flow in Flow, on: flow.flow_slug == run.flow_slug)
      |> apply_filters(opts)
      |> select([run], count(run.run_id))
      |> repo.one()

    {:ok, count}
  end

  @doc """
  Lists a run's persisted step states in execution order.
  """
  @spec list_step_states(module(), Ecto.UUID.t()) ::
          {:ok, [StepState.t()]} | {:error, :invalid_id}
  def list_step_states(repo, run_id) do
    with {:ok, run_id} <- Helpers.cast_uuid(run_id) do
      states =
        StepState
        |> where([state], state.run_id == ^run_id)
        |> order_by([state], asc_nulls_last: state.started_at, asc: state.step_slug)
        |> repo.all()

      {:ok, states}
    end
  end

  @doc """
  Lists every persisted task for a run in step and task-index order.
  """
  @spec list_run_tasks(module(), Ecto.UUID.t()) ::
          {:ok, [StepTask.t()]} | {:error, :invalid_id}
  def list_run_tasks(repo, run_id) do
    with {:ok, run_id} <- Helpers.cast_uuid(run_id) do
      tasks =
        StepTask
        |> where([task], task.run_id == ^run_id)
        |> order_by([task], asc: task.step_slug, asc: task.task_index)
        |> repo.all()

      {:ok, tasks}
    end
  end

  @doc """
  Lists all persisted tasks for a run step in task-index order.
  """
  @spec list_step_tasks(module(), Ecto.UUID.t(), String.t()) ::
          {:ok, [StepTask.t()]} | {:error, :invalid_id}
  def list_step_tasks(repo, run_id, step_slug) when is_binary(step_slug) do
    with {:ok, run_id} <- Helpers.cast_uuid(run_id) do
      tasks =
        StepTask
        |> where([task], task.run_id == ^run_id and task.step_slug == ^step_slug)
        |> order_by([task], asc: task.task_index)
        |> repo.all()

      {:ok, tasks}
    end
  end

  @doc """
  Gets one persisted task by run, step, and task index.
  """
  @spec get_step_task(module(), Ecto.UUID.t(), String.t(), non_neg_integer()) ::
          {:ok, StepTask.t()} | {:error, :invalid_id | :not_found}
  def get_step_task(repo, run_id, step_slug, task_index)
      when is_binary(step_slug) and is_integer(task_index) and task_index >= 0 do
    with {:ok, run_id} <- Helpers.cast_uuid(run_id),
         %StepTask{} = task <-
           repo.one(
             from(task in StepTask,
               where:
                 task.run_id == ^run_id and task.step_slug == ^step_slug and
                   task.task_index == ^task_index
             )
           ) do
      {:ok, task}
    else
      nil -> {:error, :not_found}
      {:error, :invalid_id} = error -> error
    end
  end

  @doc """
  Gets the adjacent run UUID in newest-first navigation order.

  `:next` selects the next older run and `:prev` selects the previous newer
  run. Timestamp ties are resolved by UUID.
  """
  @spec adjacent(module(), Ecto.UUID.t(), :next | :prev) ::
          {:ok, Ecto.UUID.t()} | {:error, :invalid_id | :invalid_direction | :not_found}
  def adjacent(repo, run_id, direction) when direction in [:next, :prev] do
    with {:ok, run_id} <- Helpers.cast_uuid(run_id),
         %Run{} = current <- repo.get(Run, run_id),
         adjacent_id when not is_nil(adjacent_id) <- repo.one(adjacent_query(current, direction)) do
      {:ok, adjacent_id}
    else
      nil -> {:error, :not_found}
      {:error, :invalid_id} = error -> error
    end
  end

  def adjacent(_repo, _run_id, _direction), do: {:error, :invalid_direction}

  @doc """
  Lists typed step-history cells for the most recent runs of a flow.

  The `:limit` option controls how many runs are included, not the number of
  returned cells.
  """
  @spec history(module(), String.t(), keyword()) :: {:ok, [RunHistoryCell.t()]}
  def history(repo, flow_slug, opts \\ []) when is_binary(flow_slug) do
    limit = Helpers.positive_limit(opts, @default_limit)

    recent_runs =
      from(run in Run,
        where: run.flow_slug == ^flow_slug,
        order_by: [desc: run.started_at, desc: run.run_id],
        limit: ^limit,
        select: %{run_id: run.run_id, started_at: run.started_at}
      )

    cells =
      from(run in subquery(recent_runs),
        left_join: state in StepState,
        on: state.run_id == run.run_id,
        order_by: [desc: run.started_at, desc: run.run_id, asc: state.step_slug],
        select: %{
          run_id: run.run_id,
          started_at: run.started_at,
          step_slug: state.step_slug,
          status: state.status,
          duration_ms:
            type(
              fragment(
                "CASE WHEN ? IS NULL THEN NULL ELSE EXTRACT(EPOCH FROM (COALESCE(?, ?, ?, NOW()) - ?)) * 1000 END",
                state.started_at,
                state.completed_at,
                state.skipped_at,
                state.failed_at,
                state.started_at
              ),
              :decimal
            )
        }
      )
      |> repo.all()
      |> Enum.map(&RunHistoryCell.new/1)

    {:ok, cells}
  end

  @doc """
  Counts durable PGMQ messages belonging to one or more run UUIDs.

  The count reads each message's embedded `run_id`, so it includes orphaned
  queue messages whose relational lifecycle rows no longer exist. The
  `:location` option accepts `:all` (the default), `:live`, or `:archive`.
  Missing queues and an empty run-ID list both return `{:ok, 0}`.

  The JSON payload predicate is diagnostic-oriented and requires an O(queue
  size) scan unless the consumer adds matching expression indexes to its PGMQ
  queue tables.
  """
  @spec count_queue_messages(
          module(),
          String.t() | atom(),
          Ecto.UUID.t() | [Ecto.UUID.t()],
          keyword()
        ) ::
          {:ok, non_neg_integer()}
          | {:error, :invalid_id | :invalid_flow_slug | :invalid_location | term()}
  def count_queue_messages(repo, flow_slug, run_ids, opts \\ []) do
    database_result(fn ->
      with {:ok, flow_slug} <- Helpers.cast_flow_slug(flow_slug),
           {:ok, run_ids} <- normalize_run_ids(run_ids),
           {:ok, prefixes} <- queue_prefixes(Keyword.get(opts, :location, :all)) do
        count_queue_messages_for_ids(repo, prefixes, flow_slug, run_ids)
      end
    end)
  end

  defp count_queue_messages_for_ids(_repo, _prefixes, _flow_slug, []), do: {:ok, 0}

  defp count_queue_messages_for_ids(repo, prefixes, flow_slug, run_ids) do
    with :ok <- Flows.validate_slug(repo, flow_slug) do
      count_messages_in_queues(repo, prefixes, flow_slug, run_ids)
    end
  end

  @doc """
  Makes every queued task for a run immediately visible to workers.

  Visibility is scoped by the exact run UUID embedded in the PGMQ payload so
  it also covers an orphaned queue message whose task row is missing.

  An absent run or queue is a successful no-op.
  """
  @spec make_available(module(), Ecto.UUID.t()) ::
          :ok | {:error, :invalid_id | :invalid_flow_slug | term()}
  def make_available(repo, run_id) do
    database_result(fn -> make_available_by_id(repo, run_id) end)
  end

  @doc """
  Deletes a run and all of its queued and persisted lifecycle data.

  The operation locks an existing run and performs all cleanup in one
  transaction. Queue cleanup uses the exact run UUID embedded in each PGMQ
  payload, including orphaned messages without a task row. An absent run or
  queue is a successful no-op.
  """
  @spec delete(module(), Ecto.UUID.t()) ::
          :ok | {:error, :invalid_id | :invalid_flow_slug | term()}
  def delete(repo, run_id) do
    database_result(fn ->
      with {:ok, run_id} <- Helpers.cast_uuid(run_id) do
        repo
        |> transact_delete(run_id)
        |> transaction_result()
      end
    end)
  end

  defp database_result(fun) do
    fun.()
  rescue
    error in [Postgrex.Error, DBConnection.ConnectionError] -> {:error, error}
  end

  defp make_available_by_id(repo, run_id) do
    with {:ok, run_id} <- Helpers.cast_uuid(run_id),
         %Run{} = run <- repo.get(Run, run_id) do
      make_run_available(repo, run)
    else
      nil -> :ok
      {:error, :invalid_id} = error -> error
    end
  end

  defp make_run_available(repo, %Run{flow_slug: flow_slug, run_id: run_id}) do
    with :ok <- Flows.validate_slug(repo, flow_slug),
         {:ok, queue_table} <- existing_queue_table(repo, "q", flow_slug) do
      update_queue_visibility(repo, queue_table, run_id)
    end
  end

  defp transact_delete(repo, run_id) do
    repo.transaction(fn ->
      case delete_run(repo, run_id) do
        :ok -> :ok
        {:error, reason} -> repo.rollback(reason)
      end
    end)
  end

  defp transaction_result({:ok, :ok}), do: :ok
  defp transaction_result({:error, reason}), do: {:error, reason}

  defp delete_run(repo, run_id) do
    case locked_run(repo, run_id) do
      %Run{} = run -> delete_locked_run(repo, run)
      nil -> :ok
    end
  end

  defp locked_run(repo, run_id) do
    Run
    |> where([run], run.run_id == ^run_id)
    |> lock("FOR UPDATE")
    |> repo.one()
  end

  defp delete_locked_run(repo, %Run{flow_slug: flow_slug, run_id: run_id}) do
    with :ok <- Flows.validate_slug(repo, flow_slug),
         :ok <- delete_queue_messages(repo, "q", flow_slug, run_id),
         :ok <- delete_queue_messages(repo, "a", flow_slug, run_id) do
      delete_relational_rows(repo, run_id)
    end
  end

  defp existing_queue_table(repo, prefix, flow_slug) do
    case SQL.query(
           repo,
           "SELECT quote_ident(pgmq.format_table_name($1::text, $2::text))",
           [flow_slug, prefix]
         ) do
      {:ok, %{rows: [[quoted_table_name]]}} ->
        find_queue_table(repo, ~s("pgmq".#{quoted_table_name}))

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp find_queue_table(repo, queue_table) do
    case SQL.query(repo, "SELECT to_regclass($1::text) IS NOT NULL", [queue_table]) do
      {:ok, %{rows: [[true]]}} -> {:ok, queue_table}
      {:ok, %{rows: [[false]]}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_run_ids(run_ids) when is_list(run_ids) do
    run_ids
    |> Enum.reduce_while({:ok, []}, fn run_id, {:ok, cast_ids} ->
      case Helpers.cast_uuid(run_id) do
        {:ok, cast_id} -> {:cont, {:ok, [cast_id | cast_ids]}}
        {:error, :invalid_id} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, cast_ids} -> {:ok, cast_ids |> Enum.uniq() |> Enum.reverse()}
      {:error, :invalid_id} = error -> error
    end
  end

  defp normalize_run_ids(run_id) do
    case Helpers.cast_uuid(run_id) do
      {:ok, cast_id} -> {:ok, [cast_id]}
      {:error, :invalid_id} = error -> error
    end
  end

  defp queue_prefixes(:all), do: {:ok, ["q", "a"]}
  defp queue_prefixes(:live), do: {:ok, ["q"]}
  defp queue_prefixes(:archive), do: {:ok, ["a"]}
  defp queue_prefixes(_location), do: {:error, :invalid_location}

  defp count_messages_in_queues(repo, prefixes, flow_slug, run_ids) do
    with {:ok, queue_tables} <- existing_queue_tables(repo, prefixes, flow_slug) do
      count_messages_in_tables(repo, queue_tables, run_ids)
    end
  end

  defp existing_queue_tables(repo, prefixes, flow_slug) do
    Enum.reduce_while(prefixes, {:ok, []}, fn prefix, {:ok, queue_tables} ->
      case existing_queue_table(repo, prefix, flow_slug) do
        {:ok, nil} -> {:cont, {:ok, queue_tables}}
        {:ok, queue_table} -> {:cont, {:ok, [queue_table | queue_tables]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, queue_tables} -> {:ok, Enum.reverse(queue_tables)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp count_messages_in_tables(_repo, [], _run_ids), do: {:ok, 0}

  defp count_messages_in_tables(repo, queue_tables, run_ids) do
    messages_sql =
      Enum.map_join(queue_tables, " UNION ALL ", fn queue_table ->
        "SELECT message FROM #{queue_table}"
      end)

    case SQL.query(
           repo,
           "SELECT count(*) FROM (#{messages_sql}) AS messages WHERE message->>'run_id' = ANY($1::text[])",
           [run_ids]
         ) do
      {:ok, %{rows: [[count]]}} -> {:ok, count}
      {:error, reason} -> {:error, reason}
    end
  end

  defp update_queue_visibility(_repo, nil, _run_id), do: :ok

  defp update_queue_visibility(repo, queue_table, run_id) do
    case SQL.query(
           repo,
           "UPDATE #{queue_table} SET vt = clock_timestamp() WHERE message->>'run_id' = $1::text",
           [run_id]
         ) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_queue_messages(repo, prefix, flow_slug, run_id) do
    with {:ok, queue_table} <- existing_queue_table(repo, prefix, flow_slug) do
      delete_queue_messages(repo, queue_table, run_id)
    end
  end

  defp delete_queue_messages(_repo, nil, _run_id), do: :ok

  # The UUID in the PGMQ payload is the durable run boundary. Scoping by
  # step-task message IDs would leave orphaned live or archived messages
  # behind when their relational task row is already missing.
  defp delete_queue_messages(repo, queue_table, run_id) do
    case SQL.query(repo, "DELETE FROM #{queue_table} WHERE message->>'run_id' = $1::text", [
           run_id
         ]) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_relational_rows(repo, run_id) do
    StepTask
    |> where([task], task.run_id == ^run_id)
    |> repo.delete_all()

    StepState
    |> where([state], state.run_id == ^run_id)
    |> repo.delete_all()

    Run
    |> where([run], run.run_id == ^run_id)
    |> repo.delete_all()

    :ok
  end

  defp run_with_states_query(run_id) do
    states_query =
      from(state in StepState, order_by: [asc_nulls_last: state.started_at, asc: state.step_slug])

    from(run in Run,
      where: run.run_id == ^run_id,
      preload: [step_states: ^states_query]
    )
  end

  defp run_summary_query(opts, cursor) do
    paginated_runs =
      Run
      |> join(:inner, [run], flow in Flow, on: flow.flow_slug == run.flow_slug)
      |> apply_filters(opts)
      |> apply_cursor(cursor)
      |> order_by([run], desc: run.started_at, desc: run.run_id)
      |> limit(^Helpers.positive_limit(opts, @default_limit))
      |> select([run, flow], %{
        run_id: run.run_id,
        flow_slug: run.flow_slug,
        flow_type: coalesce(flow.flow_type, "flow"),
        status: run.status,
        input: run.input,
        output: run.output,
        started_at: run.started_at,
        completed_at: run.completed_at,
        failed_at: run.failed_at
      })

    progress =
      from(state in StepState,
        where: state.run_id == parent_as(:run).run_id,
        select: %{
          total_steps: count(state.step_slug),
          completed_steps: filter(count(state.step_slug), state.status == "completed"),
          failed_steps: filter(count(state.step_slug), state.status == "failed"),
          skipped_steps: filter(count(state.step_slug), state.status == "skipped")
        }
      )

    from(run in subquery(paginated_runs),
      as: :run,
      left_lateral_join: progress in subquery(progress),
      on: true,
      order_by: [desc: run.started_at, desc: run.run_id],
      select: %{
        run_id: run.run_id,
        flow_slug: run.flow_slug,
        flow_type: run.flow_type,
        status: run.status,
        input: run.input,
        output: run.output,
        started_at: run.started_at,
        completed_at: run.completed_at,
        duration_ms:
          type(
            fragment(
              "EXTRACT(EPOCH FROM (COALESCE(?, ?, NOW()) - ?)) * 1000",
              run.completed_at,
              run.failed_at,
              run.started_at
            ),
            :decimal
          ),
        total_steps: coalesce(progress.total_steps, 0),
        completed_steps: coalesce(progress.completed_steps, 0),
        failed_steps: coalesce(progress.failed_steps, 0),
        skipped_steps: coalesce(progress.skipped_steps, 0),
        progress_percent:
          type(
            fragment(
              "CASE WHEN COALESCE(?, 0) > 0 THEN ROUND(((COALESCE(?, 0) + COALESCE(?, 0))::numeric / ?) * 100, 1) ELSE 0::numeric END",
              progress.total_steps,
              progress.completed_steps,
              progress.skipped_steps,
              progress.total_steps
            ),
            :decimal
          )
      }
    )
  end

  defp apply_filters(query, opts) do
    query
    |> filter_started_after(
      Keyword.get(opts, :started_after),
      Keyword.get(opts, :time_range)
    )
    |> filter_started_before(Keyword.get(opts, :started_before))
    |> filter_flow_slug(Keyword.get(opts, :flow_slug))
    |> filter_status(Keyword.get(opts, :status))
    |> filter_flow_type(Keyword.get(opts, :flow_type))
    |> filter_input(Keyword.get(opts, :input_contains))
  end

  defp filter_started_after(query, %DateTime{} = started_after, _time_range) do
    where(query, [run], run.started_at >= ^started_after)
  end

  defp filter_started_after(query, nil, nil), do: query

  defp filter_started_after(query, nil, time_range) do
    started_after = Helpers.time_range_start(time_range)
    where(query, [run], run.started_at > ^started_after)
  end

  defp filter_started_before(query, nil), do: query

  defp filter_started_before(query, %DateTime{} = started_before) do
    where(query, [run], run.started_at <= ^started_before)
  end

  defp filter_flow_slug(query, nil), do: query
  defp filter_flow_slug(query, flow_slug), do: where(query, [run], run.flow_slug == ^flow_slug)

  defp filter_status(query, nil), do: query

  defp filter_status(query, status) do
    where(query, [run], run.status == ^Helpers.status_to_string(status))
  end

  defp filter_flow_type(query, nil), do: query

  defp filter_flow_type(query, flow_type) do
    flow_type = Helpers.status_to_string(flow_type)
    where(query, [_run, flow], coalesce(flow.flow_type, "flow") == ^flow_type)
  end

  defp filter_input(query, nil), do: query

  defp filter_input(query, input_contains) do
    where(query, [run], fragment("? @> ?", run.input, type(^input_contains, JSON)))
  end

  defp apply_cursor(query, nil), do: query

  defp apply_cursor(query, cursor) do
    cursor_query =
      from(run in Run, where: run.run_id == ^cursor, select: {run.started_at, run.run_id})

    where(
      query,
      [run],
      fragment("(?, ?) < (?)", run.started_at, run.run_id, subquery(cursor_query))
    )
  end

  defp adjacent_query(%Run{run_id: run_id, started_at: started_at}, :next) do
    from(run in Run,
      where:
        fragment(
          "(?, ?) < (?, ?)",
          run.started_at,
          run.run_id,
          ^started_at,
          type(^run_id, :binary_id)
        ),
      order_by: [desc: run.started_at, desc: run.run_id],
      limit: 1,
      select: run.run_id
    )
  end

  defp adjacent_query(%Run{run_id: run_id, started_at: started_at}, :prev) do
    from(run in Run,
      where:
        fragment(
          "(?, ?) > (?, ?)",
          run.started_at,
          run.run_id,
          ^started_at,
          type(^run_id, :binary_id)
        ),
      order_by: [asc: run.started_at, asc: run.run_id],
      limit: 1,
      select: run.run_id
    )
  end
end
