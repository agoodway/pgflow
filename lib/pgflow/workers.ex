defmodule PgFlow.Workers do
  @moduledoc """
  Typed operational reads and cleanup for persisted PgFlow workers.

  Every function receives the consumer repository explicitly. Worker summaries
  combine a persisted worker with its current health and queue load, while task
  reads return complete `PgFlow.Schema.StepTask` records.
  """

  import Ecto.Query

  alias PgFlow.Queries.Helpers
  alias PgFlow.Schema.{Flow, StepTask, Worker}
  alias PgFlow.WorkerSummary

  @default_limit 50

  @doc """
  Gets a worker with its calculated health and queue load.
  """
  @spec get(module(), Ecto.UUID.t()) ::
          {:ok, WorkerSummary.t()} | {:error, :invalid_id | :not_found}
  def get(repo, worker_id) do
    with {:ok, worker_id} <- Helpers.cast_uuid(worker_id),
         %WorkerSummary{} = worker <-
           worker_id
           |> worker_detail_query()
           |> repo.one()
           |> to_worker_summary() do
      {:ok, worker}
    else
      nil -> {:error, :not_found}
      {:error, :invalid_id} = error -> error
    end
  end

  @doc """
  Lists worker summaries in newest-heartbeat-first order.

  Supported options are `:flow_slug`, `:health_status`, `:cursor`, and `:limit`.
  Health is `healthy` within 30 seconds, `stale` through 60 seconds, and `dead`
  after 60 seconds. Stopped or deprecated workers are always dead. Queue-load
  aggregates are calculated only after pagination.
  """
  @spec list(module(), keyword()) :: {:ok, [WorkerSummary.t()]} | {:error, :invalid_id}
  def list(repo, opts \\ []) do
    with {:ok, cursor} <- Helpers.optional_uuid(Keyword.get(opts, :cursor)) do
      workers =
        opts
        |> worker_summary_query(cursor)
        |> repo.all()
        |> Enum.map(&WorkerSummary.new/1)

      {:ok, workers}
    end
  end

  @doc """
  Counts persisted workers matching the flow and health filters.

  Pagination options do not affect the count.
  """
  @spec count(module(), keyword()) :: {:ok, non_neg_integer()}
  def count(repo, opts \\ []) do
    count =
      Worker
      |> apply_filters(opts)
      |> select([worker], count(worker.worker_id))
      |> repo.one()

    {:ok, count}
  end

  @doc """
  Lists complete persisted task rows last owned by a worker.
  """
  @spec list_tasks(module(), Ecto.UUID.t(), keyword()) ::
          {:ok, [StepTask.t()]} | {:error, :invalid_id}
  def list_tasks(repo, worker_id, opts \\ []) do
    with {:ok, worker_id} <- Helpers.cast_uuid(worker_id) do
      tasks =
        StepTask
        |> where([task], task.last_worker_id == ^worker_id)
        |> order_by(
          [task],
          desc: fragment("COALESCE(?, ?, ?)", task.completed_at, task.started_at, task.queued_at),
          desc: task.run_id,
          asc: task.step_slug,
          asc: task.task_index
        )
        |> limit(^Helpers.positive_limit(opts, @default_limit))
        |> repo.all()

      {:ok, tasks}
    end
  end

  @doc """
  Gets the adjacent worker UUID in newest-heartbeat-first navigation order.

  `:next` selects the next older worker and `:prev` selects the previous newer
  worker. Timestamp ties are resolved by UUID.
  """
  @spec adjacent(module(), Ecto.UUID.t(), :next | :prev) ::
          {:ok, Ecto.UUID.t()} | {:error, :invalid_id | :invalid_direction | :not_found}
  def adjacent(repo, worker_id, direction) when direction in [:next, :prev] do
    with {:ok, worker_id} <- Helpers.cast_uuid(worker_id),
         %Worker{} = current <- repo.get(Worker, worker_id),
         adjacent_id when not is_nil(adjacent_id) <- repo.one(adjacent_query(current, direction)) do
      {:ok, adjacent_id}
    else
      nil -> {:error, :not_found}
      {:error, :invalid_id} = error -> error
    end
  end

  def adjacent(_repo, _worker_id, _direction), do: {:error, :invalid_direction}

  @doc """
  Deletes a persisted worker without deleting its historical task or run rows.

  PostgreSQL clears `StepTask.last_worker_id` through the table's
  `ON DELETE SET NULL` foreign key, so the task remains without worker
  attribution.

  Deleting an absent worker is a successful no-op.
  """
  @spec delete(module(), Ecto.UUID.t()) :: :ok | {:error, :invalid_id | term()}
  def delete(repo, worker_id) do
    database_result(fn ->
      with {:ok, worker_id} <- Helpers.cast_uuid(worker_id) do
        Worker
        |> where([worker], worker.worker_id == ^worker_id)
        |> repo.delete_all()

        :ok
      end
    end)
  end

  defp database_result(fun) do
    fun.()
  rescue
    error in [Postgrex.Error, DBConnection.ConnectionError] -> {:error, error}
  end

  defp worker_detail_query(worker_id) do
    Worker
    |> where([worker], worker.worker_id == ^worker_id)
    |> worker_summary_query(1)
  end

  defp worker_summary_query(opts, cursor) when is_list(opts) do
    Worker
    |> apply_filters(opts)
    |> apply_cursor(cursor)
    |> worker_summary_query(Helpers.positive_limit(opts, @default_limit))
  end

  defp worker_summary_query(query, limit) do
    paginated_workers =
      query
      |> join(:left, [worker], flow in Flow, on: flow.flow_slug == worker.queue_name)
      |> order_by([worker], desc: worker.last_heartbeat_at, desc: worker.worker_id)
      |> limit(^limit)
      |> select([worker, flow], %{
        worker_id: worker.worker_id,
        flow_slug: worker.queue_name,
        flow_type: coalesce(flow.flow_type, "flow"),
        last_heartbeat_at: worker.last_heartbeat_at,
        health_status:
          fragment(
            "CASE WHEN ? IS NOT NULL OR ? IS NOT NULL THEN 'dead' WHEN ? > NOW() - INTERVAL '30 seconds' THEN 'healthy' WHEN ? > NOW() - INTERVAL '60 seconds' THEN 'stale' ELSE 'dead' END",
            worker.stopped_at,
            worker.deprecated_at,
            worker.last_heartbeat_at,
            worker.last_heartbeat_at
          )
      })

    load =
      from(task in StepTask,
        where: task.flow_slug == parent_as(:worker).flow_slug,
        select: %{
          active_tasks: filter(count(task.run_id), task.status == "started"),
          completed_tasks_24h:
            filter(
              count(task.run_id),
              task.status == "completed" and
                fragment("? > NOW() - INTERVAL '24 hours'", task.completed_at)
            )
        }
      )

    from(worker in subquery(paginated_workers),
      as: :worker,
      left_lateral_join: load in subquery(load),
      on: true,
      order_by: [desc: worker.last_heartbeat_at, desc: worker.worker_id],
      select: %{
        worker_id: worker.worker_id,
        flow_slug: worker.flow_slug,
        flow_type: worker.flow_type,
        last_heartbeat_at: worker.last_heartbeat_at,
        health_status: worker.health_status,
        active_tasks: coalesce(load.active_tasks, 0),
        completed_tasks_24h: coalesce(load.completed_tasks_24h, 0)
      }
    )
  end

  defp to_worker_summary(nil), do: nil
  defp to_worker_summary(attributes), do: WorkerSummary.new(attributes)

  defp apply_filters(query, opts) do
    query
    |> filter_flow_slug(Keyword.get(opts, :flow_slug))
    |> filter_health_status(Keyword.get(opts, :health_status))
  end

  defp filter_flow_slug(query, nil), do: query

  defp filter_flow_slug(query, flow_slug),
    do: where(query, [worker], worker.queue_name == ^flow_slug)

  defp filter_health_status(query, nil), do: query

  defp filter_health_status(query, health_status) do
    health_status = Helpers.health_status_to_string(health_status)

    where(
      query,
      [worker],
      fragment(
        "CASE WHEN ? IS NOT NULL OR ? IS NOT NULL THEN 'dead' WHEN ? > NOW() - INTERVAL '30 seconds' THEN 'healthy' WHEN ? > NOW() - INTERVAL '60 seconds' THEN 'stale' ELSE 'dead' END",
        worker.stopped_at,
        worker.deprecated_at,
        worker.last_heartbeat_at,
        worker.last_heartbeat_at
      ) == ^health_status
    )
  end

  defp apply_cursor(query, nil), do: query

  defp apply_cursor(query, cursor) do
    cursor_heartbeat_query =
      from(worker in Worker,
        where: worker.worker_id == ^cursor,
        select: worker.last_heartbeat_at
      )

    where(
      query,
      [worker],
      worker.last_heartbeat_at < subquery(cursor_heartbeat_query) or
        (worker.last_heartbeat_at == subquery(cursor_heartbeat_query) and
           worker.worker_id < type(^cursor, :binary_id))
    )
  end

  defp adjacent_query(
         %Worker{worker_id: worker_id, last_heartbeat_at: last_heartbeat_at},
         :next
       ) do
    from(worker in Worker,
      where:
        fragment(
          "(?, ?) < (?, ?)",
          worker.last_heartbeat_at,
          worker.worker_id,
          ^last_heartbeat_at,
          type(^worker_id, :binary_id)
        ),
      order_by: [desc: worker.last_heartbeat_at, desc: worker.worker_id],
      limit: 1,
      select: worker.worker_id
    )
  end

  defp adjacent_query(
         %Worker{worker_id: worker_id, last_heartbeat_at: last_heartbeat_at},
         :prev
       ) do
    from(worker in Worker,
      where:
        fragment(
          "(?, ?) > (?, ?)",
          worker.last_heartbeat_at,
          worker.worker_id,
          ^last_heartbeat_at,
          type(^worker_id, :binary_id)
        ),
      order_by: [asc: worker.last_heartbeat_at, asc: worker.worker_id],
      limit: 1,
      select: worker.worker_id
    )
  end
end
