defmodule PgFlow.Definitions do
  @moduledoc """
  Typed operational reads for persisted PgFlow definitions and schedules.

  Definition statistics use an explicit 24-hour window. List queries page the
  stored definitions before aggregating runs and steps for only that page.

  Calculating `next_run_at` for a non-UTC `cron.timezone` requires the host
  application to configure an IANA-compatible `Calendar.TimeZoneDatabase`.
  When the database timezone cannot be resolved, `next_run_at` is `nil`.
  """

  import Ecto.Query

  alias Crontab.CronExpression
  alias Crontab.CronExpression.Parser, as: CronParser
  alias Crontab.Scheduler, as: CronScheduler
  alias Ecto.Adapters.SQL
  alias PgFlow.{CronSummary, DefinitionSummary}
  alias PgFlow.Queries.Helpers
  alias PgFlow.Schema.{Dep, Flow, Run, Step}

  @default_limit 50

  @exact_cron_sql """
  SELECT flow.flow_slug, COALESCE(flow.flow_type, 'flow'), job.schedule,
         COALESCE(job.active, true), flow.opt_max_attempts,
         flow.opt_base_delay, flow.opt_timeout
  FROM pgflow.flows AS flow
  INNER JOIN cron.job AS job ON job.jobname = 'pgflow:' || flow.flow_slug
  WHERE ($1::text IS NULL OR flow.flow_slug = $1)
  ORDER BY flow.flow_slug
  LIMIT $2
  """

  @after_cron_sql """
  SELECT flow.flow_slug, COALESCE(flow.flow_type, 'flow'), job.schedule,
         COALESCE(job.active, true), flow.opt_max_attempts,
         flow.opt_base_delay, flow.opt_timeout
  FROM pgflow.flows AS flow
  INNER JOIN cron.job AS job ON job.jobname = 'pgflow:' || flow.flow_slug
  WHERE ($1::text IS NULL OR flow.flow_slug > $1)
  ORDER BY flow.flow_slug
  LIMIT $2
  """

  @cron_timezone_sql "SELECT current_setting('cron.timezone', true)"

  @doc """
  Gets a stored flow definition with its 24-hour operational statistics.
  """
  @spec get_flow(module(), String.t()) ::
          {:ok, DefinitionSummary.t()} | {:error, :not_found}
  def get_flow(repo, flow_slug) when is_binary(flow_slug) do
    get_definition(repo, flow_slug, "flow")
  end

  @doc """
  Lists stored flow definitions in deterministic slug order.

  Supported options are `:cursor` and `:limit`.
  """
  @spec list_flows(module(), keyword()) :: {:ok, [DefinitionSummary.t()]}
  def list_flows(repo, opts \\ []), do: list_definitions(repo, "flow", opts)

  @doc """
  Counts stored flow definitions.
  """
  @spec count_flows(module()) :: {:ok, non_neg_integer()}
  def count_flows(repo), do: count_definitions(repo, "flow")

  @doc """
  Gets a step using its complete composite key.
  """
  @spec get_step(module(), String.t(), String.t()) :: {:ok, Step.t()} | {:error, :not_found}
  def get_step(repo, flow_slug, step_slug)
      when is_binary(flow_slug) and is_binary(step_slug) do
    case repo.one(
           from(step in Step,
             where: step.flow_slug == ^flow_slug and step.step_slug == ^step_slug
           )
         ) do
      %Step{} = step -> {:ok, step}
      nil -> {:error, :not_found}
    end
  end

  @doc """
  Lists every stored step for a flow in execution order.
  """
  @spec list_steps(module(), String.t()) :: {:ok, [Step.t()]}
  def list_steps(repo, flow_slug) when is_binary(flow_slug) do
    steps =
      Step
      |> where([step], step.flow_slug == ^flow_slug)
      |> order_by([step], asc: step.step_index, asc: step.step_slug)
      |> repo.all()

    {:ok, steps}
  end

  @doc """
  Lists dependency rows for a flow using the complete composite key.
  """
  @spec list_deps(module(), String.t()) :: {:ok, [Dep.t()]}
  def list_deps(repo, flow_slug) when is_binary(flow_slug) do
    deps =
      Dep
      |> where([dep], dep.flow_slug == ^flow_slug)
      |> order_by([dep], asc: dep.step_slug, asc: dep.dep_slug)
      |> repo.all()

    {:ok, deps}
  end

  @doc """
  Gets a stored job definition with its 24-hour operational statistics.
  """
  @spec get_job(module(), String.t()) :: {:ok, DefinitionSummary.t()} | {:error, :not_found}
  def get_job(repo, flow_slug) when is_binary(flow_slug) do
    get_definition(repo, flow_slug, "job")
  end

  @doc """
  Lists stored job definitions in deterministic slug order.

  Supported options are `:cursor` and `:limit`.
  """
  @spec list_jobs(module(), keyword()) :: {:ok, [DefinitionSummary.t()]}
  def list_jobs(repo, opts \\ []), do: list_definitions(repo, "job", opts)

  @doc """
  Counts stored job definitions.
  """
  @spec count_jobs(module()) :: {:ok, non_neg_integer()}
  def count_jobs(repo), do: count_definitions(repo, "job")

  @doc """
  Gets a scheduled definition with its stored schedule and operational data.
  """
  @spec get_cron(module(), String.t()) :: {:ok, CronSummary.t()} | {:error, :not_found | term()}
  def get_cron(repo, flow_slug) when is_binary(flow_slug) do
    with {:ok, records} <- cron_records(repo, flow_slug, 1, :exact),
         [record] <- records do
      {:ok, [record] |> summarize_crons(repo) |> hd()}
    else
      [] -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Lists scheduled definitions in deterministic slug order.

  Supported options are `:cursor` and `:limit`.
  """
  @spec list_crons(module(), keyword()) :: {:ok, [CronSummary.t()]} | {:error, term()}
  def list_crons(repo, opts \\ []) do
    cursor = Keyword.get(opts, :cursor)
    limit = Helpers.positive_limit(opts, @default_limit)

    with {:ok, records} <- cron_records(repo, cursor, limit, :after) do
      {:ok, summarize_crons(records, repo)}
    end
  end

  @doc """
  Counts scheduled PgFlow definitions.
  """
  @spec count_crons(module()) :: {:ok, non_neg_integer()} | {:error, term()}
  def count_crons(repo) do
    case SQL.query(
           repo,
           """
           SELECT COUNT(*)
           FROM pgflow.flows AS flow
           INNER JOIN cron.job AS job ON job.jobname = 'pgflow:' || flow.flow_slug
           """,
           []
         ) do
      {:ok, %{rows: [[count]]}} -> {:ok, count}
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_definition(repo, flow_slug, flow_type) do
    query =
      Flow
      |> where([flow], flow.flow_slug == ^flow_slug)
      |> where([flow], fragment("COALESCE(?, 'flow') = ?", flow.flow_type, ^flow_type))

    case repo.one(query) do
      %Flow{} = flow -> {:ok, [flow] |> summarize_definitions(repo) |> hd()}
      nil -> {:error, :not_found}
    end
  end

  defp list_definitions(repo, flow_type, opts) do
    cursor = Keyword.get(opts, :cursor)
    limit = Helpers.positive_limit(opts, @default_limit)

    definitions =
      Flow
      |> where([flow], fragment("COALESCE(?, 'flow') = ?", flow.flow_type, ^flow_type))
      |> after_slug(cursor)
      |> order_by([flow], asc: flow.flow_slug)
      |> limit(^limit)
      |> repo.all()

    {:ok, summarize_definitions(definitions, repo)}
  end

  defp count_definitions(repo, flow_type) do
    count =
      Flow
      |> where([flow], fragment("COALESCE(?, 'flow') = ?", flow.flow_type, ^flow_type))
      |> repo.aggregate(:count, :flow_slug)

    {:ok, count}
  end

  defp after_slug(query, nil), do: query

  defp after_slug(query, cursor) when is_binary(cursor),
    do: where(query, [flow], flow.flow_slug > ^cursor)

  defp summarize_definitions([], _repo), do: []

  defp summarize_definitions(definitions, repo) do
    slugs = Enum.map(definitions, & &1.flow_slug)
    statistics = statistics_by_slug(repo, slugs)
    step_counts = step_counts_by_slug(repo, slugs)

    Enum.map(definitions, fn flow ->
      flow
      |> definition_attributes(
        Map.get(statistics, flow.flow_slug, empty_statistics()),
        Map.get(step_counts, flow.flow_slug, 0)
      )
      |> DefinitionSummary.new()
    end)
  end

  defp definition_attributes(flow, statistics, step_count) do
    statistics
    |> Map.put(:flow_slug, flow.flow_slug)
    |> Map.put(:flow_type, flow.flow_type || "flow")
    |> Map.put(:opt_max_attempts, flow.opt_max_attempts)
    |> Map.put(:opt_base_delay, flow.opt_base_delay)
    |> Map.put(:opt_timeout, flow.opt_timeout)
    |> Map.put(:step_count, step_count)
  end

  defp statistics_by_slug(repo, slugs) do
    started_after = Helpers.time_range_start(:last_24h)

    Run
    |> where([run], run.flow_slug in ^slugs and run.started_at > ^started_after)
    |> group_by([run], run.flow_slug)
    |> select([run], %{
      flow_slug: run.flow_slug,
      total_runs_24h: count(run.run_id),
      completed_runs_24h: fragment("COUNT(*) FILTER (WHERE ? = 'completed')", run.status),
      failed_runs_24h: fragment("COUNT(*) FILTER (WHERE ? = 'failed')", run.status),
      avg_duration_ms:
        type(
          fragment(
            "COALESCE(AVG(EXTRACT(EPOCH FROM (? - ?)) * 1000) FILTER (WHERE ? = 'completed'), 0)",
            run.completed_at,
            run.started_at,
            run.status
          ),
          :decimal
        ),
      p95_duration_ms:
        type(
          fragment(
            "COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (? - ?)) * 1000) FILTER (WHERE ? = 'completed'), 0)",
            run.completed_at,
            run.started_at,
            run.status
          ),
          :float
        )
    })
    |> repo.all()
    |> Map.new(fn %{flow_slug: flow_slug} = statistics ->
      statistics = Map.put(statistics, :success_rate_24h, success_rate(statistics))
      {flow_slug, Map.delete(statistics, :flow_slug)}
    end)
  end

  defp step_counts_by_slug(repo, slugs) do
    Step
    |> where([step], step.flow_slug in ^slugs)
    |> group_by([step], step.flow_slug)
    |> select([step], {step.flow_slug, count(step.step_slug)})
    |> repo.all()
    |> Map.new()
  end

  defp success_rate(%{total_runs_24h: 0}), do: Decimal.new(0)

  defp success_rate(%{total_runs_24h: total, completed_runs_24h: completed}) do
    completed
    |> Decimal.new()
    |> Decimal.div(Decimal.new(total))
    |> Decimal.mult(Decimal.new(100))
    |> Decimal.round(1)
  end

  defp empty_statistics do
    %{
      total_runs_24h: 0,
      completed_runs_24h: 0,
      failed_runs_24h: 0,
      success_rate_24h: Decimal.new(0),
      avg_duration_ms: Decimal.new(0),
      p95_duration_ms: 0.0
    }
  end

  defp cron_records(repo, slug, limit, :exact),
    do: repo |> SQL.query(@exact_cron_sql, [slug, limit]) |> cron_record_result()

  defp cron_records(repo, slug, limit, :after),
    do: repo |> SQL.query(@after_cron_sql, [slug, limit]) |> cron_record_result()

  defp cron_record_result({:ok, %{rows: rows}}), do: {:ok, Enum.map(rows, &cron_record/1)}
  defp cron_record_result({:error, reason}), do: {:error, reason}

  defp cron_record([flow_slug, flow_type, expression, active, attempts, delay, timeout]) do
    %{
      flow_slug: flow_slug,
      flow_type: flow_type,
      cron_expression: expression,
      is_active: active,
      opt_max_attempts: attempts,
      opt_base_delay: delay,
      opt_timeout: timeout
    }
  end

  defp summarize_crons([], _repo), do: []

  defp summarize_crons(records, repo) do
    slugs = Enum.map(records, & &1.flow_slug)
    statistics = statistics_by_slug(repo, slugs)
    last_runs = last_runs_by_slug(repo, slugs)
    time_zone = cron_time_zone(repo)

    Enum.map(records, fn record ->
      record
      |> Map.merge(Map.get(statistics, record.flow_slug, empty_statistics()))
      |> Map.merge(
        Map.get(last_runs, record.flow_slug, %{last_run_at: nil, last_run_status: nil})
      )
      |> Map.put(:next_run_at, calculate_next_run(record.cron_expression, time_zone))
      |> CronSummary.new()
    end)
  end

  defp last_runs_by_slug(repo, slugs) do
    Run
    |> where([run], run.flow_slug in ^slugs)
    |> distinct([run], run.flow_slug)
    |> order_by([run], asc: run.flow_slug, desc: run.started_at, desc: run.run_id)
    |> select([run], %{
      flow_slug: run.flow_slug,
      last_run_at: run.completed_at,
      last_run_status: run.status
    })
    |> repo.all()
    |> Map.new(fn %{flow_slug: flow_slug} = run -> {flow_slug, Map.delete(run, :flow_slug)} end)
  end

  defp cron_time_zone(repo) do
    case SQL.query(repo, @cron_timezone_sql, []) do
      {:ok, %{rows: [[time_zone]]}} when is_binary(time_zone) -> normalize_time_zone(time_zone)
      _error -> nil
    end
  end

  defp normalize_time_zone(time_zone) when time_zone in ["GMT", "UTC"], do: "Etc/UTC"
  defp normalize_time_zone(time_zone), do: time_zone

  defp calculate_next_run(nil, _time_zone), do: nil
  defp calculate_next_run(_expression, nil), do: nil

  defp calculate_next_run(expression, time_zone) do
    time_zone_database = Calendar.get_time_zone_database()

    with {:ok, %CronExpression{reboot: false} = cron_expression} <- CronParser.parse(expression),
         {:ok, local_now} <-
           DateTime.shift_zone(DateTime.utc_now(), time_zone, time_zone_database),
         {:ok, local_next_run} <- CronScheduler.get_next_run_date(cron_expression, local_now),
         {:ok, next_run} <-
           DateTime.shift_zone(local_next_run, "Etc/UTC", time_zone_database) do
      next_run
    else
      _error -> nil
    end
  end
end
