defmodule PgFlow.CronCompiler do
  @moduledoc """
  Compiles cron definitions into SQL statements.

  Delegates to `PgFlow.FlowCompiler` for base SQL generation (create_flow + add_step),
  then appends an UPDATE statement to set `flow_type = 'cron'` and a `cron.schedule()`
  call to register the recurring job with pg_cron.

  ## Example

      definition = MyApp.Crons.DailyReport.__pgflow_definition__()
      expression = MyApp.Crons.DailyReport.__pgflow_cron_expression__()
      input = MyApp.Crons.DailyReport.__pgflow_cron_input__()
      sql_statements = PgFlow.CronCompiler.compile(definition, expression, input)

      # Returns:
      # [
      #   "SELECT pgflow.create_flow('daily_report', 3, 1, 30)",
      #   "SELECT pgflow.add_step('daily_report', 'perform', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')",
      #   "UPDATE pgflow.flows SET flow_type = 'cron' WHERE flow_slug = 'daily_report'",
      #   "SELECT cron.schedule('pgflow:daily_report', '0 9 * * *', $$SELECT pgflow.start_flow('daily_report', '{\"report_type\":\"daily\"}'::jsonb)$$)"
      # ]

  """

  alias PgFlow.Flow.Definition
  alias PgFlow.FlowCompiler

  @doc """
  Compiles a cron definition into a list of SQL statements.

  Returns the same SQL as `FlowCompiler.compile/1` plus:
  - An UPDATE to mark the flow as a cron in the database
  - A `cron.schedule()` call to register with pg_cron
  """
  @spec compile(Definition.t(), String.t(), map()) :: [String.t()]
  def compile(%Definition{} = definition, expression, input \\ %{}) do
    base_sql = FlowCompiler.compile(definition)
    flow_slug = Atom.to_string(definition.slug)

    update_sql =
      "UPDATE pgflow.flows SET flow_type = 'cron' WHERE flow_slug = '#{escape(flow_slug)}'"

    schedule_sql = schedule_sql(flow_slug, expression, input)

    base_sql ++ [update_sql, schedule_sql]
  end

  @doc """
  Returns the SQL to unschedule a pg_cron job.

  This should be called in the down migration BEFORE deleting the flow/steps/queue.
  """
  @spec unschedule_sql(String.t()) :: String.t()
  def unschedule_sql(flow_slug) do
    "SELECT cron.unschedule('pgflow:#{escape(flow_slug)}')"
  end

  defp schedule_sql(flow_slug, expression, input) do
    input_json = Jason.encode!(input)

    inner_sql = "SELECT pgflow.start_flow('#{escape(flow_slug)}', '#{escape(input_json)}'::jsonb)"

    # Use tagged dollar quote ($pgflow$) to prevent any accidental $$ in JSON from breaking out
    "SELECT cron.schedule('pgflow:#{escape(flow_slug)}', '#{escape(expression)}', $pgflow$#{inner_sql}$pgflow$)"
  end

  defp escape(str) when is_binary(str) do
    String.replace(str, "'", "''")
  end
end
