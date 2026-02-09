defmodule PgFlow.JobCompiler do
  @moduledoc """
  Compiles job definitions into SQL statements.

  Delegates to `PgFlow.FlowCompiler` for base SQL generation (create_flow + add_step),
  then appends an UPDATE statement to set `flow_type = 'job'` on the flow record.

  ## Example

      definition = MyApp.Jobs.SendEmail.__pgflow_definition__()
      sql_statements = PgFlow.JobCompiler.compile(definition)

      # Returns:
      # [
      #   "SELECT pgflow.create_flow('send_email', 1, 1, 30)",
      #   "SELECT pgflow.add_step('send_email', 'send_email', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')",
      #   "UPDATE pgflow.flows SET flow_type = 'job' WHERE flow_slug = 'send_email'"
      # ]

  """

  alias PgFlow.Flow.Definition
  alias PgFlow.FlowCompiler

  @doc """
  Compiles a job definition into a list of SQL statements.

  Returns the same SQL as `FlowCompiler.compile/1` plus an additional UPDATE
  to mark the flow as a job in the database.
  """
  @spec compile(Definition.t()) :: [String.t()]
  def compile(%Definition{} = definition) do
    base_sql = FlowCompiler.compile(definition)
    flow_slug = Atom.to_string(definition.slug)

    update_sql =
      "UPDATE pgflow.flows SET flow_type = 'job' WHERE flow_slug = '#{escape(flow_slug)}'"

    base_sql ++ [update_sql]
  end

  @doc """
  Delegates to `FlowCompiler.has_cron?/1` to check if the module has cron configured.
  """
  @spec has_cron?(module()) :: boolean()
  defdelegate has_cron?(module), to: FlowCompiler

  @doc """
  Delegates to `FlowCompiler.cron_unschedule_sql/1` for generating unschedule SQL.
  """
  @spec cron_unschedule_sql(atom()) :: String.t()
  defdelegate cron_unschedule_sql(slug), to: FlowCompiler

  defp escape(str) when is_binary(str) do
    String.replace(str, "'", "''")
  end
end
