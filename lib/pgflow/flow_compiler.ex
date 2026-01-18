defmodule PgFlow.FlowCompiler do
  @moduledoc """
  Compiles flow definitions into SQL statements.

  This module transforms `PgFlow.Flow.Definition` structs into executable SQL
  that registers flows and steps in the pgflow database schema. The generated
  SQL is intended to be run inside Ecto migrations.

  ## Generated SQL

  For each flow, the compiler generates:
  1. A `SELECT pgflow.create_flow(...)` call to create the flow record and PGMQ queue
  2. A `SELECT pgflow.add_step(...)` call for each step in the flow

  ## Example

      definition = MyApp.Flows.ArticleFlow.__pgflow_definition__()
      sql_statements = PgFlow.FlowCompiler.compile(definition)

      # Returns:
      # [
      #   "SELECT pgflow.create_flow('article_flow', 3, 5, 120)",
      #   "SELECT pgflow.add_step('article_flow', 'fetch_article', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')",
      #   ...
      # ]

  """

  alias PgFlow.Flow.{Definition, Step}

  @doc """
  Compiles a flow definition into a list of SQL statements.

  Returns a list of SQL strings that, when executed in order, will register
  the flow and all its steps in the database.

  ## Parameters

    * `definition` - A `PgFlow.Flow.Definition` struct

  ## Returns

    * A list of SQL statement strings

  ## Example

      iex> definition = %PgFlow.Flow.Definition{
      ...>   slug: :test_flow,
      ...>   module: TestFlow,
      ...>   opts: [max_attempts: 3, base_delay: 5, timeout: 60],
      ...>   steps: [
      ...>     %PgFlow.Flow.Step{slug: :step_a},
      ...>     %PgFlow.Flow.Step{slug: :step_b, depends_on: [:step_a]}
      ...>   ]
      ...> }
      iex> PgFlow.FlowCompiler.compile(definition)
      [
        "SELECT pgflow.create_flow('test_flow', 3, 5, 60)",
        "SELECT pgflow.add_step('test_flow', 'step_a', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')",
        "SELECT pgflow.add_step('test_flow', 'step_b', ARRAY['step_a']::text[], NULL, NULL, NULL, NULL, 'single')"
      ]

  """
  @spec compile(Definition.t()) :: [String.t()]
  def compile(%Definition{} = definition) do
    flow_sql = create_flow_sql(definition)
    step_sqls = Enum.map(definition.steps, &add_step_sql(definition.slug, &1))
    [flow_sql | step_sqls]
  end

  @doc """
  Generates the SQL to create a flow.

  ## Parameters

    * `definition` - A `PgFlow.Flow.Definition` struct

  ## Returns

    * A SQL string for creating the flow

  """
  @spec create_flow_sql(Definition.t()) :: String.t()
  def create_flow_sql(%Definition{slug: slug, opts: opts}) do
    flow_slug = Atom.to_string(slug)
    max_attempts = Keyword.get(opts, :max_attempts, 3)
    base_delay = Keyword.get(opts, :base_delay, 1)
    timeout = Keyword.get(opts, :timeout, 60)

    "SELECT pgflow.create_flow(#{sql_value(flow_slug)}, #{sql_value(max_attempts)}, #{sql_value(base_delay)}, #{sql_value(timeout)})"
  end

  @doc """
  Generates the SQL to add a step to a flow.

  ## Parameters

    * `flow_slug` - The flow slug atom
    * `step` - A `PgFlow.Flow.Step` struct

  ## Returns

    * A SQL string for adding the step

  """
  @spec add_step_sql(atom(), Step.t()) :: String.t()
  def add_step_sql(flow_slug, %Step{} = step) do
    # pgflow.add_step(flow_slug, step_slug, deps_slugs[], max_attempts, base_delay, timeout, start_delay, step_type)
    args = [
      sql_value(Atom.to_string(flow_slug)),
      sql_value(Atom.to_string(step.slug)),
      sql_array(step.depends_on),
      sql_value(step.max_attempts),
      sql_value(step.base_delay),
      sql_value(step.timeout),
      sql_value(step.start_delay),
      sql_value(Atom.to_string(step.step_type))
    ]

    "SELECT pgflow.add_step(#{Enum.join(args, ", ")})"
  end

  # SQL value encoding helpers

  @spec sql_value(nil | String.t() | integer() | atom()) :: String.t()
  defp sql_value(nil), do: "NULL"
  defp sql_value(0), do: "NULL"
  defp sql_value(value) when is_binary(value), do: "'#{escape(value)}'"
  defp sql_value(value) when is_integer(value), do: Integer.to_string(value)
  defp sql_value(value) when is_atom(value), do: sql_value(Atom.to_string(value))

  @spec sql_array([atom()]) :: String.t()
  defp sql_array([]), do: "ARRAY[]::text[]"

  defp sql_array(items) when is_list(items) do
    values = Enum.map_join(items, ", ", &"'#{Atom.to_string(&1)}'")
    "ARRAY[#{values}]::text[]"
  end

  @spec escape(String.t()) :: String.t()
  defp escape(str) when is_binary(str) do
    String.replace(str, "'", "''")
  end
end
