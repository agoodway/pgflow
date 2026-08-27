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
    base_sql = [flow_sql | step_sqls]

    case get_cron_expression(definition.module) do
      nil ->
        base_sql

      cron_expression ->
        cron_input = get_cron_input(definition.module)
        cron_sql = cron_schedule_sql(definition.slug, cron_expression, cron_input)
        base_sql ++ [cron_sql]
    end
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

    "SELECT pgflow.create_flow(#{sql_value(flow_slug)}, #{sql_value(max_attempts)}, #{sql_base_delay(base_delay)}, #{sql_value(timeout)})"
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
    # pgflow.add_step(flow_slug, step_slug, deps_slugs[], max_attempts, base_delay, timeout, start_delay, step_type
    #   [, required_input_pattern, forbidden_input_pattern, when_unmet, when_exhausted])
    #
    # The condition args are optional columns with SQL-side DEFAULTs (when_unmet
    # defaults to 'skip', when_exhausted to 'fail'). Postgres only allows mixed
    # positional/named calls with positional args first, so the 8 required
    # params stay positional and any condition opts the step actually set are
    # appended as named args - letting SQL own the defaults instead of us
    # duplicating them here.
    positional_args = [
      sql_value(Atom.to_string(flow_slug)),
      sql_value(Atom.to_string(step.slug)),
      sql_array(step.depends_on),
      sql_value(step.max_attempts),
      sql_base_delay(step.base_delay),
      sql_value(step.timeout),
      sql_value(step.start_delay),
      sql_value(Atom.to_string(step.step_type))
    ]

    named_args =
      [
        named_arg("required_input_pattern", step.if, &sql_json/1),
        named_arg("forbidden_input_pattern", step.if_not, &sql_json/1),
        named_arg("when_unmet", step.when_unmet, &sql_mode/1),
        named_arg("when_exhausted", step.when_exhausted, &sql_mode/1)
      ]
      |> Enum.reject(&is_nil/1)

    "SELECT pgflow.add_step(#{Enum.join(positional_args ++ named_args, ", ")})"
  end

  # SQL value encoding helpers

  @spec sql_value(nil | String.t() | integer() | atom()) :: String.t()
  defp sql_value(nil), do: "NULL"
  defp sql_value(0), do: "NULL"
  defp sql_value(value) when is_binary(value), do: "'#{escape(value)}'"
  defp sql_value(value) when is_integer(value), do: Integer.to_string(value)
  defp sql_value(value) when is_atom(value), do: sql_value(Atom.to_string(value))

  @spec sql_base_delay(nil | non_neg_integer()) :: String.t()
  defp sql_base_delay(nil), do: "NULL"
  defp sql_base_delay(value) when is_integer(value) and value >= 0, do: Integer.to_string(value)

  @spec sql_array([atom()]) :: String.t()
  defp sql_array([]), do: "ARRAY[]::text[]"

  defp sql_array(items) when is_list(items) do
    values = Enum.map_join(items, ", ", &"'#{Atom.to_string(&1)}'")
    "ARRAY[#{values}]::text[]"
  end

  # Builds a `name => encoded_value` named-arg fragment, or nil when the step
  # didn't set the value (so it's omitted and the SQL DEFAULT applies).
  @spec named_arg(String.t(), term(), (term() -> String.t())) :: String.t() | nil
  defp named_arg(_name, nil, _encode), do: nil
  defp named_arg(name, value, encode), do: "#{name} => #{encode.(value)}"

  # Only called via named_arg/3, which already filters out nil - if/if_not
  # are always maps here.
  defp sql_json(map) when is_map(map), do: "'#{escape(Jason.encode!(map))}'::jsonb"

  # when_unmet / when_exhausted are NOT NULL; never emit SQL NULL for modes.
  defp sql_mode(:skip_cascade), do: "'skip-cascade'"
  defp sql_mode(mode) when mode in [:fail, :skip], do: "'#{mode}'"

  @spec escape(String.t()) :: String.t()
  defp escape(str) when is_binary(str) do
    String.replace(str, "'", "''")
  end

  # Cron scheduling helpers

  @doc """
  Generates the SQL to schedule a flow/job with pg_cron.

  ## Parameters

    * `slug` - The flow/job slug atom
    * `expression` - The cron expression string (e.g., "0 * * * *")
    * `input` - The input map to pass to the flow/job

  ## Returns

    * A SQL string for scheduling the cron job

  """
  @spec cron_schedule_sql(atom(), String.t(), map()) :: String.t()
  def cron_schedule_sql(slug, expression, input) do
    flow_slug = Atom.to_string(slug)
    job_name = "pgflow:#{flow_slug}"
    json_input = Jason.encode!(input)

    "SELECT cron.schedule('#{escape(job_name)}', '#{escape(expression)}', $$SELECT pgflow.start_flow('#{escape(flow_slug)}', '#{escape(json_input)}'::jsonb)$$)"
  end

  @doc """
  Generates the SQL to unschedule a flow/job from pg_cron.

  ## Parameters

    * `slug` - The flow/job slug atom

  ## Returns

    * A SQL string for unscheduling the cron job

  """
  @spec cron_unschedule_sql(atom()) :: String.t()
  def cron_unschedule_sql(slug) do
    flow_slug = Atom.to_string(slug)
    job_name = "pgflow:#{flow_slug}"

    "SELECT cron.unschedule('#{escape(job_name)}')"
  end

  @doc """
  Checks if the flow module has a cron expression configured.

  ## Parameters

    * `module` - The flow module

  ## Returns

    * `true` if the module has a cron expression, `false` otherwise

  """
  @spec has_cron?(module()) :: boolean()
  def has_cron?(module) do
    get_cron_expression(module) != nil
  end

  # Private helpers for getting cron config from module

  defp get_cron_expression(module) do
    if function_exported?(module, :__pgflow_cron_expression__, 0) do
      module.__pgflow_cron_expression__()
    else
      nil
    end
  end

  defp get_cron_input(module) do
    if function_exported?(module, :__pgflow_cron_input__, 0) do
      module.__pgflow_cron_input__()
    else
      %{}
    end
  end
end
