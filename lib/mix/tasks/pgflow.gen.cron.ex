defmodule Mix.Tasks.Pgflow.Gen.Cron do
  @shortdoc "Generates an Ecto migration to compile a PgFlow cron job"

  @moduledoc """
  Generates an Ecto migration that registers a PgFlow cron job in the database.

  Cron jobs are single-step flows scheduled via pg_cron. This task generates migration
  SQL that creates the flow, adds the `:perform` step, marks it as `flow_type = 'cron'`,
  and registers the cron schedule with pg_cron.

  ## Usage

      mix pgflow.gen.cron MyApp.Crons.DailyReport
      mix pgflow.gen.cron MyApp.Crons.DailyReport --migrations-path priv/repo/migrations

  ## Options

    * `--migrations-path` - Path to the migrations directory.
      Defaults to `priv/repo/migrations`.

  ## Generated SQL

  The migration executes SQL statements that:
  1. Create the flow record and PGMQ queue
  2. Add the `:perform` step
  3. Mark the flow as `flow_type = 'cron'`
  4. Register the cron schedule with pg_cron via `cron.schedule()`

  The down migration:
  1. Unschedules the pg_cron job (FIRST, before deleting flow data)
  2. Deletes flow dependencies, steps, and flow record
  3. Drops the PGMQ queue

  ## Requirements

  The cron module must:
  1. Use `PgFlow.Cron`
  2. Define a valid cron with `@cron` and a `schedule` block
  3. Be compilable (no syntax errors)

  The database must have the pg_cron extension installed.

  """

  use Mix.Task

  alias PgFlow.Flow.Definition
  alias PgFlow.CronCompiler

  @impl Mix.Task
  def run(args) do
    {opts, args, _} =
      OptionParser.parse(args,
        switches: [migrations_path: :string],
        aliases: [p: :migrations_path]
      )

    case args do
      [module_string] ->
        generate_migration(module_string, opts)

      [] ->
        Mix.raise("""
        Missing cron module argument.

        Usage: mix pgflow.gen.cron MyApp.Crons.MyCron
        """)

      _ ->
        Mix.raise("""
        Too many arguments provided.

        Usage: mix pgflow.gen.cron MyApp.Crons.MyCron
        """)
    end
  end

  defp generate_migration(module_string, opts) do
    Mix.Task.run("compile", [])

    module = String.to_atom("Elixir.#{module_string}")

    unless Code.ensure_loaded?(module) do
      Mix.raise("""
      Module #{module_string} could not be loaded.

      Make sure the module exists and the project compiles successfully.
      """)
    end

    unless function_exported?(module, :__pgflow_definition__, 0) do
      Mix.raise("""
      Module #{module_string} is not a PgFlow cron job.

      The module must use PgFlow.Cron and define a schedule block.

      Example:
          defmodule #{module_string} do
            use PgFlow.Cron

            @cron queue: :my_cron, expression: "0 9 * * *"

            schedule do
              fn input, _ctx -> %{result: input} end
            end
          end
      """)
    end

    definition = module.__pgflow_definition__()

    unless definition.flow_type == :cron do
      Mix.raise("""
      Module #{module_string} is not a PgFlow cron job (flow_type is #{inspect(definition.flow_type)}).

      Use `mix pgflow.gen.cron` only for modules that use `PgFlow.Cron`.
      """)
    end

    case Definition.validate(definition) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Mix.raise("""
        Cron definition validation failed: #{reason}

        Please fix the cron definition and try again.
        """)
    end

    expression = module.__pgflow_cron_expression__()
    input = module.__pgflow_cron_input__()
    sql_statements = CronCompiler.compile(definition, expression, input)

    migrations_path = Keyword.get(opts, :migrations_path, "priv/repo/migrations")
    File.mkdir_p!(migrations_path)

    timestamp = generate_timestamp()

    cron_slug = Atom.to_string(definition.slug)
    migration_module = "Compile#{camelize(cron_slug)}"

    migration_content =
      generate_migration_content(migration_module, cron_slug, sql_statements)

    filename = "#{timestamp}_compile_#{cron_slug}.exs"
    filepath = Path.join(migrations_path, filename)

    File.write!(filepath, migration_content)

    Mix.shell().info("""
    Generated migration: #{filepath}

    Run the migration with:
        mix ecto.migrate

    This will:
      1. Create the '#{cron_slug}' cron as a flow in pgflow.flows
      2. Create the PGMQ queue 'pgmq.q_#{cron_slug}'
      3. Register the :perform step
      4. Mark flow_type = 'cron' for dashboard differentiation
      5. Schedule the cron job with pg_cron (expression: #{expression})

    After migration, pg_cron will trigger this job on schedule.
    Your worker processes the resulting tasks from the PGMQ queue.
    """)
  end

  defp generate_migration_content(migration_module, cron_slug, sql_statements) do
    # Escape the cron_slug at generation time for safe SQL embedding
    escaped_slug = escape_sql(cron_slug)

    # Use sigil strings (~s|...|) to avoid issues with JSON curly braces
    up_statements =
      sql_statements
      |> Enum.map_join("\n", &"    execute ~s|#{&1}|")

    """
    defmodule PgFlow.Repo.Migrations.#{migration_module} do
      @moduledoc \"\"\"
      Compiles the '#{cron_slug}' cron job definition into the database.

      This migration creates:
      - The flow record in pgflow.flows (with flow_type = 'cron')
      - The PGMQ queue for this cron job
      - The :perform step definition
      - The pg_cron schedule entry

      Generated by: mix pgflow.gen.cron
      \"\"\"
      use Ecto.Migration

      def up do
        # Pre-flight check: ensure required extensions are installed
        execute \"\"\"
        DO $pgflow$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgmq') THEN
            RAISE EXCEPTION 'pgmq extension is not installed. Install it with: CREATE EXTENSION pgmq;';
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
            RAISE EXCEPTION 'pg_cron extension is not installed. Install it with: CREATE EXTENSION pg_cron;';
          END IF;
        END $pgflow$;
        \"\"\"

    #{up_statements}
      end

      def down do
        # Safely unschedule - ignore if job doesn't exist (handles partial migration failures)
        execute \"\"\"
        DO $pgflow$
        BEGIN
          PERFORM cron.unschedule('pgflow:#{escaped_slug}');
        EXCEPTION WHEN OTHERS THEN
          RAISE NOTICE 'pg_cron job pgflow:#{escaped_slug} not found, skipping unschedule';
        END $pgflow$;
        \"\"\"
        execute ~s|DELETE FROM pgflow.deps WHERE flow_slug = '#{escaped_slug}'|
        execute ~s|DELETE FROM pgflow.steps WHERE flow_slug = '#{escaped_slug}'|
        execute ~s|DELETE FROM pgflow.flows WHERE flow_slug = '#{escaped_slug}'|
        execute ~s|SELECT pgmq.drop_queue('#{escaped_slug}')|
      end
    end
    """
  end

  defp escape_sql(str), do: String.replace(str, "'", "''")

  defp generate_timestamp do
    {{year, month, day}, {hour, minute, second}} = :calendar.universal_time()

    :io_lib.format("~4..0B~2..0B~2..0B~2..0B~2..0B~2..0B", [
      year,
      month,
      day,
      hour,
      minute,
      second
    ])
    |> IO.iodata_to_binary()
  end

  defp camelize(string) do
    string
    |> String.split("_")
    |> Enum.map_join(&String.capitalize/1)
  end
end
