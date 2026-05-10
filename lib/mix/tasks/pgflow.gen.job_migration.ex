defmodule Mix.Tasks.Pgflow.Gen.JobMigration do
  @shortdoc "Generates an Ecto migration to compile a PgFlow job"

  @moduledoc """
  Generates an Ecto migration that registers a PgFlow job in the database.

  Jobs are single-step flows under the hood. This task generates migration SQL
  that creates the flow, adds the step, and marks it as `flow_type = 'job'`.

  ## Usage

      mix pgflow.gen.job_migration MyApp.Jobs.SendEmail
      mix pgflow.gen.job_migration MyApp.Jobs.SendEmail --migrations-path priv/repo/migrations

  ## Options

    * `--migrations-path` - Path to the migrations directory.
      Defaults to `priv/repo/migrations`.

  ## Generated SQL

  The migration executes SQL statements that:
  1. Create the flow record and PGMQ queue
  2. Add the step (slug defaults to the job queue name, or an explicit name from `perform :name do`)
  3. Mark the flow as `flow_type = 'job'`

  Example generated migration:

      defmodule MyApp.Repo.Migrations.CompileSendEmail do
        use Ecto.Migration

        def up do
          execute "SELECT pgflow.create_flow('send_email', 3, 5, 60)"
          execute "SELECT pgflow.add_step('send_email', 'send_email', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')"
          execute "UPDATE pgflow.flows SET flow_type = 'job' WHERE flow_slug = 'send_email'"
        end

        def down do
          execute "DELETE FROM pgflow.deps WHERE flow_slug = 'send_email'"
          execute "DELETE FROM pgflow.steps WHERE flow_slug = 'send_email'"
          execute "DELETE FROM pgflow.flows WHERE flow_slug = 'send_email'"
          execute "SELECT pgmq.drop_queue('send_email')"
        end
      end

  ## Requirements

  The job module must:
  1. Use `PgFlow.Job`
  2. Define a valid job with `@job` and a `perform` block
  3. Be compilable (no syntax errors)

  """

  use Mix.Task

  alias Mix.Tasks.Pgflow.Helpers
  alias PgFlow.Flow.Definition
  alias PgFlow.JobCompiler

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
        Missing job module argument.

        Usage: mix pgflow.gen.job_migration MyApp.Jobs.MyJob
        """)

      _ ->
        Mix.raise("""
        Too many arguments provided.

        Usage: mix pgflow.gen.job_migration MyApp.Jobs.MyJob
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
      Module #{module_string} is not a PgFlow job.

      The module must use PgFlow.Job and define a perform block.

      Example:
          defmodule #{module_string} do
            use PgFlow.Job

            @job slug: :my_job, max_attempts: 3

            perform :my_step do
              fn input, _ctx -> %{result: input} end
            end
          end
      """)
    end

    definition = module.__pgflow_definition__()

    unless definition.flow_type == :job do
      Mix.raise("""
      Module #{module_string} is not a PgFlow job (it appears to be a flow).

      Use `mix pgflow.gen.flow_migration` for flow modules, or make sure this module uses `PgFlow.Job`.
      """)
    end

    case Definition.validate(definition) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Mix.raise("""
        Job definition validation failed: #{reason}

        Please fix the job definition and try again.
        """)
    end

    sql_statements = JobCompiler.compile(definition)

    # Check if job has cron configured
    has_cron = JobCompiler.has_cron?(module)

    migrations_path = Keyword.get(opts, :migrations_path, "priv/repo/migrations")
    File.mkdir_p!(migrations_path)

    timestamp = Helpers.generate_timestamp()

    job_slug = Atom.to_string(definition.slug)
    migration_module = "Compile#{Helpers.camelize(job_slug)}"

    migration_content =
      generate_migration_content(migration_module, job_slug, sql_statements, has_cron)

    filename = "#{timestamp}_compile_#{job_slug}.exs"
    filepath = Path.join(migrations_path, filename)

    File.write!(filepath, migration_content)

    Mix.shell().info("""
    Generated migration: #{filepath}

    Run the migration with:
        mix ecto.migrate

    This will:
      1. Create the '#{job_slug}' job as a flow in pgflow.flows
      2. Create the PGMQ queue 'pgmq.q_#{job_slug}'
      3. Register the step definition
      4. Mark flow_type = 'job' for dashboard differentiation

    After migration, your worker can process tasks from this job.
    """)
  end

  defp generate_migration_content(migration_module, job_slug, sql_statements, has_cron) do
    up_statements = format_execute_statements(sql_statements)
    unschedule_sql = if has_cron, do: JobCompiler.cron_unschedule_sql(String.to_atom(job_slug))
    down_statements = Helpers.build_down_statements(job_slug, has_cron, unschedule_sql)

    """
    defmodule PgFlow.Repo.Migrations.#{migration_module} do
      @moduledoc \"\"\"
      Compiles the '#{job_slug}' job definition into the database.

      This migration creates:
      - The flow record in pgflow.flows (with flow_type = 'job')
      - The PGMQ queue for this job
      - The step definition

      Generated by: mix pgflow.gen.job_migration
      \"\"\"
      use Ecto.Migration

      def up do
    #{up_statements}
      end

      def down do
    #{down_statements}
      end
    end
    """
  end

  defp format_execute_statements(sql_statements) do
    Enum.map_join(sql_statements, "\n", fn sql ->
      escaped = String.replace(sql, "\"", "\\\"")
      ~s(    execute "#{escaped}")
    end)
  end
end
