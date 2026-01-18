defmodule Mix.Tasks.Pgflow.Gen.Flow do
  @shortdoc "Generates an Ecto migration to compile a PgFlow flow"

  @moduledoc """
  Generates an Ecto migration that registers a PgFlow flow in the database.

  ## Usage

      mix pgflow.gen.flow MyApp.Flows.ArticleFlow
      mix pgflow.gen.flow MyApp.Flows.ArticleFlow --migrations-path priv/repo/migrations

  ## Options

    * `--migrations-path` - Path to the migrations directory.
      Defaults to `priv/repo/migrations`.

  ## Generated SQL

  The migration executes SQL statements that:
  1. Create the flow record and PGMQ queue
  2. Add each step with its dependencies and configuration

  Example generated migration:

      defmodule MyApp.Repo.Migrations.CompileArticleFlow do
        use Ecto.Migration

        def up do
          execute "SELECT pgflow.create_flow('article_flow', 3, 5, 120)"
          execute "SELECT pgflow.add_step('article_flow', 'fetch_article', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')"
          execute "SELECT pgflow.add_step('article_flow', 'summarize', ARRAY['fetch_article']::text[], NULL, NULL, NULL, NULL, 'single')"
        end

        def down do
          execute "DELETE FROM pgflow.deps WHERE flow_slug = 'article_flow'"
          execute "DELETE FROM pgflow.steps WHERE flow_slug = 'article_flow'"
          execute "DELETE FROM pgflow.flows WHERE flow_slug = 'article_flow'"
          execute "SELECT pgmq.drop_queue('article_flow')"
        end
      end

  ## Requirements

  The flow module must:
  1. Use `PgFlow.Flow`
  2. Define a valid flow with `@flow` and at least one step
  3. Be compilable (no syntax errors)

  ## Example

      # Define a flow
      defmodule MyApp.Flows.ArticleFlow do
        use PgFlow.Flow

        @flow slug: :article_flow, max_attempts: 3

        step :fetch do
          fn input, _ctx -> %{data: input} end
        end

        step :process, depends_on: [:fetch] do
          fn deps, _ctx -> %{result: deps.fetch} end
        end
      end

      # Generate the migration
      $ mix pgflow.gen.flow MyApp.Flows.ArticleFlow

      # Run the migration
      $ mix ecto.migrate

  """

  use Mix.Task

  alias PgFlow.Flow.Definition
  alias PgFlow.FlowCompiler

  @impl Mix.Task
  def run(args) do
    # Parse arguments
    {opts, args, _} =
      OptionParser.parse(args,
        switches: [migrations_path: :string],
        aliases: [p: :migrations_path]
      )

    # Validate we have a module name
    case args do
      [module_string] ->
        generate_migration(module_string, opts)

      [] ->
        Mix.raise("""
        Missing flow module argument.

        Usage: mix pgflow.gen.flow MyApp.Flows.MyFlow
        """)

      _ ->
        Mix.raise("""
        Too many arguments provided.

        Usage: mix pgflow.gen.flow MyApp.Flows.MyFlow
        """)
    end
  end

  defp generate_migration(module_string, opts) do
    # Ensure the application is loaded so we can access flow modules
    Mix.Task.run("compile", [])

    # Convert string to module
    module = String.to_atom("Elixir.#{module_string}")

    # Verify the module exists and has the required function
    unless Code.ensure_loaded?(module) do
      Mix.raise("""
      Module #{module_string} could not be loaded.

      Make sure the module exists and the project compiles successfully.
      """)
    end

    unless function_exported?(module, :__pgflow_definition__, 0) do
      Mix.raise("""
      Module #{module_string} is not a PgFlow flow.

      The module must use PgFlow.Flow and define at least one step.

      Example:
          defmodule #{module_string} do
            use PgFlow.Flow

            @flow slug: :my_flow, max_attempts: 3

            step :my_step do
              fn input, _ctx -> %{result: input} end
            end
          end
      """)
    end

    # Get the flow definition
    definition = module.__pgflow_definition__()

    # Validate the definition
    case Definition.validate(definition) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Mix.raise("""
        Flow definition validation failed: #{reason}

        Please fix the flow definition and try again.
        """)
    end

    # Generate SQL statements
    sql_statements = FlowCompiler.compile(definition)

    # Determine migrations path
    migrations_path = Keyword.get(opts, :migrations_path, "priv/repo/migrations")

    # Create migrations directory if it doesn't exist
    File.mkdir_p!(migrations_path)

    # Generate timestamp for migration filename
    timestamp = generate_timestamp()

    # Generate migration module name from flow slug
    flow_slug = Atom.to_string(definition.slug)
    migration_module = "Compile#{camelize(flow_slug)}"

    # Generate the migration content
    migration_content = generate_migration_content(migration_module, flow_slug, sql_statements)

    # Write the migration file
    filename = "#{timestamp}_compile_#{flow_slug}.exs"
    filepath = Path.join(migrations_path, filename)

    File.write!(filepath, migration_content)

    Mix.shell().info("""
    Generated migration: #{filepath}

    Run the migration with:
        mix ecto.migrate

    This will:
      1. Create the '#{flow_slug}' flow in pgflow.flows
      2. Create the PGMQ queue 'pgmq.q_#{flow_slug}'
      3. Register all #{length(definition.steps)} step(s)

    After migration, your worker can process tasks from this flow.
    """)
  end

  defp generate_migration_content(migration_module, flow_slug, sql_statements) do
    up_statements =
      Enum.map_join(sql_statements, "\n", &"    execute \"#{&1}\"")

    """
    defmodule PgFlow.Repo.Migrations.#{migration_module} do
      @moduledoc \"\"\"
      Compiles the '#{flow_slug}' flow definition into the database.

      This migration creates:
      - The flow record in pgflow.flows
      - The PGMQ queue for this flow
      - All step definitions in pgflow.steps

      Generated by: mix pgflow.gen.flow
      \"\"\"
      use Ecto.Migration

      def up do
    #{up_statements}
      end

      def down do
        execute "DELETE FROM pgflow.deps WHERE flow_slug = '#{flow_slug}'"
        execute "DELETE FROM pgflow.steps WHERE flow_slug = '#{flow_slug}'"
        execute "DELETE FROM pgflow.flows WHERE flow_slug = '#{flow_slug}'"
        execute "SELECT pgmq.drop_queue('#{flow_slug}')"
      end
    end
    """
  end

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
