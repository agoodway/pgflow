defmodule Mix.Tasks.Pgflow.Stamp do
  @shortdoc "Stamp an existing pgflow deployment with the EctoEvolver tracking comment"

  @moduledoc """
  Marks an existing pgflow schema as installed under EctoEvolver's tracking
  model without re-running V01 DDL.

  Use when the pgflow schema already exists in the target database but is
  missing the tracking comment on `pgflow.pgflow_version` — typical triggers:

    * Restored from a DB dump that lost object comments.
    * Schema was seeded manually via `psql` or another out-of-band path.
    * `PgFlow.Migration.up/0`'s preflight raised and pointed you here.

  Writes the tracking comment only; runs no schema SQL. The next
  `mix ecto.migrate` sees version 1 already applied and becomes a no-op.

  **Destructive if schema drift exists.** If the installed schema diverges
  from what `PgFlow.Migration` V01 would install, this task masks that
  drift silently. Use only when you know the installed schema matches the
  vendored V01 bundle.

  ## Usage

      mix pgflow.stamp
      mix pgflow.stamp --repo MyApp.OtherRepo --prefix custom_schema

  ## Options

    * `--repo` - Repo module to stamp (default: first in `:ecto_repos`).
    * `--prefix` - Schema prefix (default: `"pgflow"`).
  """

  use Mix.Task

  @impl Mix.Task
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string, prefix: :string])

    repo = resolve_repo(opts[:repo])
    prefix = Keyword.get(opts, :prefix, "pgflow")

    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:pgflow)
    {:ok, _} = repo.start_link(pool_size: 2)

    # Verify the pgflow_version view exists — required for the COMMENT target.
    case repo.query(
           """
           SELECT 1 FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = $1 AND c.relname = 'pgflow_version' AND c.relkind = 'v'
           """,
           [prefix]
         ) do
      {:ok, %{rows: [[1]]}} ->
        :ok

      _ ->
        Mix.raise("""
        Can't stamp: `#{prefix}.pgflow_version` view is missing.

        Either run `PgFlow.Migration.up/0` (which creates it) or create it
        manually:

          CREATE OR REPLACE VIEW #{prefix}.pgflow_version AS SELECT 1 AS placeholder
        """)
    end

    # Write the EctoEvolver-style comment directly.
    comment = "PgFlow version=1"
    sql = ~s|COMMENT ON VIEW "#{prefix}"."pgflow_version" IS '#{comment}'|

    case repo.query(sql) do
      {:ok, _} ->
        Mix.shell().info("Stamped #{prefix}.pgflow_version → #{inspect(comment)}")

      {:error, err} ->
        Mix.raise("Failed to stamp: #{Exception.message(err)}")
    end
  end

  defp resolve_repo(nil) do
    app = Mix.Project.config()[:app]

    case Application.get_env(app, :ecto_repos, []) do
      [repo | _] -> repo
      [] -> Mix.raise("No Ecto repos configured. Add `:ecto_repos` to your app config.")
    end
  end

  defp resolve_repo(repo_string), do: Module.concat([repo_string])
end
