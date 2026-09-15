#!/usr/bin/env mix run
# Test-only upstream compatibility harness. Not a production dependency.
#
# Required host packages by suite:
#   catalog     — psql, git
#   pgtap       — psql, git, supabase CLI, and Postgres host package postgresql-*-pgtap
#   typescript  — node, pnpm, git (upstream packages built from pinned checkout)
#   all         — union of the above
#
# The pgtap suite is a required profile (never silently skipped). Fixture databases must
# already carry the Elixir-installed schema (see setup_compat_database!/1). The harness
# creates CREATE EXTENSION pgtap on the fixture DB before running upstream pgTAP files.
#
# Local docker stack: test/support/db/compose.yaml + test/support/db/install-pgtap.sh
# CI: .github/workflows/upstream-compatibility.yml installs postgresql-17-pgtap in the
# Postgres service container before running --suite all.

Mix.Task.run("app.config")
Mix.Task.run("compile")
{:ok, _} = Application.ensure_all_started(:ecto_sql)

{opts, argv, invalid} =
  OptionParser.parse(System.argv(),
    strict: [
      checkout: :string,
      sha: :string,
      database_url: :string,
      suite: :string
    ]
  )

if invalid != [] or argv != [] do
  IO.puts(:stderr, "usage: mix run --no-start test/support/upstream/run.exs \\")

  IO.puts(
    :stderr,
    "  --checkout PATH --sha SHA --database-url URL --suite catalog|pgtap|typescript|all"
  )

  System.halt(2)
end

checkout = Keyword.fetch!(opts, :checkout)
sha = Keyword.fetch!(opts, :sha)
database_url = Keyword.fetch!(opts, :database_url)

suite =
  case Keyword.get(opts, :suite, "all") do
    "catalog" ->
      :catalog

    "pgtap" ->
      :pgtap

    "typescript" ->
      :typescript

    "all" ->
      :all

    other ->
      IO.puts(:stderr, "unknown suite #{inspect(other)}")
      System.halt(2)
  end

System.put_env("PGFLOW_UPSTREAM_CHECKOUT", checkout)

case PgFlow.Test.UpstreamHarness.run(
       checkout: checkout,
       sha: sha,
       database_url: database_url,
       suite: suite
     ) do
  {:ok, report} ->
    IO.puts(Jason.encode!(report, pretty: true))

    if report["failures"] > 0 or report["tests"] == 0 or
         Enum.any?(report["results"], &(&1["tests"] == 0 or &1["status"] == "failed")) do
      System.halt(1)
    end

  {:error, reason} ->
    IO.puts(:stderr, reason)
    System.halt(1)
end
