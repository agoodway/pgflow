defmodule PgFlow.Migrations.Core.V01 do
  @moduledoc """
  Version 1 of the core pgflow schema.

  The SQL bundle at `priv/pgflow_core/sql/versions/v01/v01_up.sql` is
  vendored from the upstream
  [pgflow-dev/pgflow](https://github.com/pgflow-dev/pgflow) Postgres SQL
  migrations. See `v01_manifest.json` for the pinned upstream commit SHA
  and the file list consumed at mint time.

  V01 is frozen. When upstream ships new core SQL, it becomes a new V02
  delta — V01 is never rewritten, otherwise deployments already on V01
  would silently diverge from the current bundle.

  Four categories of statements are stripped during sync because they
  require Supabase-specific infrastructure:

    * `CREATE SCHEMA "pgmq"` / `CREATE EXTENSION "pgmq"` — pgmq is installed
      SQL-only before this migration runs (via `mix pgflow.gen.pgmq_migration`).
    * `CREATE EXTENSION "pg_net"` — Supabase-only HTTP extension; pgflow
      Elixir bindings make HTTP calls from Elixir, not from SQL.
    * Functions that reference `net.http_post` — depend on pg_net.
  """

  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "01",
    sql_path: "pgflow_core/sql/versions"
end
