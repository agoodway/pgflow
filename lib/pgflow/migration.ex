defmodule PgFlow.Migration do
  @moduledoc """
  Installs the pgflow core schema (tables, triggers, functions) into a
  consumer app's database via EctoEvolver.

  ## Usage (consumer side)

  A consumer uses `mix pgflow.setup` to generate a wrapper migration:

      defmodule MyApp.Repo.Migrations.SetupPgflow do
        use Ecto.Migration

        def up do
          PgFlow.Migration.up()
          PgFlow.HelpersMigration.up()
        end

        def down do
          PgFlow.HelpersMigration.down()
          PgFlow.Migration.down()
        end
      end

  ## Prerequisites

  This migration assumes the following are already in place in the target DB:

    * `pgmq` schema and its functions (install via `mix pgflow.gen.pgmq_migration`)
    * `pg_cron` extension registered (`CREATE EXTENSION pg_cron`)
    * Standard Postgres extensions: `citext`, `pg_trgm`, `pgcrypto`

  Ordering: the prerequisites above must be applied first (separate
  migrations). The typical project-level order is
  `postgres_extensions → pgmq → pgflow.setup`, where `setup` bundles a
  wrapper migration that runs `PgFlow.Migration.up/0` then
  `PgFlow.HelpersMigration.up/0` in one transaction.

  ## Schema prefix

  The SQL bundle hardcodes `"pgflow"` as the schema name (matching upstream
  conventions). `default_prefix: "pgflow"` is fixed; runtime prefix override
  via `up(prefix: ...)` is not supported by this version because it would
  require rewriting every qualified reference in the vendored SQL. If the
  library later needs multi-tenant schema prefixes, regenerate the bundle
  with `$SCHEMA$` placeholders.

  ## Versioning

  New upstream pgflow releases become `V02`, `V03`, ... — each a delta from
  the previous version. V01 is never rewritten; existing deployments apply
  only new versions on upgrade.

  ## Legacy install detection

  `up/1` refuses to run V01 DDL against a schema that already contains
  pgflow tables but is missing EctoEvolver's tracking comment — a restored
  dump that lost comments, a manual `psql` seed, or any schema not installed
  by `PgFlow.Migration.up/0` itself. The raised error points the operator at
  `mix pgflow.stamp`, which adopts the existing schema into the tracking
  model without re-running the bundle.
  """

  @default_prefix "pgflow"

  # Core tables installed by V01. If any exist in a prefix with no tracking
  # comment, the schema was installed by something other than
  # `PgFlow.Migration.up/1`.
  @sentinel_tables ~w(flows runs steps step_states step_tasks workers)

  import Ecto.Migration, only: [repo: 0]

  # EctoEvolver's __using__ generates up/1, down/1, current_version/0, and
  # migrated_version/1. Isolating those in a nested module lets the public
  # up/1 run a preflight check before delegating.
  defmodule Evolver do
    @moduledoc false

    # EctoEvolver's `use` macro generates clauses for :table, :view, and
    # :materialized_view tracking objects. Only :view is exercised here.
    @dialyzer :no_match

    use EctoEvolver,
      otp_app: :pgflow,
      default_prefix: "pgflow",
      versions: [PgFlow.Migrations.Core.V01],
      tracking_object: {:view, "pgflow_version"}
  end

  @doc """
  Installs or upgrades the pgflow schema to the latest version.

  Options:

    * `:prefix` — schema prefix (default `"pgflow"`).
    * `:version` — target version (default: latest).

  Raises if the target schema already contains pgflow tables without an
  EctoEvolver tracking comment — see "Legacy install detection" above.
  """
  @spec up(keyword()) :: :ok
  def up(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, @default_prefix)
    :ok = preflight!(prefix)
    Evolver.up(opts)
  end

  defdelegate down(opts \\ []), to: Evolver
  defdelegate current_version(), to: Evolver
  defdelegate migrated_version(opts \\ []), to: Evolver

  defp preflight!(prefix) do
    cond do
      Evolver.migrated_version(prefix: prefix) > 0 -> :ok
      legacy_install?(prefix) -> raise legacy_install_message(prefix)
      true -> :ok
    end
  end

  # Queries pg_class directly (same source EctoEvolver's adapter uses) so the
  # preflight and tracking-version checks see identical catalog state.
  # Fails closed: an unexpected DB error raises rather than silently
  # returning `false`, which would let V01 run against an uninspectable
  # schema and produce the CREATE TABLE conflicts the preflight exists to
  # prevent.
  defp legacy_install?(prefix) do
    query = """
    SELECT COUNT(*)::int
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = ANY($2::text[]) AND c.relkind = 'r'
    """

    case repo().query(query, [prefix, @sentinel_tables]) do
      {:ok, %{rows: [[count]]}} ->
        count > 0

      {:error, reason} ->
        raise preflight_query_error(prefix, reason)

      other ->
        raise "pgflow preflight: unexpected query result shape: #{inspect(other)}"
    end
  end

  defp preflight_query_error(prefix, reason) do
    detail = if is_exception(reason), do: Exception.message(reason), else: inspect(reason)

    """
    pgflow: preflight query failed in schema "#{prefix}"

    #{detail}

    Refusing to proceed — running V01 against a schema pgflow can't inspect
    risks CREATE TABLE conflicts against existing objects. Resolve the DB
    issue and retry, or inspect the target schema manually.
    """
  end

  defp legacy_install_message(prefix) do
    """
    pgflow: legacy install detected in schema "#{prefix}"

    Tables from an older pgflow install are present, but EctoEvolver's
    tracking comment on "#{prefix}".pgflow_version is missing. Running the
    V01 bundle now would conflict with the existing schema.

    If the installed schema matches PgFlow.Migration V01, adopt it with:

        mix pgflow.stamp --prefix #{prefix}

    then re-run `mix ecto.migrate`. If the installed schema has drifted
    from V01, do NOT stamp — reconcile the drift manually or rebuild from
    a clean schema.
    """
  end
end
