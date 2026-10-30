defmodule PgFlow.Upgrade.SlugAudit do
  @moduledoc """
  Read-only audit of flow and step slugs against the pgflow 0.17.2 rules.

  `PgFlow.Slug` is the reference implementation. The SQL here must agree with
  it, and it must not call `pgflow.is_valid_slug/1`: on core V02 that function
  still implements the old rule. The same SQL text feeds `mix pgflow.audit_slugs`,
  the core V03 pre-flight block, and the tests.

  The audit never renames slugs. Fix each finding, then upgrade. See
  `docs/UPGRADING_UPSTREAM_2026_10.md`.
  """

  alias Ecto.Adapters.SQL

  @read_only_statement "SET TRANSACTION READ ONLY"
  @prefix_pattern ~r/\A[a-z_][a-z0-9_]*\z/
  @kinds ~w(flow step step_collision)a
  @reasons ~w(
    empty
    too_long
    bad_chars
    reserved
    bad_start
    bad_end
    double_underscore
    queue_name_too_long
    case_insensitive_duplicate
  )a

  @type kind :: :flow | :step | :step_collision
  @type reason ::
          :empty
          | :too_long
          | :bad_chars
          | :reserved
          | :bad_start
          | :bad_end
          | :double_underscore
          | :queue_name_too_long
          | :case_insensitive_duplicate
  @type finding :: %{
          required(:kind) => kind(),
          required(:flow_slug) => String.t(),
          optional(:step_slug) => String.t(),
          required(:reason) => reason()
        }

  @doc """
  Boolean SQL for one slug expression, in the same order as `PgFlow.Slug.validate/1`.

  `expr` is interpolated, so it must be a trusted SQL expression such as `$1`
  or a column name. This does not call `pgflow.is_valid_slug/1`.
  """
  @spec rule_sql(String.t()) :: String.t()
  def rule_sql(expr) when is_binary(expr) do
    "#{expr} IS NOT NULL AND #{expr} <> '' AND length(#{expr}) <= 128 " <>
      "AND #{expr} ~ '^[a-zA-Z_][a-zA-Z0-9_]*$' AND #{expr} NOT IN ('run') " <>
      "AND left(#{expr}, 1) ~ '[a-zA-Z]' AND right(#{expr}, 1) ~ '[a-zA-Z0-9]' " <>
      "AND position('__' in #{expr}) = 0"
  end

  @doc """
  `CASE` expression that returns the `PgFlow.Slug` reason as text, or NULL when valid.
  """
  @spec reason_sql(String.t()) :: String.t()
  def reason_sql(expr) when is_binary(expr) do
    """
    CASE
      WHEN #{expr} IS NULL OR #{expr} = '' THEN 'empty'
      WHEN length(#{expr}) > 128 THEN 'too_long'
      WHEN #{expr} !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN 'bad_chars'
      WHEN #{expr} IN ('run') THEN 'reserved'
      WHEN left(#{expr}, 1) !~ '[a-zA-Z]' THEN 'bad_start'
      WHEN right(#{expr}, 1) !~ '[a-zA-Z0-9]' THEN 'bad_end'
      WHEN position('__' in #{expr}) > 0 THEN 'double_underscore'
      ELSE NULL
    END\
    """
  end

  @doc """
  Selects `kind`, `flow_slug`, `step_slug`, and `reason` for every 0.17.2 violation.

  `prefix` must match `~r/\\A[a-z_][a-z0-9_]*\\z/` and is inserted as a quoted identifier.
  """
  @spec findings_sql(String.t()) :: String.t()
  def findings_sql(prefix) when is_binary(prefix) do
    schema = quoted_prefix!(prefix)
    flow_reason = reason_sql("flow_slug")
    step_reason = reason_sql("step_slug")

    """
    SELECT kind, flow_slug, step_slug, reason
    FROM (
      SELECT 'flow'::text AS kind, flow_slug, NULL::text AS step_slug, #{flow_reason} AS reason
      FROM #{schema}.flows
      WHERE NOT (#{rule_sql("flow_slug")})
      UNION ALL
      SELECT 'flow'::text, flow_slug, NULL::text, 'queue_name_too_long'
      FROM #{schema}.flows
      WHERE length(lower(flow_slug)) > 47
      UNION ALL
      SELECT 'step'::text, flow_slug, step_slug, #{step_reason}
      FROM #{schema}.steps
      WHERE NOT (#{rule_sql("step_slug")})
      UNION ALL
      SELECT 'step_collision'::text, flow_slug,
             string_agg(step_slug, ',' ORDER BY step_slug COLLATE "C"),
             'case_insensitive_duplicate'
      FROM #{schema}.steps
      GROUP BY flow_slug, lower(step_slug)
      HAVING count(*) > 1
    ) AS findings
    ORDER BY kind, flow_slug, step_slug
    """
  end

  @doc """
  Returns slug findings for the given repo.

  Issues `#{@read_only_statement}` as the first statement of a transaction, then
  reads `findings_sql/1`. A caller already inside a transaction that has run
  statements (the Ecto SQL sandbox) makes PostgreSQL reject that command with
  `25001`. The savepoint is rolled back and the same read-only `SELECT` runs
  in the caller's transaction. `mix pgflow.audit_slugs` opens a fresh
  transaction, so that path stays read only.

  Flow rows omit a nil `step_slug` so the finding map matches the audit tests.
  """
  @spec run(module(), keyword()) :: [finding()]
  def run(repo, opts \\ []) when is_atom(repo) do
    sql = findings_sql(prefix!(opts))

    case repo.transaction(fn -> read_in_transaction(repo, sql) end) do
      {:ok, findings} -> findings
      {:error, :read_only_too_late} -> fetch!(repo, sql)
    end
  end

  defp read_in_transaction(repo, sql) do
    case SQL.query(repo, @read_only_statement, [], log: false) do
      {:ok, _} ->
        ensure_read_only!(repo)
        fetch!(repo, sql)

      {:error, error} ->
        rollback_or_raise(repo, error)
    end
  end

  defp rollback_or_raise(repo, error) do
    if read_only_too_late?(error) do
      repo.rollback(:read_only_too_late)
    else
      raise error
    end
  end

  @doc """
  The statement `run/2` issues before reading findings.
  """
  @spec read_only_statement() :: String.t()
  def read_only_statement, do: @read_only_statement

  @doc """
  `DO` block that raises `check_violation` listing every offender.

  Installed databases are never rewritten. The hint points at
  `mix pgflow.audit_slugs` and `docs/UPGRADING_UPSTREAM_2026_10.md`.
  """
  @spec preflight_sql(String.t()) :: String.t()
  def preflight_sql(prefix) when is_binary(prefix) do
    """
    DO $pgflow_slug_preflight$
    DECLARE
      v text;
    BEGIN
      SELECT string_agg(
        format('%s %s%s: %s', kind, flow_slug, coalesce('.' || step_slug, ''), reason),
        E'\\n'
      )
      INTO v
      FROM (
    #{findings_sql(prefix)}
      ) AS f;

      IF v IS NOT NULL THEN
        RAISE EXCEPTION 'pgflow 0.17.2 slug pre-flight failed:%', E'\\n' || v
          USING ERRCODE = 'check_violation',
            HINT = #{sql_quote(preflight_hint())};
      END IF;
    END
    $pgflow_slug_preflight$
    """
  end

  defp preflight_hint do
    "Run mix pgflow.audit_slugs and see docs/UPGRADING_UPSTREAM_2026_10.md. " <>
      "Slugs are never renamed automatically."
  end

  defp sql_quote(text) do
    "'" <> String.replace(text, "'", "''") <> "'"
  end

  defp prefix!(opts) do
    prefix = Keyword.get(opts, :prefix, "pgflow")

    if is_binary(prefix) do
      prefix
    else
      raise ArgumentError, "invalid pgflow schema prefix #{inspect(prefix)}"
    end
  end

  defp quoted_prefix!(prefix) do
    if Regex.match?(@prefix_pattern, prefix) do
      ~s("#{prefix}")
    else
      raise ArgumentError, "invalid pgflow schema prefix #{inspect(prefix)}"
    end
  end

  defp ensure_read_only!(repo) do
    %{rows: [[mode]]} = SQL.query!(repo, "SHOW transaction_read_only", [], log: false)

    unless mode == "on" do
      raise "slug audit transaction is #{inspect(mode)}, expected read only"
    end
  end

  defp fetch!(repo, sql) do
    %{rows: rows} = SQL.query!(repo, sql, [])

    Enum.map(rows, fn [kind, flow_slug, step_slug, reason] ->
      finding = %{
        kind: existing_atom!(kind, @kinds),
        flow_slug: flow_slug,
        reason: existing_atom!(reason, @reasons)
      }

      if is_nil(step_slug), do: finding, else: Map.put(finding, :step_slug, step_slug)
    end)
  end

  defp existing_atom!(value, allowed) when is_binary(value) do
    atom = String.to_existing_atom(value)

    if atom in allowed do
      atom
    else
      raise ArgumentError, "unexpected slug audit value #{inspect(value)}"
    end
  end

  defp read_only_too_late?(%Postgrex.Error{postgres: %{pg_code: "25001"}}), do: true
  defp read_only_too_late?(%Postgrex.Error{postgres: %{code: :active_sql_transaction}}), do: true
  defp read_only_too_late?(_error), do: false
end
