defmodule PgflowDemo.Checks.SchemaContract do
  @moduledoc """
  Reports the pgflow schema contract of the running demo database.

    * `PgFlow.compatibility_report/0`: pinned upstream release and the bundled
      core/helpers schema versions
    * `PgFlow.SchemaCheck.run/1`: installed versions, signatures, and record layout
    * `PgFlow.Upgrade.SlugAudit.run/1`: read-only slug audit (no findings expected)
    * `pgflow.workers.pgflow_version` for live Elixir workers, which carry the
      `PgFlow.Version.worker_stamp/0` value (`elixir-<vsn>`)

  A worker is live when it has neither stopped nor been deprecated and its
  heartbeat is less than a minute old.
  """

  @behaviour PgflowDemo.ScenarioChecks

  alias Ecto.Adapters.SQL
  alias PgFlow.SchemaCheck
  alias PgFlow.Upgrade.SlugAudit
  alias PgflowDemo.Repo

  @live_workers_sql """
  SELECT count(*), coalesce(array_agg(DISTINCT pgflow_version ORDER BY pgflow_version), '{}')
  FROM pgflow.workers
  WHERE stopped_at IS NULL
    AND deprecated_at IS NULL
    AND last_heartbeat_at > now() - interval '1 minute'
    AND function_name LIKE 'elixir:%'
  """

  @impl true
  @spec run(map()) :: map()
  def run(_input) do
    report = PgFlow.compatibility_report()
    worker_stamp = PgFlow.Version.worker_stamp()
    {live_count, stamps} = live_workers()

    %{
      "upstream_release" => report.upstream_release,
      "upstream_sha" => report.upstream_sha,
      "core_version_string" => report.core_version_string,
      "bundled_core_version" => report.bundled_core_version,
      "bundled_helpers_version" => report.bundled_helpers_version,
      "schema_check" => schema_check(),
      "slug_audit_findings" => Enum.map(SlugAudit.run(Repo), &stringify_finding/1),
      "worker_stamp" => worker_stamp,
      "live_elixir_workers" => live_count,
      "live_worker_stamps" => stamps,
      "live_workers_stamped" => live_count > 0 and stamps == [worker_stamp]
    }
  end

  defp schema_check do
    case SchemaCheck.run(Repo) do
      :ok -> "ok"
      {:error, messages} -> messages
    end
  end

  defp live_workers do
    %{rows: [[count, stamps]]} = SQL.query!(Repo, @live_workers_sql, [])
    {count, stamps}
  end

  defp stringify_finding(finding) do
    Map.new(finding, fn {key, value} -> {Atom.to_string(key), to_string(value)} end)
  end
end
