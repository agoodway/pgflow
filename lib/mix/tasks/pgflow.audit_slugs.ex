defmodule Mix.Tasks.Pgflow.AuditSlugs do
  @shortdoc "Reports flow and step slugs that pgflow 0.17.2 rejects"

  @moduledoc """
  Reports flow and step slugs that pgflow 0.17.2 rejects.

  The check is read-only. It never renames or rewrites slugs. Fix each
  finding, then upgrade. See `docs/UPGRADING_UPSTREAM_2026_10.md`.

  ## Usage

      mix pgflow.audit_slugs [options]

  ## Options

    * `--repo` - The Ecto repo to read. Default: the first configured repo
    * `--prefix` - Schema that holds `flows` and `steps`. Default: `pgflow`

  ## Examples

      mix pgflow.audit_slugs
      mix pgflow.audit_slugs --repo MyApp.Repo

  """

  use Mix.Task

  alias Mix.Tasks.Pgflow.Helpers
  alias PgFlow.Upgrade.SlugAudit

  @impl Mix.Task
  @spec run([String.t()]) :: :ok
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, switches: [repo: :string, prefix: :string])

    Mix.Task.run("app.config")

    repo = Helpers.resolve_repo(Keyword.get(opts, :repo))
    prefix = Keyword.get(opts, :prefix, "pgflow")

    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    Helpers.start_repo(repo)

    case SlugAudit.run(repo, prefix: prefix) do
      [] ->
        Mix.shell().info("no slug findings")

      findings ->
        Enum.each(findings, fn finding -> Mix.shell().info(format_finding(finding)) end)
        Mix.raise("#{length(findings)} slug finding(s); fix them before upgrading")
    end
  end

  defp format_finding(%{kind: kind, flow_slug: flow_slug, reason: reason} = finding) do
    step =
      case Map.get(finding, :step_slug) do
        nil -> ""
        slug -> ".#{slug}"
      end

    "#{kind} #{flow_slug}#{step}: #{reason}"
  end
end
