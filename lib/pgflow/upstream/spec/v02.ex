defmodule PgFlow.Upstream.Spec.V02 do
  @moduledoc """
  Record of how core V02 was imported from upstream `94490709`.

  V02 is published. `PgFlow.Upstream.Bundle` refuses to rebuild it. These
  functions stay so the pinned files, hashes, and portability rules remain
  reviewable next to the frozen SQL.
  """

  alias PgFlow.Upstream.Bundle

  import PgFlow.Upstream.Spec.Matchers,
    only: [localize_lock_timeout: 1, set_lock_timeout?: 1, start_tasks_definition?: 1]

  @pinned_sha "94490709f79ebf366141dd925b047f0c1013e759"
  @files [
    "20260607175525_pgflow_worker_start_mode.sql",
    "20260904095427_pgflow_task_lifecycle_hardening.sql",
    "20260907082520_pgflow_remove_legacy_flow_compilation.sql",
    "20260913093141_pgflow_persist_queue_identity.sql"
  ]
  @source_hashes %{
    "20260607175525_pgflow_worker_start_mode.sql" =>
      "29c66ed86b85e2ae5693fc49ec410c901bfeebf320cda2a02720dccda233640a",
    "20260904095427_pgflow_task_lifecycle_hardening.sql" =>
      "41fb8e26642685f2269aa0bcc9bd4e9a512292bd7f2467f59d3577e1687f2d68",
    "20260907082520_pgflow_remove_legacy_flow_compilation.sql" =>
      "bc10af88b655a84f271a5aa5aea79f8ac74680e423293f8be1fac5656b4983b0",
    "20260913093141_pgflow_persist_queue_identity.sql" =>
      "156de3b8a73812cdba59ee19af1ec8524834f74209c9cb914a2d575c33fe505a"
  }

  @type rule :: %{
          required(:id) => atom(),
          required(:match) => (String.t() -> boolean()),
          required(:action) =>
            :omit | :reject | {:replace, (String.t() -> term())} | {:prepend, String.t()},
          required(:expected) => non_neg_integer(),
          required(:reason) => String.t()
        }

  @doc "Upstream commit whose bytes produced core V02."
  @spec pinned_sha() :: String.t()
  def pinned_sha, do: @pinned_sha

  @doc "Upstream migration filenames, in import order."
  @spec files() :: [String.t()]
  def files, do: @files

  @doc "SHA-256 of each pinned upstream file."
  @spec source_hashes() :: %{String.t() => String.t()}
  def source_hashes, do: @source_hashes

  @doc """
  Portability rules applied when V02 was built.

  The first matching rule wins. `:reject` rules are expected to match nothing
  in the pinned sources.
  """
  @spec rules() :: [rule()]
  def rules do
    [
      %{
        id: :omit_ensure_workers,
        match: &ensure_workers_with_net?/1,
        action: :omit,
        expected: 1,
        reason: "requires Supabase-only net.http_post"
      },
      %{
        id: :reject_ensure_workers_without_net,
        match: &ensure_workers_without_net?/1,
        action: :reject,
        expected: 0,
        reason: "refusing to omit pgflow.ensure_workers(): expected net.http_post dependency"
      },
      %{
        id: :reject_net_http_post,
        match: &other_net_http_post?/1,
        action: :reject,
        expected: 0,
        reason: "unrecognized net.http_post infrastructure dependency"
      },
      %{
        id: :set_local_lock_timeout,
        match: &set_lock_timeout?/1,
        action: {:replace, &localize_lock_timeout/1},
        expected: 1,
        reason: "transaction-local setting does not leak into pooled connections"
      },
      %{
        id: :step_task_record_dispatch,
        match: &start_tasks_definition?/1,
        action: {:replace, &Bundle.shape_dispatch/1},
        expected: 2,
        reason: "preserve the Elixir attempts_count extension"
      }
    ]
  end

  @doc "Manifest fields that describe the V02 import but are not derived from the SQL."
  @spec manifest_notes() :: map()
  def manifest_notes do
    %{
      "removals" => [
        %{
          "object" => "pgflow.ensure_workers()",
          "reason" => "requires Supabase-only net.http_post"
        }
      ],
      "upstream_path" => "pkgs/core/supabase/migrations",
      "upstream_repo" => "pgflow-dev/pgflow"
    }
  end

  defp ensure_workers_with_net?(statement) do
    ensure_workers_definition?(statement) and statement =~ "net.http_post"
  end

  defp ensure_workers_without_net?(statement) do
    ensure_workers_definition?(statement) and not (statement =~ "net.http_post")
  end

  defp other_net_http_post?(statement) do
    statement =~ "net.http_post" and not ensure_workers_definition?(statement)
  end

  defp ensure_workers_definition?(statement) do
    Regex.match?(
      ~r/CREATE OR REPLACE FUNCTION "pgflow"\."ensure_workers" \(\) RETURNS TABLE/s,
      statement
    )
  end
end
