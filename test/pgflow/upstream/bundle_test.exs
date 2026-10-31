defmodule PgFlow.Upstream.BundleTest do
  use ExUnit.Case, async: false

  alias PgFlow.Upstream.Bundle
  alias PgFlow.Upstream.Spec.V02

  @sha "94490709f79ebf366141dd925b047f0c1013e759"
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

  test "V02 output is frozen to its manifest checksum" do
    sql = File.read!("priv/pgflow_core/sql/versions/v02/v02_up.sql")

    manifest =
      "priv/pgflow_core/sql/versions/v02/v02_manifest.json"
      |> File.read!()
      |> Jason.decode!()

    assert :crypto.hash(:sha256, sql) |> Base.encode16(case: :lower) == manifest["output_sha256"]
  end

  test "build/3 rejects unknown versions" do
    assert {:error, "unsupported core delta version \"09\"; expected 03"} =
             Bundle.build("/nonexistent", "deadbeef", "09")
  end

  test "build/3 with 02 refuses because V02 is published" do
    assert {:error, "refusing to rewrite published core version V02"} =
             Bundle.build("/nonexistent", "deadbeef", "02")
  end

  test "build/3 refuses an unpinned SHA for V03" do
    assert {:error, message} = Bundle.build("/nonexistent", "deadbeef", "03")
    assert message =~ "refusing unpinned upstream SHA"
    assert message =~ "baa9e43f5f9122133deacbc3e8ffc80e284b4f50"
  end

  describe "committed artifacts" do
    test "V02 output and manifest match pinned source provenance and V01 remains frozen" do
      root = File.cwd!()
      version_dir = Path.join(root, "priv/pgflow_core/sql/versions/v02")
      sql = File.read!(Path.join(version_dir, "v02_up.sql"))

      manifest =
        Path.join(version_dir, "v02_manifest.json")
        |> File.read!()
        |> Jason.decode!()

      assert manifest["upstream_sha"] == @sha
      assert manifest["files"] == @files
      assert manifest["source_sha256"] == @source_hashes
      assert V02.pinned_sha() == @sha
      assert V02.files() == @files
      assert V02.source_hashes() == @source_hashes
      assert manifest["output_sha256"] == sha256(sql)
      assert manifest["statement_count"] == 41

      assert manifest["removals"] == [
               %{
                 "object" => "pgflow.ensure_workers()",
                 "reason" => "requires Supabase-only net.http_post"
               }
             ]

      assert manifest["transformations"] == [
               %{
                 "change" => "SET lock_timeout = '10s' -> SET LOCAL lock_timeout = '10s'",
                 "reason" => "transaction-local setting does not leak into pooled connections"
               },
               %{
                 "change" =>
                   "start_tasks installations dispatch on seven- or eight-column step_task_record",
                 "reason" => "preserve the Elixir attempts_count extension"
               }
             ]

      refute sql =~ "net.http_post"
      refute sql =~ ~r/^SET lock_timeout\b/m
      assert sql =~ "SET LOCAL lock_timeout = '10s'"
      assert sql =~ ~s("worker_functions"."start_mode")
      assert sql =~ ~s("track_worker_function" ("function_name" text, "start_mode" text)
      assert sql =~ "unsupported pgflow.step_task_record layout"
      assert sql =~ "step_tasks.attempts_count"
      assert sql =~ "st.attempts_count"

      assert sha256(File.read!("priv/pgflow_core/sql/versions/v01/v01_up.sql")) ==
               "c41af810811f82d3ef7e97113921d5d5f52d837011c33c168e3b2de8c908d253"

      assert sha256(File.read!("priv/pgflow_core/sql/versions/v01/v01_down.sql")) ==
               "628b8f798c8f5cca43765eee9492bd55cee90254736b7edad9bad08e738b6032"

      assert sha256(File.read!("priv/pgflow_core/sql/versions/v01/v01_manifest.json")) ==
               "0a639d5309dcf9b59f5759b0d175b3b72366ce0fcf3e9e9864590b8d0ad5b23f"
    end
  end

  defp sha256(content), do: :crypto.hash(:sha256, content) |> Base.encode16(case: :lower)

  describe "with PGFLOW_UPSTREAM_CHECKOUT" do
    @describetag :upstream_checkout
    @sha "baa9e43f5f9122133deacbc3e8ffc80e284b4f50"

    setup do
      %{checkout: System.fetch_env!("PGFLOW_UPSTREAM_CHECKOUT")}
    end

    test "V03 build is deterministic and matches committed output", %{checkout: checkout} do
      {:ok, a} = Bundle.build(checkout, @sha, "03")
      {:ok, b} = Bundle.build(checkout, @sha, "03")
      assert a.sql == b.sql
      assert a.sql == File.read!("priv/pgflow_core/sql/versions/v03/v03_up.sql")
    end

    test "V03 records every rule with its exact count", %{checkout: checkout} do
      {:ok, %{manifest: manifest}} = Bundle.build(checkout, @sha, "03")
      counts = Map.new(manifest["transformations"], &{&1["rule"], &1["matched"]})

      assert counts == %{
               "slug_preflight" => 1,
               "set_local_lock_timeout" => 1,
               "omit_applied_in_v02" => 11,
               "drop_if_exists" => 1,
               "replace_desc_runs_index" => 1,
               "step_task_record_dispatch" => 1,
               "telemetry_pgflow_ddl_allowlist" => 4,
               "reject_net_http_post" => 0
             }
    end

    test "V03 output never references pgflow_telemetry, cron or net", %{checkout: checkout} do
      {:ok, %{sql: sql}} = Bundle.build(checkout, @sha, "03")
      refute sql =~ "pgflow_telemetry"
      refute sql =~ ~r/\bcron\./
      refute sql =~ "net.http_post"
      assert sql =~ "SET LOCAL lock_timeout = '10s'"
      refute sql =~ ~r/^SET lock_timeout/m
    end

    test "omit_applied_in_v02 fails when a V02 counterpart is missing", %{checkout: checkout} do
      assert {:error, message} = Bundle.build(checkout, @sha, "03", v02_sql: "SELECT 1")
      assert message =~ "omit_applied_in_v02: V02 counterpart missing for"
    end
  end
end
