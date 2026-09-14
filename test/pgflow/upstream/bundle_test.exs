defmodule PgFlow.Upstream.BundleTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Pgflow.SyncUpstream
  alias PgFlow.Upstream.Bundle

  @sha "94490709f79ebf366141dd925b047f0c1013e759"
  @upstream_path "pkgs/core/supabase/migrations"
  @fixture_sources Path.expand("../../support/upstream/sources", __DIR__)
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

  describe "portable rejection checks" do
    test "rejects checkout when HEAD differs from requested SHA even if source bytes match" do
      checkout = create_checkout!()

      assert {:error, message} = Bundle.build(checkout, @sha)
      assert message =~ "checkout HEAD"
      assert message =~ "does not match requested SHA #{@sha}"
      refute message =~ "does not match pinned #{@sha} bytes"
    end

    test "rejects every SHA except the authoritative pin" do
      checkout = create_checkout!()

      assert {:error, message} = Bundle.build(checkout, String.duplicate("0", 40))
      assert message =~ "refusing unpinned upstream SHA"
      assert message =~ @sha
    end
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

  describe "with PGFLOW_UPSTREAM_CHECKOUT" do
    @describetag :upstream_checkout
    setup do
      [checkout: System.fetch_env!("PGFLOW_UPSTREAM_CHECKOUT")]
    end

    test "rejects dirty source bytes even when checkout HEAD matches the pin", %{
      checkout: checkout
    } do
      path = Path.join([checkout, @upstream_path, List.first(@files)])
      original = File.read!(path)
      File.write!(path, original <> "\n-- dirty\n")

      on_exit(fn -> File.write!(path, original) end)

      assert {:error, message} = Bundle.build(checkout, @sha)
      assert message =~ "does not match pinned #{@sha} bytes"
      assert message =~ Map.fetch!(@source_hashes, List.first(@files))
    end

    test "builds the pinned delta deterministically with complete provenance", %{
      checkout: checkout
    } do
      assert {:ok, first} = Bundle.build(checkout, @sha)
      assert {:ok, second} = Bundle.build(checkout, @sha)
      assert first == second

      assert first.manifest["upstream_sha"] == @sha
      assert first.manifest["files"] == @files
      assert first.manifest["source_sha256"] == @source_hashes
      assert first.manifest["output_sha256"] == sha256(first.sql)

      assert first.manifest["statement_count"] ==
               length(String.split(first.sql, "\n\n--SPLIT--\n\n"))

      refute first.sql =~ "net.http_post"
      refute first.sql =~ ~r/^SET lock_timeout\b/m
      assert first.sql =~ "SET LOCAL lock_timeout = '10s'"
    end

    test "committed V02 output matches the deterministic builder", %{checkout: checkout} do
      assert {:ok, bundle} = Bundle.build(checkout, @sha)
      version_dir = Path.join(File.cwd!(), "priv/pgflow_core/sql/versions/v02")

      assert File.read!(Path.join(version_dir, "v02_up.sql")) == bundle.sql

      assert Path.join(version_dir, "v02_manifest.json")
             |> File.read!()
             |> Jason.decode!() == bundle.manifest
    end

    test "sync task checks committed output without rewriting it", %{checkout: checkout} do
      up_path = "priv/pgflow_core/sql/versions/v02/v02_up.sql"
      before = File.stat!(up_path).mtime
      Mix.Task.reenable("pgflow.sync_upstream")

      SyncUpstream.run([
        "--checkout",
        checkout,
        "--sha",
        @sha,
        "--version",
        "02",
        "--check"
      ])

      assert File.stat!(up_path).mtime == before
    end

    test "sync task refuses to rewrite published V01", %{checkout: checkout} do
      Mix.Task.reenable("pgflow.sync_upstream")

      assert_raise Mix.Error, ~r/refusing to rewrite published core version V01/, fn ->
        SyncUpstream.run([
          "--checkout",
          checkout,
          "--sha",
          @sha,
          "--version",
          "01"
        ])
      end
    end
  end

  defp create_checkout! do
    checkout =
      Path.join(System.tmp_dir!(), "pgflow-upstream-#{System.unique_integer([:positive])}")

    migrations = Path.join(checkout, @upstream_path)
    File.mkdir_p!(migrations)

    Enum.each(@files, fn file ->
      File.cp!(Path.join(@fixture_sources, file), Path.join(migrations, file))
    end)

    git!(checkout, ["init", "--quiet"])
    git!(checkout, ["add", "."])

    git!(checkout, [
      "-c",
      "user.name=PgFlow Test",
      "-c",
      "user.email=pgflow@example.invalid",
      "commit",
      "--quiet",
      "-m",
      "Pinned #{@sha} source fixture"
    ])

    on_exit(fn -> File.rm_rf!(checkout) end)
    checkout
  end

  defp git!(checkout, args) do
    case System.cmd("git", ["-C", checkout | args], stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {output, status} -> flunk("git exited #{status}: #{output}")
    end
  end

  defp sha256(content), do: :crypto.hash(:sha256, content) |> Base.encode16(case: :lower)
end
