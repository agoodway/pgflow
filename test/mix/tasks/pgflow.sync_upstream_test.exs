defmodule Mix.Tasks.Pgflow.SyncUpstreamTest do
  use ExUnit.Case, async: false
  alias Mix.Tasks.Pgflow.SyncUpstream

  defmodule ConsumerProject do
    def project, do: [app: :pgflow_consumer, version: "0.1.0", elixirc_paths: []]
  end

  test "refuses to write generated migrations in a consuming project" do
    Mix.Project.push(ConsumerProject)

    try do
      assert_raise Mix.Error, ~r/only be run from the pgflow project/, fn ->
        SyncUpstream.run([])
      end
    after
      Mix.Project.pop()
    end
  end

  test "refuses to rewrite published core version V01" do
    Mix.Task.reenable("pgflow.sync_upstream")

    assert_raise Mix.Error, ~r/refusing to rewrite published core version V01/, fn ->
      SyncUpstream.run([
        "--checkout",
        "/nonexistent",
        "--sha",
        "deadbeef",
        "--version",
        "01"
      ])
    end
  end

  test "refuses to rewrite published core version V02" do
    Mix.Task.reenable("pgflow.sync_upstream")

    assert_raise Mix.Error, ~r/refusing to rewrite published core version V02/, fn ->
      SyncUpstream.run([
        "--checkout",
        "/nonexistent",
        "--sha",
        "deadbeef",
        "--version",
        "02"
      ])
    end
  end

  test "rejects an unsupported version and names V03" do
    Mix.Task.reenable("pgflow.sync_upstream")

    assert_raise Mix.Error, ~r/unsupported core delta version "09"; expected 03/, fn ->
      SyncUpstream.run([
        "--checkout",
        "/nonexistent",
        "--sha",
        "deadbeef",
        "--version",
        "09"
      ])
    end
  end
end
