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
end
