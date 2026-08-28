defmodule PgFlowDashboard.ArchitectureBoundaryTest do
  use ExUnit.Case, async: true

  @dashboard_root Path.expand("../../lib/pgflow_dashboard", __DIR__)

  test "runtime dashboard code has no Dashboard query modules or schema dependencies" do
    runtime_files =
      @dashboard_root
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.reject(&String.contains?(&1, "/migrations/"))

    violations =
      for path <- runtime_files,
          source = File.read!(path),
          String.contains?(source, "PgFlowDashboard." <> "Queries") or
            String.contains?(source, "@schema_prefix " <> ~s("pgflow_dashboard")) do
        Path.relative_to_cwd(path)
      end

    assert violations == []
  end
end
