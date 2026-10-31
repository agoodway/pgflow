defmodule PgFlowTest do
  use ExUnit.Case, async: true

  describe "compatibility_report/0" do
    test "reports the pgflow 0.17.2 schema contract" do
      assert PgFlow.upstream_sha() == "baa9e43f5f9122133deacbc3e8ffc80e284b4f50"

      report = PgFlow.compatibility_report()

      assert report.upstream_release == "0.17.2"
      assert report.upstream_sha == PgFlow.upstream_sha()
      assert report.bundled_core_version == 3
      assert report.bundled_helpers_version == 7
      assert report.core_version_string == PgFlow.core_version()
    end
  end

  describe "child_spec/1" do
    test "returns valid child spec" do
      spec = PgFlow.child_spec(repo: FakeRepo)

      assert spec.id == PgFlow
      assert spec.type == :supervisor
      assert {PgFlow, :start_link, [[repo: FakeRepo]]} = spec.start
    end
  end
end
