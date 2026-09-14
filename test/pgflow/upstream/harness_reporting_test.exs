defmodule PgFlow.Upstream.HarnessReportingTest do
  use ExUnit.Case, async: true

  alias PgFlow.Test.UpstreamHarness

  test "TAP failures fail even when psql exits zero" do
    assert %{tests: 2, failures: 1} =
             UpstreamHarness.tap_result("1..2\nok 1 - first\nnot ok 2 - broken\n", 0)
  end

  test "empty output and truncated plans cannot pass" do
    assert %{tests: 0, failures: 1} = UpstreamHarness.tap_result("", 0)
    assert %{tests: 1, failures: 1} = UpstreamHarness.tap_result("1..2\nok 1\n", 0)
  end

  test "counts assertions rather than files" do
    assert %{tests: 2, failures: 0} =
             UpstreamHarness.tap_result(" 1..2\n ok 1 - first\n ok 2 - second\n", 0)
  end

  test "process errors and TAP diagnostics cannot pass" do
    assert %{failures: 1} = UpstreamHarness.tap_result("1..1\nok 1\n", 1)

    assert %{failures: 1} =
             UpstreamHarness.tap_result("1..1\nok 1\n# Looks like you failed 1 test\n", 0)
  end
end
