defmodule Mix.Tasks.Pgflow.AuditSlugsTest do
  use PgFlow.DataCase, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Pgflow.AuditSlugs
  alias PgFlow.TestRepo

  test "reports no slug findings on a clean database" do
    output =
      capture_io(fn ->
        AuditSlugs.run(["--repo", "PgFlow.TestRepo"])
      end)

    assert output =~ "no slug findings"
  end

  test "raises when a slug violates the 0.17.2 rules" do
    TestRepo.query!("ALTER TABLE pgflow.flows DROP CONSTRAINT slug_is_valid")
    TestRepo.query!("INSERT INTO pgflow.flows (flow_slug) VALUES ('bad__flow')")

    assert_raise Mix.Error, ~r/1 slug finding/, fn ->
      capture_io(fn ->
        AuditSlugs.run(["--repo", "PgFlow.TestRepo"])
      end)
    end
  end
end
