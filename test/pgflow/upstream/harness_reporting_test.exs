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

  test "pgflow_ddl_only keeps only pgflow-schema DDL from the telemetry migration" do
    sql = """
    CREATE SCHEMA "pgflow_telemetry";
    CREATE INDEX "idx_runs_started_at" ON "pgflow"."runs" ("started_at");
    ALTER TABLE "pgflow"."workers" ADD COLUMN "pgflow_version" text NULL;
    CREATE FUNCTION "pgflow_telemetry"."bucket_count" () RETURNS int LANGUAGE sql AS $$ select 1 from pgflow.runs $$;
    SELECT cron.schedule('x', '* * * * *', 'select 1');
    """

    assert UpstreamHarness.pgflow_ddl_only_sql(sql) ==
             ~s|CREATE INDEX "idx_runs_started_at" ON "pgflow"."runs" ("started_at");\n| <>
               ~s|ALTER TABLE "pgflow"."workers" ADD COLUMN "pgflow_version" text NULL;\n|
  end

  test "manifest targets the pgflow 0.17.2 tag and catalog" do
    target = UpstreamHarness.manifest()["target"]
    assert target["upstream_sha"] == "baa9e43f5f9122133deacbc3e8ffc80e284b4f50"
    assert target["upstream_release"] == "0.17.2"
    assert {target["core_version"], target["helpers_version"]} == {3, 7}
    assert target["claim_signature"] == "pgflow.start_tasks(text,bigint[],uuid,text,text)"
    assert target["compile_signature"] == "pgflow.ensure_flow_compiled(text,jsonb,text,jsonb)"
  end
end
