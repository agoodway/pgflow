defmodule PgFlow.Upgrade.SlugAuditTest do
  use PgFlow.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.TestRepo
  alias PgFlow.Upgrade.SlugAudit

  test "reports every rule violation with a reason" do
    seed_offenders!()

    findings = SlugAudit.run(TestRepo, prefix: "pgflow")

    assert %{kind: :flow, flow_slug: "bad__flow", reason: :double_underscore} in findings

    assert %{kind: :flow, flow_slug: String.duplicate("a", 48), reason: :queue_name_too_long} in findings

    assert %{
             kind: :step_collision,
             flow_slug: "ok_flow",
             step_slug: "Fetch,fetch",
             reason: :case_insensitive_duplicate
           } in findings

    refute Enum.any?(findings, &(&1.flow_slug == "ok_flow" and &1.kind == :flow))
  end

  test "SQL rule agrees with PgFlow.Slug on the 0.17.2 matrix" do
    for slug <- [
          "a",
          "Run",
          "run",
          "a__b",
          "_a",
          "a_",
          "1a",
          "has-dash",
          String.duplicate("a", 129)
        ] do
      # $1::text gives PostgreSQL a type. A bare $1 is ambiguous_parameter (42P08)
      # once the expression mixes comparisons and regex operators.
      %{rows: [[db_valid]]} =
        SQL.query!(TestRepo, "SELECT #{SlugAudit.rule_sql("$1::text")}", [slug])

      assert db_valid == PgFlow.Slug.valid?(slug), "disagreement on #{inspect(slug)}"
    end
  end

  test "preflight_sql raises check_violation listing offenders" do
    seed_offenders!()

    assert_raise Postgrex.Error, ~r/check_violation|23514.*bad__flow/s, fn ->
      SQL.query!(TestRepo, SlugAudit.preflight_sql("pgflow"), [])
    end
  end

  test "run/2 executes in a READ ONLY transaction" do
    assert SlugAudit.read_only_statement() == "SET TRANSACTION READ ONLY"

    Sandbox.unboxed_run(TestRepo, fn ->
      assert SlugAudit.run(TestRepo, prefix: "pgflow") == []

      assert_raise Postgrex.Error, ~r/read-only|25006/, fn ->
        TestRepo.transaction(fn ->
          TestRepo.query!(SlugAudit.read_only_statement())
          TestRepo.query!("INSERT INTO pgflow.flows (flow_slug) VALUES ('read_only_probe')")
        end)
      end
    end)
  end

  defp seed_offenders! do
    TestRepo.query!("ALTER TABLE pgflow.flows DROP CONSTRAINT slug_is_valid")
    TestRepo.query!("ALTER TABLE pgflow.steps DROP CONSTRAINT steps_step_slug_check")
    TestRepo.query!("INSERT INTO pgflow.flows (flow_slug) VALUES ('ok_flow')")
    TestRepo.query!("INSERT INTO pgflow.flows (flow_slug) VALUES ('bad__flow')")

    TestRepo.query!("INSERT INTO pgflow.flows (flow_slug) VALUES ($1)", [
      String.duplicate("a", 48)
    ])

    TestRepo.query!("""
    INSERT INTO pgflow.steps (flow_slug, step_slug, step_index, queue_name)
    VALUES ('ok_flow', 'fetch', 0, 'ok_flow')
    """)

    TestRepo.query!("""
    INSERT INTO pgflow.steps (flow_slug, step_slug, step_index, queue_name)
    VALUES ('ok_flow', 'Fetch', 1, 'ok_flow')
    """)
  end
end
