defmodule PgFlow.DefinitionsTest do
  use PgFlow.IntegrationCase

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.{CronSummary, Definitions, DefinitionSummary}
  alias PgFlow.Schema.{Dep, Step}

  describe "stored flow and step definitions" do
    test "returns typed summaries, complete steps, and explicit dependency rows" do
      create_flow("definitions_test_flow")

      TestRepo.query!("""
      UPDATE pgflow.flows
      SET opt_max_attempts = 7, opt_base_delay = 4, opt_timeout = 90
      WHERE flow_slug = 'definitions_test_flow'
      """)

      add_step_with_retry_options("definitions_test_flow", "prepare",
        max_attempts: 2,
        base_delay: 3,
        timeout: 45,
        start_delay: 8
      )

      add_conditional_step("definitions_test_flow", "publish",
        deps: ["prepare"],
        if: %{"approved" => true},
        if_not: %{"blocked" => true},
        when_unmet: "skip-cascade",
        when_exhausted: "skip",
        max_attempts: 5
      )

      assert {:ok,
              %DefinitionSummary{
                flow_slug: "definitions_test_flow",
                flow_type: "flow",
                opt_max_attempts: 7,
                opt_base_delay: 4,
                opt_timeout: 90,
                step_count: 2
              }} = Definitions.get_flow(TestRepo, "definitions_test_flow")

      assert {:ok, [%Step{step_slug: "prepare"}, %Step{step_slug: "publish"}]} =
               Definitions.list_steps(TestRepo, "definitions_test_flow")

      assert {:ok,
              %Step{
                step_slug: "prepare",
                step_type: "single",
                step_index: 0,
                deps_count: 0,
                opt_max_attempts: 2,
                opt_base_delay: 3,
                opt_timeout: 45,
                opt_start_delay: 8,
                required_input_pattern: nil,
                forbidden_input_pattern: nil,
                when_unmet: "skip",
                when_exhausted: "fail"
              }} = Definitions.get_step(TestRepo, "definitions_test_flow", "prepare")

      assert {:ok,
              %Step{
                step_slug: "publish",
                deps_count: 1,
                required_input_pattern: %{"approved" => true},
                forbidden_input_pattern: %{"blocked" => true},
                when_unmet: "skip-cascade",
                when_exhausted: "skip"
              }} = Definitions.get_step(TestRepo, "definitions_test_flow", "publish")

      assert {:ok,
              [
                %Dep{
                  flow_slug: "definitions_test_flow",
                  step_slug: "publish",
                  dep_slug: "prepare"
                }
              ]} = Definitions.list_deps(TestRepo, "definitions_test_flow")

      assert {:error, :not_found} =
               Definitions.get_step(TestRepo, "definitions_test_flow", "missing")

      assert {:error, :not_found} = Definitions.get_flow(TestRepo, "missing")
    end
  end

  describe "flow and job summaries" do
    test "filters by definition type and calculates only explicit 24-hour statistics" do
      create_flow("definitions_test_recent_flow")
      add_step("definitions_test_recent_flow", "work")
      create_flow("definitions_test_job")
      add_step("definitions_test_job", "perform")

      TestRepo.query!(
        "UPDATE pgflow.flows SET flow_type = 'job' WHERE flow_slug = 'definitions_test_job'"
      )

      completed_id = start_flow_run("definitions_test_recent_flow", %{})
      failed_id = start_flow_run("definitions_test_recent_flow", %{})
      old_id = start_flow_run("definitions_test_recent_flow", %{})

      terminalize_run(completed_id, "completed", 10, 2)
      terminalize_run(failed_id, "failed", 5, 1)
      terminalize_run(old_id, "completed", 30, 25)

      assert {:ok,
              [
                %DefinitionSummary{
                  flow_slug: "definitions_test_recent_flow",
                  flow_type: "flow",
                  total_runs_24h: 2,
                  completed_runs_24h: 1,
                  failed_runs_24h: 1,
                  success_rate_24h: success_rate,
                  avg_duration_ms: avg_duration,
                  p95_duration_ms: p95_duration,
                  step_count: 1
                }
              ]} = Definitions.list_flows(TestRepo)

      assert Decimal.equal?(success_rate, Decimal.new("50.0"))
      assert Decimal.equal?(avg_duration, Decimal.new(10_000))
      assert_in_delta p95_duration, 10_000, 1

      assert {:ok, [%DefinitionSummary{flow_slug: "definitions_test_job", flow_type: "job"}]} =
               Definitions.list_jobs(TestRepo)

      assert {:ok, 1} = Definitions.count_flows(TestRepo)
      assert {:ok, 1} = Definitions.count_jobs(TestRepo)
      assert {:error, :not_found} = Definitions.get_flow(TestRepo, "definitions_test_job")
      assert {:error, :not_found} = Definitions.get_job(TestRepo, "definitions_test_recent_flow")
    end

    test "paginates definitions deterministically before calculating their aggregates" do
      Enum.each(["a", "b", "c"], fn suffix ->
        create_flow("definitions_test_page_#{suffix}")
        add_step("definitions_test_page_#{suffix}", "work")
      end)

      assert {:ok, [first, second]} = Definitions.list_flows(TestRepo, limit: 2)

      assert Enum.map([first, second], & &1.flow_slug) == [
               "definitions_test_page_a",
               "definitions_test_page_b"
             ]

      assert {:ok, [%DefinitionSummary{flow_slug: "definitions_test_page_c"}]} =
               Definitions.list_flows(TestRepo, cursor: second.flow_slug, limit: 2)
    end
  end

  describe "cron summaries" do
    setup do
      previous_database = Calendar.get_time_zone_database()
      Calendar.put_time_zone_database(PgFlow.Test.FixedTimeZoneDatabase)
      on_exit(fn -> Calendar.put_time_zone_database(previous_database) end)

      TestRepo.query!("""
      SELECT cron.unschedule(jobname)
      FROM cron.job
      WHERE jobname LIKE 'pgflow:definitions_test_%'
      """)

      on_exit(fn ->
        Sandbox.mode(TestRepo, :auto)

        try do
          TestRepo.query!("DELETE FROM cron.job WHERE jobname LIKE 'pgflow:definitions_test_%'")
        after
          Sandbox.mode(TestRepo, :manual)
        end
      end)

      :ok
    end

    test "joins stored schedules and returns typed last and next run data" do
      create_flow("definitions_test_cron")
      add_step("definitions_test_cron", "work")
      schedule_cron("definitions_test_cron", "*/15 * * * *", active: false)

      run_id = start_flow_run("definitions_test_cron", %{})
      terminalize_run(run_id, "completed", 12, 1)

      assert {:ok,
              %CronSummary{
                flow_slug: "definitions_test_cron",
                flow_type: "flow",
                cron_expression: "*/15 * * * *",
                is_active: false,
                total_runs_24h: 1,
                completed_runs_24h: 1,
                failed_runs_24h: 0,
                last_run_status: "completed",
                last_run_at: %DateTime{},
                next_run_at: %DateTime{}
              }} = Definitions.get_cron(TestRepo, "definitions_test_cron")

      assert {:ok, [%CronSummary{flow_slug: "definitions_test_cron"}]} =
               Definitions.list_crons(TestRepo, limit: 1)

      assert {:ok, 1} = Definitions.count_crons(TestRepo)
      assert {:error, :not_found} = Definitions.get_cron(TestRepo, "missing")
    end

    test "uses cron.timezone for the next run and safely ignores unsupported schedules" do
      create_flow("definitions_test_timezone")
      create_flow("definitions_test_reboot")
      create_flow("definitions_test_invalid")
      schedule_cron("definitions_test_timezone", "0 8 * * *")
      schedule_cron("definitions_test_reboot", "@reboot")
      schedule_cron("definitions_test_invalid", "0 * * * *")

      TestRepo.query!(
        "UPDATE cron.job SET schedule = 'not-a-cron' WHERE jobname = 'pgflow:definitions_test_invalid'"
      )

      assert {:ok, %CronSummary{next_run_at: %DateTime{} = next_run_at}} =
               Definitions.get_cron(TestRepo, "definitions_test_timezone")

      assert next_run_at.time_zone == "Etc/UTC"

      assert {:ok, local_next_run} =
               DateTime.shift_zone(
                 next_run_at,
                 "America/New_York",
                 PgFlow.Test.FixedTimeZoneDatabase
               )

      assert local_next_run.hour == 8

      assert {:ok, %CronSummary{next_run_at: nil}} =
               Definitions.get_cron(TestRepo, "definitions_test_reboot")

      assert {:ok, %CronSummary{next_run_at: nil}} =
               Definitions.get_cron(TestRepo, "definitions_test_invalid")

      Calendar.put_time_zone_database(Calendar.UTCOnlyTimeZoneDatabase)

      assert {:ok, %CronSummary{next_run_at: nil}} =
               Definitions.get_cron(TestRepo, "definitions_test_timezone")

      Calendar.put_time_zone_database(PgFlow.Test.FixedTimeZoneDatabase)
    end

    test "keeps exact lookup and cursor pagination isolated from neighboring cron rows" do
      Enum.each(Enum.with_index(["a", "b", "c"]), fn {suffix, minute} ->
        flow_slug = "definitions_test_cron_#{suffix}"
        create_flow(flow_slug)
        schedule_cron(flow_slug, "#{minute} * * * *")
      end)

      assert {:ok, %CronSummary{flow_slug: "definitions_test_cron_b"}} =
               Definitions.get_cron(TestRepo, "definitions_test_cron_b")

      assert {:ok, [first, second]} = Definitions.list_crons(TestRepo, limit: 2)

      assert Enum.map([first, second], & &1.flow_slug) == [
               "definitions_test_cron_a",
               "definitions_test_cron_b"
             ]

      assert {:ok, [%CronSummary{flow_slug: "definitions_test_cron_c"}]} =
               Definitions.list_crons(TestRepo, cursor: second.flow_slug, limit: 2)
    end

    test "preserves completion-time semantics for the most recent cron run" do
      Enum.each(["completed", "failed", "started"], fn status ->
        flow_slug = "definitions_test_#{status}_cron"
        create_flow(flow_slug)
        add_step(flow_slug, "work")
        schedule_cron(flow_slug, "0 * * * *")
      end)

      completed_run_id = start_flow_run("definitions_test_completed_cron", %{})
      failed_run_id = start_flow_run("definitions_test_failed_cron", %{})
      started_run_id = start_flow_run("definitions_test_started_cron", %{})

      terminalize_run(completed_run_id, "completed", 10, 3)
      terminalize_run(failed_run_id, "failed", 10, 2)
      set_run_started_at(started_run_id, 1)

      assert {:ok, %CronSummary{last_run_at: %DateTime{}, last_run_status: "completed"}} =
               Definitions.get_cron(TestRepo, "definitions_test_completed_cron")

      Enum.each(["failed", "started"], fn status ->
        assert {:ok, %CronSummary{last_run_at: nil, last_run_status: ^status}} =
                 Definitions.get_cron(TestRepo, "definitions_test_#{status}_cron")
      end)
    end
  end

  describe "unschedule/2" do
    test "unschedules present definitions and is idempotent when the schedule is absent" do
      create_flow("definitions_test_unschedule")
      schedule_cron("definitions_test_unschedule", "0 * * * *")

      assert :ok = Definitions.unschedule(TestRepo, "definitions_test_unschedule")
      assert {:error, :not_found} = Definitions.get_cron(TestRepo, "definitions_test_unschedule")
      assert :ok = Definitions.unschedule(TestRepo, "definitions_test_unschedule")
    end

    test "unschedules a dangling PgFlow schedule without requiring a stored definition" do
      schedule_cron("definitions_test_dangling", "0 * * * *")

      assert :ok = Definitions.unschedule(TestRepo, "definitions_test_dangling")

      assert %{rows: [[0]]} =
               TestRepo.query!("SELECT count(*) FROM cron.job WHERE jobname = $1", [
                 "pgflow:definitions_test_dangling"
               ])
    end

    test "reports another role's surviving same-named job to a superuser as :not_owned" do
      role = "definitions_test_cron_owner_#{System.unique_integer([:positive])}"
      job_name = "pgflow:definitions_test_owned_schedule"

      TestRepo.query!("CREATE ROLE #{role}")
      TestRepo.query!("GRANT USAGE ON SCHEMA cron TO #{role}")

      try do
        TestRepo.transaction(fn ->
          TestRepo.query!("SET LOCAL ROLE #{role}")
          TestRepo.query!("SELECT cron.schedule($1, $2, 'SELECT 1')", [job_name, "0 * * * *"])
          TestRepo.query!("RESET ROLE")
          schedule_cron("definitions_test_owned_schedule", "0 * * * *")

          assert {:error, :not_owned} =
                   Definitions.unschedule(TestRepo, "definitions_test_owned_schedule")

          assert %{rows: [[^role]]} =
                   TestRepo.query!("SELECT username FROM cron.job WHERE jobname = $1", [job_name])

          TestRepo.query!("SET LOCAL ROLE #{role}")
          TestRepo.query!("SELECT cron.unschedule($1::text)", [job_name])
          TestRepo.query!("RESET ROLE")
        end)
      after
        TestRepo.query!("REVOKE USAGE ON SCHEMA cron FROM #{role}")
        TestRepo.query!("DROP ROLE #{role}")
      end

      assert %{rows: [[0]]} =
               TestRepo.query!("SELECT count(*) FROM cron.job WHERE jobname = $1", [job_name])

      assert %{rows: [[false]]} =
               TestRepo.query!("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)", [role])
    end

    test "rejects invalid slugs and returns tagged database errors" do
      assert {:error, :invalid_flow_slug} = Definitions.unschedule(TestRepo, "invalid-slug")
      assert {:error, :invalid_flow_slug} = Definitions.unschedule(TestRepo, 123)

      TestRepo.query!("ALTER TABLE cron.job RENAME TO job_unavailable")

      try do
        assert {:error, %Postgrex.Error{}} =
                 Definitions.unschedule(TestRepo, "definitions_test_database_error")
      after
        TestRepo.query!("ALTER TABLE cron.job_unavailable RENAME TO job")
      end
    end
  end

  defp schedule_cron(flow_slug, schedule, opts \\ []) do
    job_name = "pgflow:#{flow_slug}"

    TestRepo.query!("SELECT cron.schedule($1, $2, 'SELECT 1')", [job_name, schedule])
    set_cron_active(job_name, Keyword.get(opts, :active, true))
  end

  defp set_cron_active(_job_name, true), do: :ok

  defp set_cron_active(job_name, false) do
    TestRepo.query!("UPDATE cron.job SET active = false WHERE jobname = $1", [job_name])
    :ok
  end

  defp set_run_started_at(run_id, hours_ago) do
    TestRepo.query!(
      "UPDATE pgflow.runs SET started_at = now() - ($2 * interval '1 hour') WHERE run_id = $1",
      [Ecto.UUID.dump!(run_id), hours_ago]
    )
  end

  defp terminalize_run(run_id, "completed", duration_seconds, hours_ago) do
    TestRepo.query!(
      """
      UPDATE pgflow.runs
      SET status = 'completed', remaining_steps = 0, failed_at = NULL,
          started_at = now() - ($3 * interval '1 hour'),
          completed_at = now() - ($3 * interval '1 hour') + ($2 * interval '1 second')
      WHERE run_id = $1
      """,
      [Ecto.UUID.dump!(run_id), duration_seconds, hours_ago]
    )
  end

  defp terminalize_run(run_id, "failed", duration_seconds, hours_ago) do
    TestRepo.query!(
      """
      UPDATE pgflow.runs
      SET status = 'failed', remaining_steps = 0, completed_at = NULL,
          started_at = now() - ($3 * interval '1 hour'),
          failed_at = now() - ($3 * interval '1 hour') + ($2 * interval '1 second')
      WHERE run_id = $1
      """,
      [Ecto.UUID.dump!(run_id), duration_seconds, hours_ago]
    )
  end
end
