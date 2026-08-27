defmodule PgFlow.JobCompilerTest do
  use ExUnit.Case, async: true

  alias PgFlow.Flow.{Definition, Step}
  alias PgFlow.JobCompiler

  describe "compile/1" do
    test "compiles a job definition with create_flow, add_step, and flow_type UPDATE" do
      definition = %Definition{
        slug: :send_email,
        module: TestJob,
        opts: [max_attempts: 3, base_delay: 5, timeout: 60],
        steps: [
          %Step{slug: :perform, step_type: :single, depends_on: []}
        ],
        flow_type: :job
      }

      sql_statements = JobCompiler.compile(definition)

      [flow_sql, step_sql, update_sql] = sql_statements

      assert flow_sql == "SELECT pgflow.create_flow('send_email', 3, 5, 60)"

      assert step_sql ==
               "SELECT pgflow.add_step('send_email', 'perform', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')"

      assert update_sql ==
               "UPDATE pgflow.flows SET flow_type = 'job' WHERE flow_slug = 'send_email'"
    end

    test "uses default options" do
      definition = %Definition{
        slug: :basic_job,
        module: TestJob,
        opts: [],
        steps: [
          %Step{slug: :perform, step_type: :single, depends_on: []}
        ],
        flow_type: :job
      }

      sql_statements = JobCompiler.compile(definition)

      [flow_sql, _step_sql, update_sql] = sql_statements

      assert flow_sql == "SELECT pgflow.create_flow('basic_job', 3, 1, 60)"

      assert update_sql ==
               "UPDATE pgflow.flows SET flow_type = 'job' WHERE flow_slug = 'basic_job'"
    end

    test "integrates with a real job module definition" do
      defmodule CompilerTestJob do
        use PgFlow.Job

        @job slug: :compiler_test_job, max_attempts: 2, base_delay: 3, timeout: 45

        perform do
          fn input, _ctx -> %{done: true, input: input} end
        end
      end

      definition = CompilerTestJob.__pgflow_definition__()
      sql_statements = JobCompiler.compile(definition)

      [flow_sql, step_sql, update_sql] = sql_statements

      assert flow_sql =~ "create_flow('compiler_test_job'"
      assert step_sql =~ "add_step('compiler_test_job', 'compiler_test_job'"
      assert update_sql =~ "flow_type = 'job'"
      assert update_sql =~ "flow_slug = 'compiler_test_job'"
    end

    test "includes cron SQL for job with cron option" do
      defmodule CronTestJob do
        use PgFlow.Job

        @job slug: :cron_test_job, cron: [schedule: "*/10 * * * *", input: %{scheduled: true}]

        perform do
          fn _input, _ctx -> :ok end
        end
      end

      definition = CronTestJob.__pgflow_definition__()
      sql_statements = JobCompiler.compile(definition)

      [_, _, cron_sql, update_sql] = sql_statements

      assert cron_sql =~ "cron.schedule"
      assert cron_sql =~ "'pgflow:cron_test_job'"
      assert cron_sql =~ "'*/10 * * * *'"
      assert cron_sql =~ ~s("scheduled":true)

      assert update_sql =~ "flow_type = 'job'"
    end

    test "has_cron?/1 delegates to FlowCompiler" do
      defmodule HasCronTestJob do
        use PgFlow.Job

        @job slug: :has_cron_test_job, cron: [schedule: "0 0 * * *"]

        perform do
          fn _input, _ctx -> :ok end
        end
      end

      assert JobCompiler.has_cron?(HasCronTestJob)
    end

    test "cron_unschedule_sql/1 delegates to FlowCompiler" do
      sql = JobCompiler.cron_unschedule_sql(:my_job)
      assert sql == "SELECT cron.unschedule('pgflow:my_job')"
    end
  end
end
