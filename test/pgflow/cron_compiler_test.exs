defmodule PgFlow.CronCompilerTest do
  use ExUnit.Case, async: true

  alias PgFlow.Flow.{Definition, Step}
  alias PgFlow.CronCompiler

  describe "compile/3" do
    test "compiles a cron definition with create_flow, add_step, flow_type UPDATE, and cron.schedule" do
      definition = %Definition{
        slug: :daily_report,
        module: TestCron,
        opts: [max_attempts: 3, base_delay: 5, timeout: 60],
        steps: [
          %Step{slug: :perform, step_type: :single, depends_on: []}
        ],
        flow_type: :cron
      }

      sql_statements = CronCompiler.compile(definition, "0 9 * * *", %{})

      assert length(sql_statements) == 4

      [flow_sql, step_sql, update_sql, schedule_sql] = sql_statements

      assert flow_sql == "SELECT pgflow.create_flow('daily_report', 3, 5, 60)"

      assert step_sql ==
               "SELECT pgflow.add_step('daily_report', 'perform', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')"

      assert update_sql ==
               "UPDATE pgflow.flows SET flow_type = 'cron' WHERE flow_slug = 'daily_report'"

      assert schedule_sql =~ "cron.schedule('pgflow:daily_report'"
      assert schedule_sql =~ "'0 9 * * *'"
      assert schedule_sql =~ "pgflow.start_flow('daily_report'"
    end

    test "uses default options" do
      definition = %Definition{
        slug: :basic_cron,
        module: TestCron,
        opts: [],
        steps: [
          %Step{slug: :perform, step_type: :single, depends_on: []}
        ],
        flow_type: :cron
      }

      sql_statements = CronCompiler.compile(definition, "*/5 * * * *")

      [flow_sql, _step_sql, update_sql, schedule_sql] = sql_statements

      assert flow_sql == "SELECT pgflow.create_flow('basic_cron', 3, 1, 60)"

      assert update_sql ==
               "UPDATE pgflow.flows SET flow_type = 'cron' WHERE flow_slug = 'basic_cron'"

      assert schedule_sql =~ "'*/5 * * * *'"
    end

    test "embeds static input as JSON in cron.schedule SQL" do
      definition = %Definition{
        slug: :report_cron,
        module: TestCron,
        opts: [max_attempts: 1, base_delay: 1, timeout: 30],
        steps: [
          %Step{slug: :perform, step_type: :single, depends_on: []}
        ],
        flow_type: :cron
      }

      input = %{"report_type" => "daily", "format" => "pdf"}
      sql_statements = CronCompiler.compile(definition, "0 9 * * *", input)

      schedule_sql = List.last(sql_statements)

      assert schedule_sql =~ "pgflow.start_flow('report_cron'"
      assert schedule_sql =~ "::jsonb"
      # JSON should contain the input keys
      assert schedule_sql =~ "report_type"
      assert schedule_sql =~ "daily"
      assert schedule_sql =~ "format"
      assert schedule_sql =~ "pdf"
    end

    test "empty input generates empty JSON object" do
      definition = %Definition{
        slug: :empty_input_cron,
        module: TestCron,
        opts: [max_attempts: 1, base_delay: 1, timeout: 30],
        steps: [
          %Step{slug: :perform, step_type: :single, depends_on: []}
        ],
        flow_type: :cron
      }

      sql_statements = CronCompiler.compile(definition, "0 9 * * *", %{})

      schedule_sql = List.last(sql_statements)

      assert schedule_sql =~ "'{}'"
      assert schedule_sql =~ "::jsonb"
    end

    test "nested JSON input is embedded correctly" do
      definition = %Definition{
        slug: :nested_input_cron,
        module: TestCron,
        opts: [max_attempts: 1, base_delay: 1, timeout: 30],
        steps: [
          %Step{slug: :perform, step_type: :single, depends_on: []}
        ],
        flow_type: :cron
      }

      input = %{"config" => %{"nested" => true, "level" => 2}}
      sql_statements = CronCompiler.compile(definition, "0 9 * * *", input)

      schedule_sql = List.last(sql_statements)

      assert schedule_sql =~ "config"
      assert schedule_sql =~ "nested"
      assert schedule_sql =~ "::jsonb"
    end

    test "uses dollar-quoting for inner SQL" do
      definition = %Definition{
        slug: :dollar_cron,
        module: TestCron,
        opts: [max_attempts: 1, base_delay: 1, timeout: 30],
        steps: [
          %Step{slug: :perform, step_type: :single, depends_on: []}
        ],
        flow_type: :cron
      }

      sql_statements = CronCompiler.compile(definition, "0 9 * * *")

      schedule_sql = List.last(sql_statements)

      # Uses named dollar-quoting ($pgflow$) to avoid conflicts with nested dollar-quoted strings
      assert schedule_sql =~ "$pgflow$SELECT pgflow.start_flow("
      assert schedule_sql =~ ")$pgflow$)"
    end

    test "integrates with a real cron module definition" do
      defmodule CompilerTestCron do
        use PgFlow.Cron

        @cron queue: :compiler_test_cron,
              expression: "*/15 9-17 * * 1-5",
              max_attempts: 2,
              base_delay: 3,
              timeout: 45,
              input: %{"key" => "value"}

        schedule do
          fn input, _ctx -> %{done: true, input: input} end
        end
      end

      definition = CompilerTestCron.__pgflow_definition__()
      expression = CompilerTestCron.__pgflow_cron_expression__()
      input = CompilerTestCron.__pgflow_cron_input__()
      sql_statements = CronCompiler.compile(definition, expression, input)

      assert length(sql_statements) == 4

      [flow_sql, step_sql, update_sql, schedule_sql] = sql_statements

      assert flow_sql =~ "create_flow('compiler_test_cron'"
      assert step_sql =~ "add_step('compiler_test_cron', 'perform'"
      assert update_sql =~ "flow_type = 'cron'"
      assert update_sql =~ "flow_slug = 'compiler_test_cron'"
      assert schedule_sql =~ "pgflow:compiler_test_cron"
      assert schedule_sql =~ "*/15 9-17 * * 1-5"
      assert schedule_sql =~ "key"
      assert schedule_sql =~ "value"
    end
  end

  describe "unschedule_sql/1" do
    test "returns correct unschedule SQL" do
      assert CronCompiler.unschedule_sql("daily_report") ==
               "SELECT cron.unschedule('pgflow:daily_report')"
    end

    test "uses pgflow: prefix in job name" do
      sql = CronCompiler.unschedule_sql("my_cron")
      assert sql =~ "pgflow:my_cron"
    end
  end
end
