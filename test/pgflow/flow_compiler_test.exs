defmodule PgFlow.FlowCompilerTest do
  use ExUnit.Case, async: true

  alias PgFlow.Flow.{Definition, Step}
  alias PgFlow.FlowCompiler
  alias PgFlow.TestFlows.{LinearFlow, MapFlow, ParallelFlow, SimpleFlow}

  describe "compile/1" do
    test "compiles a simple flow with one step" do
      definition = %Definition{
        slug: :simple_flow,
        module: TestFlow,
        opts: [max_attempts: 3, base_delay: 5, timeout: 60],
        steps: [
          %Step{slug: :step_a, step_type: :single, depends_on: []}
        ]
      }

      sql_statements = FlowCompiler.compile(definition)

      [flow_sql, step_sql] = sql_statements

      assert flow_sql == "SELECT pgflow.create_flow('simple_flow', 3, 5, 60)"

      assert step_sql ==
               "SELECT pgflow.add_step('simple_flow', 'step_a', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')"
    end

    test "compiles a flow with multiple steps and dependencies" do
      definition = %Definition{
        slug: :multi_step_flow,
        module: TestFlow,
        opts: [max_attempts: 3, base_delay: 1, timeout: 30],
        steps: [
          %Step{slug: :step_a, step_type: :single, depends_on: []},
          %Step{slug: :step_b, step_type: :single, depends_on: [:step_a]},
          %Step{slug: :step_c, step_type: :single, depends_on: [:step_a]},
          %Step{slug: :step_d, step_type: :single, depends_on: [:step_b, :step_c]}
        ]
      }

      sql_statements = FlowCompiler.compile(definition)

      [flow_sql, step_a_sql, step_b_sql, step_c_sql, step_d_sql] = sql_statements

      assert flow_sql == "SELECT pgflow.create_flow('multi_step_flow', 3, 1, 30)"

      assert step_a_sql ==
               "SELECT pgflow.add_step('multi_step_flow', 'step_a', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')"

      assert step_b_sql ==
               "SELECT pgflow.add_step('multi_step_flow', 'step_b', ARRAY['step_a']::text[], NULL, NULL, NULL, NULL, 'single')"

      assert step_c_sql ==
               "SELECT pgflow.add_step('multi_step_flow', 'step_c', ARRAY['step_a']::text[], NULL, NULL, NULL, NULL, 'single')"

      assert step_d_sql ==
               "SELECT pgflow.add_step('multi_step_flow', 'step_d', ARRAY['step_b', 'step_c']::text[], NULL, NULL, NULL, NULL, 'single')"
    end

    test "compiles a flow with map steps" do
      definition = %Definition{
        slug: :map_flow,
        module: TestFlow,
        opts: [max_attempts: 2, base_delay: 5, timeout: 120],
        steps: [
          %Step{slug: :generate_items, step_type: :single, depends_on: []},
          %Step{slug: :process_items, step_type: :map, depends_on: [:generate_items]},
          %Step{slug: :aggregate, step_type: :single, depends_on: [:process_items]}
        ]
      }

      sql_statements = FlowCompiler.compile(definition)

      [_flow_sql, _gen_sql, map_sql, _agg_sql] = sql_statements

      assert map_sql ==
               "SELECT pgflow.add_step('map_flow', 'process_items', ARRAY['generate_items']::text[], NULL, NULL, NULL, NULL, 'map')"
    end

    test "compiles a flow with step-level overrides" do
      definition = %Definition{
        slug: :custom_flow,
        module: TestFlow,
        opts: [max_attempts: 3, base_delay: 1, timeout: 60],
        steps: [
          %Step{
            slug: :custom_step,
            step_type: :single,
            depends_on: [],
            max_attempts: 5,
            base_delay: 10,
            timeout: 300,
            start_delay: 60
          }
        ]
      }

      sql_statements = FlowCompiler.compile(definition)

      [_flow_sql, step_sql] = sql_statements

      assert step_sql ==
               "SELECT pgflow.add_step('custom_flow', 'custom_step', ARRAY[]::text[], 5, 10, 300, 60, 'single')"
    end

    test "handles default options when not provided" do
      definition = %Definition{
        slug: :default_flow,
        module: TestFlow,
        opts: [],
        steps: [
          %Step{slug: :step, step_type: :single, depends_on: []}
        ]
      }

      sql_statements = FlowCompiler.compile(definition)

      [flow_sql, _step_sql] = sql_statements

      # Default values: max_attempts: 3, base_delay: 1, timeout: 60
      assert flow_sql == "SELECT pgflow.create_flow('default_flow', 3, 1, 60)"
    end
  end

  describe "create_flow_sql/1" do
    test "generates correct SQL for flow creation" do
      definition = %Definition{
        slug: :test_flow,
        module: TestFlow,
        opts: [max_attempts: 5, base_delay: 10, timeout: 120],
        steps: []
      }

      sql = FlowCompiler.create_flow_sql(definition)

      assert sql == "SELECT pgflow.create_flow('test_flow', 5, 10, 120)"
    end

    test "escapes single quotes in flow slug" do
      definition = %Definition{
        slug: :"test'flow",
        module: TestFlow,
        opts: [max_attempts: 3, base_delay: 1, timeout: 60],
        steps: []
      }

      sql = FlowCompiler.create_flow_sql(definition)

      assert sql == "SELECT pgflow.create_flow('test''flow', 3, 1, 60)"
    end

    test "preserves an explicit zero retry delay" do
      definition = %Definition{
        slug: :immediate_retry,
        module: TestFlow,
        opts: [max_attempts: 2, base_delay: 0, timeout: 60],
        steps: []
      }

      assert FlowCompiler.create_flow_sql(definition) ==
               "SELECT pgflow.create_flow('immediate_retry', 2, 0, 60)"
    end
  end

  describe "add_step_sql/2" do
    test "generates correct SQL for a step without dependencies" do
      step = %Step{slug: :my_step, step_type: :single, depends_on: []}

      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'my_step', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')"
    end

    test "generates correct SQL for a step with dependencies" do
      step = %Step{slug: :my_step, step_type: :single, depends_on: [:dep_a, :dep_b]}

      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'my_step', ARRAY['dep_a', 'dep_b']::text[], NULL, NULL, NULL, NULL, 'single')"
    end

    test "generates correct SQL for a map step" do
      step = %Step{slug: :map_step, step_type: :map, depends_on: [:source_step]}

      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'map_step', ARRAY['source_step']::text[], NULL, NULL, NULL, NULL, 'map')"
    end

    test "generates correct SQL with all optional parameters" do
      step = %Step{
        slug: :custom_step,
        step_type: :single,
        depends_on: [:prev_step],
        max_attempts: 5,
        base_delay: 10,
        timeout: 300,
        start_delay: 60
      }

      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'custom_step', ARRAY['prev_step']::text[], 5, 10, 300, 60, 'single')"
    end

    test "preserves a zero base delay while treating invalid zero overrides as NULL" do
      step = %Step{
        slug: :step,
        step_type: :single,
        depends_on: [],
        max_attempts: 0,
        base_delay: 0,
        timeout: 0,
        start_delay: 0
      }

      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'step', ARRAY[]::text[], NULL, 0, NULL, NULL, 'single')"
    end

    test "omits condition args when none are set" do
      step = %Step{slug: :plain}
      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'plain', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')"
    end

    test "emits required_input_pattern as a named arg and omits when_unmet when unset" do
      step = %Step{slug: :premium, if: %{plan: "premium"}}
      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'premium', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single', required_input_pattern => '{\"plan\":\"premium\"}'::jsonb)"

      refute sql =~ "when_unmet"
    end

    test "encodes skip_cascade as skip-cascade" do
      step = %Step{slug: :branch, if: %{on: true}, when_unmet: :skip_cascade}
      sql = FlowCompiler.add_step_sql(:my_flow, step)
      assert sql =~ "when_unmet => 'skip-cascade'"
      refute sql =~ "skip_cascade"
    end

    test "emits if_not and when_exhausted as named args" do
      step = %Step{
        slug: :email,
        if_not: %{plan: "premium"},
        when_unmet: :skip,
        when_exhausted: :skip
      }

      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'email', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single', forbidden_input_pattern => '{\"plan\":\"premium\"}'::jsonb, when_unmet => 'skip', when_exhausted => 'skip')"
    end

    test "omits when_unmet and when_exhausted entirely when the step doesn't set them" do
      step = %Step{slug: :branch, if: %{on: true}}
      sql = FlowCompiler.add_step_sql(:my_flow, step)

      refute sql =~ "when_unmet"
      refute sql =~ "when_exhausted"
    end

    test "emits when_exhausted alone (no if/if_not/when_unmet) as a named arg" do
      step = %Step{slug: :email, when_exhausted: :skip}
      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'email', ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single', when_exhausted => 'skip')"
    end

    test "unconditional 8-arg positional form is unaffected by named-arg support" do
      step = %Step{slug: :plain, step_type: :single, depends_on: [:dep_a]}
      sql = FlowCompiler.add_step_sql(:my_flow, step)

      assert sql ==
               "SELECT pgflow.add_step('my_flow', 'plain', ARRAY['dep_a']::text[], NULL, NULL, NULL, NULL, 'single')"

      refute sql =~ "=>"
    end
  end

  describe "integration with real flow definitions" do
    test "compiles SimpleFlow definition" do
      definition = SimpleFlow.__pgflow_definition__()
      sql_statements = FlowCompiler.compile(definition)

      [flow_sql, step_sql] = sql_statements

      assert flow_sql =~ "SELECT pgflow.create_flow('simple_flow'"
      assert step_sql =~ "SELECT pgflow.add_step('simple_flow', 'process'"
    end

    test "compiles LinearFlow definition" do
      definition = LinearFlow.__pgflow_definition__()
      sql_statements = FlowCompiler.compile(definition)

      [flow_sql, step_a, step_b, step_c] = sql_statements

      assert flow_sql =~ "SELECT pgflow.create_flow('linear_flow'"

      # Check step order and dependencies
      assert step_a =~ "'step_a', ARRAY[]::text[]"
      assert step_b =~ "'step_b', ARRAY['step_a']::text[]"
      assert step_c =~ "'step_c', ARRAY['step_b']::text[]"
    end

    test "compiles ParallelFlow definition" do
      definition = ParallelFlow.__pgflow_definition__()
      sql_statements = FlowCompiler.compile(definition)

      [_, _, _, _, step_d_sql] = sql_statements

      # step_d depends on both step_b and step_c
      assert step_d_sql =~ "'step_d'"
      assert step_d_sql =~ "ARRAY['step_b', 'step_c']::text[]"
    end

    test "compiles MapFlow definition" do
      definition = MapFlow.__pgflow_definition__()
      sql_statements = FlowCompiler.compile(definition)

      [_, map_step_sql, _aggregate_sql] = sql_statements

      # First step is a map step
      assert map_step_sql =~ "'process_items'"
      assert map_step_sql =~ "'map'"
    end
  end

  describe "cron SQL generation" do
    test "cron_schedule_sql/3 generates correct SQL" do
      sql = FlowCompiler.cron_schedule_sql(:test_flow, "0 * * * *", %{})

      assert sql =~ "cron.schedule"
      assert sql =~ "'pgflow:test_flow'"
      assert sql =~ "'0 * * * *'"
      assert sql =~ "pgflow.start_flow('test_flow'"
    end

    test "cron_schedule_sql/3 includes input JSON" do
      sql = FlowCompiler.cron_schedule_sql(:test_flow, "*/5 * * * *", %{key: "value"})

      assert sql =~ ~s('{"key":"value"}'::jsonb)
    end

    test "cron_unschedule_sql/1 generates correct SQL" do
      sql = FlowCompiler.cron_unschedule_sql(:test_flow)

      assert sql == "SELECT cron.unschedule('pgflow:test_flow')"
    end

    test "has_cron?/1 returns false for module without cron" do
      refute FlowCompiler.has_cron?(SimpleFlow)
    end

    test "compile/1 includes cron SQL for flow with cron option" do
      defmodule CronTestFlow do
        use PgFlow.Flow

        @flow slug: :cron_test_flow, cron: [schedule: "0 * * * *", input: %{test: true}]

        step :process do
          fn input, _ctx -> input end
        end
      end

      definition = CronTestFlow.__pgflow_definition__()
      sql_statements = FlowCompiler.compile(definition)

      [_, _, cron_sql] = sql_statements

      assert cron_sql =~ "cron.schedule"
      assert cron_sql =~ "'pgflow:cron_test_flow'"
      assert cron_sql =~ "'0 * * * *'"
      assert cron_sql =~ ~s("test":true)
    end

    test "compile/1 does not include cron SQL for flow without cron option" do
      definition = SimpleFlow.__pgflow_definition__()
      sql_statements = FlowCompiler.compile(definition)

      # No cron SQL should be present
      refute Enum.any?(sql_statements, &(&1 =~ "cron.schedule"))
    end
  end
end
