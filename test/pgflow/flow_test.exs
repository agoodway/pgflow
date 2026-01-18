defmodule PgFlow.FlowTest do
  use ExUnit.Case, async: true

  alias PgFlow.Flow.Definition

  alias PgFlow.TestFlows.{
    DependentMapFlow,
    LinearFlow,
    MapFlow,
    ParallelFlow,
    SimpleFlow
  }

  describe "use PgFlow.Flow" do
    test "sets up the module correctly" do
      # Ensure the module is fully loaded before checking exported functions
      Code.ensure_loaded!(SimpleFlow)

      assert function_exported?(SimpleFlow, :__pgflow_slug__, 0)
      assert function_exported?(SimpleFlow, :__pgflow_definition__, 0)
      assert function_exported?(SimpleFlow, :__pgflow_steps__, 0)
      assert function_exported?(SimpleFlow, :__pgflow_handler__, 1)
    end
  end

  describe "@flow attribute requirements" do
    test "requires @flow attribute" do
      assert_raise CompileError, ~r/Missing @flow attribute/, fn ->
        defmodule MissingFlowAttribute do
          use PgFlow.Flow

          step :process do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "requires :slug in @flow attribute" do
      assert_raise KeyError, fn ->
        defmodule MissingSlug do
          use PgFlow.Flow

          @flow max_attempts: 3

          step :process do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "accepts valid @flow with slug" do
      assert SimpleFlow.__pgflow_slug__() == :simple_flow
    end
  end

  describe "step macro" do
    test "defines steps with correct defaults" do
      definition = SimpleFlow.__pgflow_definition__()
      [step] = definition.steps

      assert step.slug == :process
      assert step.step_type == :single
      assert step.depends_on == []
      assert step.max_attempts == 3
      assert step.base_delay == 1
      assert step.timeout == 30
      assert step.start_delay == 0
    end

    test "defines steps with dependencies" do
      definition = LinearFlow.__pgflow_definition__()
      steps = definition.steps

      step_a = Enum.find(steps, &(&1.slug == :step_a))
      step_b = Enum.find(steps, &(&1.slug == :step_b))
      step_c = Enum.find(steps, &(&1.slug == :step_c))

      assert step_a.depends_on == []
      assert step_b.depends_on == [:step_a]
      assert step_c.depends_on == [:step_b]
    end

    test "handles multiple dependencies" do
      definition = ParallelFlow.__pgflow_definition__()
      steps = definition.steps

      step_d = Enum.find(steps, &(&1.slug == :step_d))
      assert :step_b in step_d.depends_on
      assert :step_c in step_d.depends_on
      assert length(step_d.depends_on) == 2
    end

    test "allows step-level option overrides" do
      defmodule CustomStepOptions do
        use PgFlow.Flow

        @flow slug: :custom_step_options, max_attempts: 1, base_delay: 1, timeout: 30

        step :custom, max_attempts: 5, base_delay: 10, timeout: 60, start_delay: 5 do
          fn input, _ctx -> input end
        end
      end

      definition = CustomStepOptions.__pgflow_definition__()
      [step] = definition.steps

      assert step.max_attempts == 5
      assert step.base_delay == 10
      assert step.timeout == 60
      assert step.start_delay == 5
    end
  end

  describe "map macro" do
    test "defines map steps correctly" do
      definition = MapFlow.__pgflow_definition__()
      [map_step | _] = definition.steps

      assert map_step.slug == :process_items
      assert map_step.step_type == :map
      assert map_step.depends_on == []
    end

    test "defines dependent map steps with array option" do
      definition = DependentMapFlow.__pgflow_definition__()
      steps = definition.steps

      map_step = Enum.find(steps, &(&1.slug == :process_items))

      assert map_step.step_type == :map
      # The :array option is converted to :depends_on
      assert map_step.depends_on == [:generate_items]
    end

    test "allows map-level option overrides" do
      defmodule CustomMapOptions do
        use PgFlow.Flow

        @flow slug: :custom_map_options, max_attempts: 1

        map :custom_map, max_attempts: 7, timeout: 90 do
          fn item, _ctx -> item end
        end
      end

      definition = CustomMapOptions.__pgflow_definition__()
      [step] = definition.steps

      assert step.max_attempts == 7
      assert step.timeout == 90
    end
  end

  describe "__pgflow_slug__/0" do
    test "returns the flow slug" do
      assert SimpleFlow.__pgflow_slug__() == :simple_flow
      assert LinearFlow.__pgflow_slug__() == :linear_flow
      assert ParallelFlow.__pgflow_slug__() == :parallel_flow
      assert MapFlow.__pgflow_slug__() == :map_flow
      assert DependentMapFlow.__pgflow_slug__() == :dependent_map_flow
    end
  end

  describe "__pgflow_definition__/0" do
    test "returns a Definition struct" do
      definition = SimpleFlow.__pgflow_definition__()

      assert %Definition{} = definition
      assert definition.slug == :simple_flow
      assert definition.module == SimpleFlow
      assert is_list(definition.steps)
      assert is_list(definition.opts)
    end

    test "includes flow-level options" do
      definition = SimpleFlow.__pgflow_definition__()

      assert definition.opts[:max_attempts] == 3
      assert definition.opts[:base_delay] == 1
      assert definition.opts[:timeout] == 30
    end

    test "includes all steps in order" do
      definition = LinearFlow.__pgflow_definition__()

      step_slugs = Enum.map(definition.steps, & &1.slug)
      assert step_slugs == [:step_a, :step_b, :step_c]
    end
  end

  describe "__pgflow_steps__/0" do
    test "returns raw step definitions" do
      raw_steps = SimpleFlow.__pgflow_steps__()

      assert is_list(raw_steps)
      assert [{:process, :step, [], _block}] = raw_steps
    end

    test "preserves step order" do
      raw_steps = LinearFlow.__pgflow_steps__()
      step_slugs = Enum.map(raw_steps, fn {slug, _type, _opts, _block} -> slug end)

      assert step_slugs == [:step_a, :step_b, :step_c]
    end

    test "includes step options" do
      raw_steps = LinearFlow.__pgflow_steps__()

      {_slug, _type, opts, _block} =
        Enum.find(raw_steps, fn {slug, _, _, _} -> slug == :step_b end)

      assert opts[:depends_on] == [:step_a]
    end
  end

  describe "__pgflow_handler__/1" do
    test "returns handler function for defined steps" do
      handler = SimpleFlow.__pgflow_handler__(:process)

      assert is_function(handler, 2)
    end

    test "handler function executes correctly" do
      handler = SimpleFlow.__pgflow_handler__(:process)
      input = %{"value" => 10}

      ctx = %PgFlow.Context{
        run_id: "test-run",
        step_slug: :process,
        task_index: 0,
        attempt: 1,
        repo: PgFlow.TestRepo
      }

      result = handler.(input, ctx)

      assert result == %{result: 20}
    end

    test "handler function receives dependencies for dependent steps" do
      handler = LinearFlow.__pgflow_handler__(:step_b)
      deps = %{step_a: %{"a_value" => 5}}

      ctx = %PgFlow.Context{
        run_id: "test-run",
        step_slug: :step_b,
        task_index: 0,
        attempt: 1,
        repo: PgFlow.TestRepo
      }

      result = handler.(deps, ctx)

      assert result == %{b_value: 15}
    end

    test "raises for undefined steps" do
      assert_raise RuntimeError, ~r/No handler defined for step: :nonexistent/, fn ->
        SimpleFlow.__pgflow_handler__(:nonexistent)
      end
    end

    test "returns different handlers for different steps" do
      handler_a = LinearFlow.__pgflow_handler__(:step_a)
      handler_b = LinearFlow.__pgflow_handler__(:step_b)

      refute handler_a == handler_b
    end
  end

  describe "handler module delegation" do
    test "delegates to handler module when specified" do
      defmodule TestHandler do
        def handle(input, _ctx) do
          %{handled_by: :module, value: input["value"] * 10}
        end
      end

      defmodule HandlerModuleFlow do
        use PgFlow.Flow

        @flow slug: :handler_module_flow

        step :delegated, handler: PgFlow.FlowTest.TestHandler do
        end
      end

      handler = HandlerModuleFlow.__pgflow_handler__(:delegated)
      input = %{"value" => 5}

      ctx = %PgFlow.Context{
        run_id: "test-run",
        step_slug: :delegated,
        task_index: 0,
        attempt: 1,
        repo: PgFlow.TestRepo
      }

      result = handler.(input, ctx)

      assert result == %{handled_by: :module, value: 50}
    end
  end

  describe "flow definition validation" do
    test "accepts valid flow definitions" do
      definition = SimpleFlow.__pgflow_definition__()

      assert {:ok, ^definition} = Definition.validate(definition)
    end

    test "validates linear dependencies" do
      definition = LinearFlow.__pgflow_definition__()

      assert {:ok, ^definition} = Definition.validate(definition)
    end

    test "validates parallel dependencies" do
      definition = ParallelFlow.__pgflow_definition__()

      assert {:ok, ^definition} = Definition.validate(definition)
    end

    test "validates map step constraints" do
      definition = DependentMapFlow.__pgflow_definition__()

      # DependentMapFlow has a map step that depends on generate_items (implicitly via array option)
      # The actual depends_on is empty because array is specified
      assert {:ok, ^definition} = Definition.validate(definition)
    end
  end
end
