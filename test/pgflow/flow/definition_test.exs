defmodule PgFlow.Flow.DefinitionTest do
  use ExUnit.Case, async: true

  alias PgFlow.Flow.{Definition, Step}

  describe "validate/1" do
    test "validates a simple linear flow" do
      definition = %Definition{
        slug: :test_flow,
        module: TestModule,
        steps: [
          %Step{slug: :step_a, depends_on: []},
          %Step{slug: :step_b, depends_on: [:step_a]},
          %Step{slug: :step_c, depends_on: [:step_b]}
        ]
      }

      assert {:ok, ^definition} = Definition.validate(definition)
    end

    test "validates a parallel flow with fan-out/fan-in" do
      definition = %Definition{
        slug: :parallel_flow,
        module: TestModule,
        steps: [
          %Step{slug: :start, depends_on: []},
          %Step{slug: :branch_a, depends_on: [:start]},
          %Step{slug: :branch_b, depends_on: [:start]},
          %Step{slug: :join, depends_on: [:branch_a, :branch_b]}
        ]
      }

      assert {:ok, ^definition} = Definition.validate(definition)
    end

    test "detects circular dependencies" do
      definition = %Definition{
        slug: :cyclic_flow,
        module: TestModule,
        steps: [
          %Step{slug: :step_a, depends_on: [:step_c]},
          %Step{slug: :step_b, depends_on: [:step_a]},
          %Step{slug: :step_c, depends_on: [:step_b]}
        ]
      }

      assert {:error, message} = Definition.validate(definition)
      assert message =~ "Circular dependency"
    end

    test "detects missing dependencies" do
      definition = %Definition{
        slug: :missing_dep_flow,
        module: TestModule,
        steps: [
          %Step{slug: :step_a, depends_on: []},
          %Step{slug: :step_b, depends_on: [:nonexistent]}
        ]
      }

      assert {:error, message} = Definition.validate(definition)
      assert message =~ "non-existent"
    end

    test "validates map steps with single dependency" do
      definition = %Definition{
        slug: :map_flow,
        module: TestModule,
        steps: [
          %Step{slug: :generate, depends_on: [], step_type: :single},
          %Step{slug: :process, depends_on: [:generate], step_type: :map}
        ]
      }

      assert {:ok, ^definition} = Definition.validate(definition)
    end

    test "rejects map steps with multiple dependencies" do
      definition = %Definition{
        slug: :bad_map_flow,
        module: TestModule,
        steps: [
          %Step{slug: :source_a, depends_on: [], step_type: :single},
          %Step{slug: :source_b, depends_on: [], step_type: :single},
          %Step{slug: :process, depends_on: [:source_a, :source_b], step_type: :map}
        ]
      }

      assert {:error, message} = Definition.validate(definition)
      assert message =~ "map" or message =~ "multiple"
    end
  end

  describe "build_steps/1" do
    test "converts raw step tuples to Step structs" do
      raw_steps = [
        :step_a,
        {:step_b, depends_on: [:step_a], max_attempts: 5}
      ]

      steps = Definition.build_steps(raw_steps)

      assert [step_a, step_b] = steps
      assert step_a.slug == :step_a
      assert step_a.depends_on == []
      assert step_b.slug == :step_b
      assert step_b.depends_on == [:step_a]
      assert step_b.max_attempts == 5
    end
  end

  describe "slug_to_string/1" do
    test "converts definition slug to string" do
      definition = %Definition{
        slug: :my_workflow,
        module: TestModule,
        steps: []
      }

      assert Definition.slug_to_string(definition) == "my_workflow"
    end

    test "converts atom slug to string" do
      assert Definition.slug_to_string(:another_flow) == "another_flow"
    end
  end
end
