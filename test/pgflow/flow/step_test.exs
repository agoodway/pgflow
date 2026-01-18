defmodule PgFlow.Flow.StepTest do
  use ExUnit.Case, async: true

  alias PgFlow.Flow.Step

  describe "struct creation" do
    test "requires slug field" do
      assert_raise ArgumentError,
                   ~r/the following keys must also be given when building struct/,
                   fn ->
                     struct!(Step, [])
                   end
    end

    test "creates step with only required fields" do
      step = struct!(Step, slug: :test_step)

      assert step.slug == :test_step
      assert step.step_type == :single
      assert step.depends_on == []
      assert step.max_attempts == nil
      assert step.base_delay == nil
      assert step.timeout == nil
      assert step.start_delay == nil
    end

    test "creates step with all fields" do
      step =
        struct!(Step,
          slug: :full_step,
          step_type: :map,
          depends_on: [:step_a, :step_b],
          max_attempts: 5,
          base_delay: 2000,
          timeout: 60_000,
          start_delay: 1000
        )

      assert step.slug == :full_step
      assert step.step_type == :map
      assert step.depends_on == [:step_a, :step_b]
      assert step.max_attempts == 5
      assert step.base_delay == 2000
      assert step.timeout == 60_000
      assert step.start_delay == 1000
    end
  end

  describe "default values" do
    test "step_type defaults to :single" do
      step = %Step{slug: :test}
      assert step.step_type == :single
    end

    test "depends_on defaults to empty list" do
      step = %Step{slug: :test}
      assert step.depends_on == []
    end

    test "optional fields default to nil" do
      step = %Step{slug: :test}

      assert step.max_attempts == nil
      assert step.base_delay == nil
      assert step.timeout == nil
      assert step.start_delay == nil
    end
  end

  describe "step_type values" do
    test "accepts :single step type" do
      step = %Step{slug: :test, step_type: :single}
      assert step.step_type == :single
    end

    test "accepts :map step type" do
      step = %Step{slug: :test, step_type: :map}
      assert step.step_type == :map
    end
  end

  describe "slug_to_string/1" do
    test "converts atom slug to string" do
      assert Step.slug_to_string(:fetch_user) == "fetch_user"
      assert Step.slug_to_string(:send_email) == "send_email"
      assert Step.slug_to_string(:process_items) == "process_items"
    end

    test "handles single character atoms" do
      assert Step.slug_to_string(:a) == "a"
    end

    test "handles atoms with underscores" do
      assert Step.slug_to_string(:multi_word_step_name) == "multi_word_step_name"
    end

    test "handles atoms with numbers" do
      assert Step.slug_to_string(:step_123) == "step_123"
    end
  end

  describe "from_tuple/1 with atom" do
    test "converts atom to step struct" do
      step = Step.from_tuple(:simple_step)

      assert %Step{} = step
      assert step.slug == :simple_step
      assert step.step_type == :single
      assert step.depends_on == []
    end

    test "applies default values" do
      step = Step.from_tuple(:test)

      assert step.max_attempts == nil
      assert step.base_delay == nil
      assert step.timeout == nil
      assert step.start_delay == nil
    end
  end

  describe "from_tuple/1 with tuple" do
    test "converts tuple with empty options" do
      step = Step.from_tuple({:test_step, []})

      assert step.slug == :test_step
      assert step.step_type == :single
      assert step.depends_on == []
    end

    test "converts tuple with depends_on option" do
      step = Step.from_tuple({:dependent_step, depends_on: [:step_a, :step_b]})

      assert step.slug == :dependent_step
      assert step.depends_on == [:step_a, :step_b]
    end

    test "converts tuple with step_type option" do
      step = Step.from_tuple({:map_step, step_type: :map})

      assert step.slug == :map_step
      assert step.step_type == :map
    end

    test "converts tuple with max_attempts option" do
      step = Step.from_tuple({:retry_step, max_attempts: 5})

      assert step.max_attempts == 5
    end

    test "converts tuple with base_delay option" do
      step = Step.from_tuple({:delayed_step, base_delay: 2000})

      assert step.base_delay == 2000
    end

    test "converts tuple with timeout option" do
      step = Step.from_tuple({:slow_step, timeout: 60_000})

      assert step.timeout == 60_000
    end

    test "converts tuple with start_delay option" do
      step = Step.from_tuple({:scheduled_step, start_delay: 5000})

      assert step.start_delay == 5000
    end

    test "converts tuple with all options" do
      step =
        Step.from_tuple(
          {:full_step,
           step_type: :map,
           depends_on: [:step_a],
           max_attempts: 5,
           base_delay: 2000,
           timeout: 60_000,
           start_delay: 1000}
        )

      assert step.slug == :full_step
      assert step.step_type == :map
      assert step.depends_on == [:step_a]
      assert step.max_attempts == 5
      assert step.base_delay == 2000
      assert step.timeout == 60_000
      assert step.start_delay == 1000
    end

    test "preserves nil for unspecified optional fields" do
      step = Step.from_tuple({:partial_step, max_attempts: 3})

      assert step.max_attempts == 3
      assert step.base_delay == nil
      assert step.timeout == nil
      assert step.start_delay == nil
    end
  end

  describe "from_tuple/1 edge cases" do
    test "handles single dependency as list" do
      step = Step.from_tuple({:test, depends_on: [:single_dep]})

      assert step.depends_on == [:single_dep]
      assert length(step.depends_on) == 1
    end

    test "handles empty depends_on list" do
      step = Step.from_tuple({:test, depends_on: []})

      assert step.depends_on == []
    end

    test "handles multiple dependencies" do
      step = Step.from_tuple({:test, depends_on: [:dep1, :dep2, :dep3]})

      assert step.depends_on == [:dep1, :dep2, :dep3]
      assert length(step.depends_on) == 3
    end
  end

  describe "pattern matching" do
    test "can pattern match on slug" do
      step = %Step{slug: :test}

      assert %Step{slug: :test} = step
    end

    test "can pattern match on step_type" do
      step = %Step{slug: :test, step_type: :map}

      assert %Step{step_type: :map} = step
    end

    test "can pattern match on depends_on" do
      step = %Step{slug: :test, depends_on: [:dep1]}

      assert %Step{depends_on: [:dep1]} = step
    end
  end

  describe "struct updates" do
    test "can update slug" do
      step = %Step{slug: :original}
      updated = %{step | slug: :updated}

      assert updated.slug == :updated
    end

    test "can update step_type" do
      step = %Step{slug: :test, step_type: :single}
      updated = %{step | step_type: :map}

      assert updated.step_type == :map
    end

    test "can update depends_on" do
      step = %Step{slug: :test, depends_on: []}
      updated = %{step | depends_on: [:new_dep]}

      assert updated.depends_on == [:new_dep]
    end

    test "can update optional fields" do
      step = %Step{slug: :test}
      updated = %{step | max_attempts: 5, timeout: 60_000}

      assert updated.max_attempts == 5
      assert updated.timeout == 60_000
    end
  end
end
