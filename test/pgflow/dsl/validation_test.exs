defmodule PgFlow.DSL.ValidationTest do
  use ExUnit.Case, async: true

  alias PgFlow.DSL.Validation

  defp fake_env do
    %Macro.Env{file: "test.ex", line: 1}
  end

  describe "compile_error!/2" do
    test "raises CompileError with correct file and line" do
      env = fake_env()

      err =
        assert_raise CompileError, fn ->
          Validation.compile_error!(env, "something went wrong")
        end

      assert err.description =~ "something went wrong"
      assert err.file == "test.ex"
      assert err.line == 1
    end
  end

  describe "validate_unknown_keys!/4" do
    test "passes when all keys are valid" do
      assert :ok ==
               Validation.validate_unknown_keys!(
                 [queue: :test, timeout: 30],
                 [:queue, :timeout],
                 :job,
                 fake_env()
               )
    end

    test "raises for unknown keys" do
      assert_raise CompileError, ~r/Unknown @job option\(s\):.*:bogus/, fn ->
        Validation.validate_unknown_keys!(
          [queue: :test, bogus: true],
          [:queue],
          :job,
          fake_env()
        )
      end
    end
  end

  describe "validate_required_keys!/4" do
    test "passes when all required keys are present" do
      assert :ok ==
               Validation.validate_required_keys!(
                 [queue: :test, expression: "* * * * *"],
                 [:queue, :expression],
                 :cron,
                 fake_env()
               )
    end

    test "raises for missing required key" do
      assert_raise CompileError, ~r/Missing :queue in @cron attribute/, fn ->
        Validation.validate_required_keys!(
          [expression: "* * * * *"],
          [:queue, :expression],
          :cron,
          fake_env()
        )
      end
    end
  end

  describe "validate_single_step!/3" do
    test "passes with exactly one step" do
      assert :ok ==
               Validation.validate_single_step!(
                 [{:perform, :step, [], nil}],
                 "error message",
                 fake_env()
               )
    end

    test "raises with zero steps" do
      assert_raise CompileError, ~r/exactly one/, fn ->
        Validation.validate_single_step!(
          [],
          "Must have exactly one `perform` block.",
          fake_env()
        )
      end
    end

    test "raises with two steps" do
      assert_raise CompileError, ~r/exactly one/, fn ->
        Validation.validate_single_step!(
          [{:perform, :step, [], nil}, {:perform, :step, [], nil}],
          "Must have exactly one `perform` block.",
          fake_env()
        )
      end
    end
  end

  describe "validate_option!/3" do
    test ":queue accepts atoms" do
      assert :ok == Validation.validate_option!(:queue, :my_queue, fake_env())
    end

    test ":queue rejects non-atoms" do
      assert_raise CompileError, ~r/:queue must be an atom/, fn ->
        Validation.validate_option!(:queue, "string", fake_env())
      end
    end

    test ":slug accepts atoms" do
      assert :ok == Validation.validate_option!(:slug, :my_slug, fake_env())
    end

    test ":slug rejects non-atoms" do
      assert_raise CompileError, ~r/:slug must be an atom/, fn ->
        Validation.validate_option!(:slug, "string", fake_env())
      end
    end

    test ":max_attempts accepts positive integers" do
      assert :ok == Validation.validate_option!(:max_attempts, 5, fake_env())
    end

    test ":max_attempts rejects zero" do
      assert_raise CompileError, ~r/:max_attempts must be a positive integer/, fn ->
        Validation.validate_option!(:max_attempts, 0, fake_env())
      end
    end

    test ":max_attempts rejects negative" do
      assert_raise CompileError, ~r/:max_attempts must be a positive integer/, fn ->
        Validation.validate_option!(:max_attempts, -1, fake_env())
      end
    end

    test ":max_attempts rejects float" do
      assert_raise CompileError, ~r/:max_attempts must be a positive integer/, fn ->
        Validation.validate_option!(:max_attempts, 1.5, fake_env())
      end
    end

    test ":max_attempts rejects string" do
      assert_raise CompileError, ~r/:max_attempts must be a positive integer/, fn ->
        Validation.validate_option!(:max_attempts, "three", fake_env())
      end
    end

    test ":base_delay accepts zero" do
      assert :ok == Validation.validate_option!(:base_delay, 0, fake_env())
    end

    test ":base_delay accepts positive integers" do
      assert :ok == Validation.validate_option!(:base_delay, 10, fake_env())
    end

    test ":base_delay rejects negative" do
      assert_raise CompileError, ~r/:base_delay must be a non-negative integer/, fn ->
        Validation.validate_option!(:base_delay, -1, fake_env())
      end
    end

    test ":base_delay rejects float" do
      assert_raise CompileError, ~r/:base_delay must be a non-negative integer/, fn ->
        Validation.validate_option!(:base_delay, 1.5, fake_env())
      end
    end

    test ":timeout accepts positive integers" do
      assert :ok == Validation.validate_option!(:timeout, 30, fake_env())
    end

    test ":timeout rejects zero" do
      assert_raise CompileError, ~r/:timeout must be a positive integer/, fn ->
        Validation.validate_option!(:timeout, 0, fake_env())
      end
    end

    test ":timeout rejects negative" do
      assert_raise CompileError, ~r/:timeout must be a positive integer/, fn ->
        Validation.validate_option!(:timeout, -5, fake_env())
      end
    end

    test ":timeout rejects float" do
      assert_raise CompileError, ~r/:timeout must be a positive integer/, fn ->
        Validation.validate_option!(:timeout, 2.5, fake_env())
      end
    end

    test ":schedule accepts strings" do
      assert :ok == Validation.validate_option!(:schedule, "0 9 * * *", fake_env())
    end

    test ":schedule rejects non-strings" do
      assert_raise CompileError, ~r/:schedule must be a string/, fn ->
        Validation.validate_option!(:schedule, :not_a_string, fake_env())
      end
    end

    test ":input accepts maps" do
      assert :ok == Validation.validate_option!(:input, %{"key" => "value"}, fake_env())
    end

    test ":input rejects non-maps" do
      assert_raise CompileError, ~r/:input must be a map/, fn ->
        Validation.validate_option!(:input, "not_a_map", fake_env())
      end
    end
  end

  describe "validate_cron_option!/2" do
    test "accepts string shorthand" do
      {expression, input} = Validation.validate_cron_option!("0 * * * *", fake_env())

      assert expression == "0 * * * *"
      assert input == %{}
    end

    test "accepts @hourly shorthand" do
      {expression, input} = Validation.validate_cron_option!("@hourly", fake_env())

      assert expression == "@hourly"
      assert input == %{}
    end

    test "accepts valid cron schedule with no input" do
      {schedule, input} =
        Validation.validate_cron_option!([schedule: "0 * * * *"], fake_env())

      assert schedule == "0 * * * *"
      assert input == %{}
    end

    test "accepts valid cron schedule with input" do
      {schedule, input} =
        Validation.validate_cron_option!(
          [schedule: "0 9 * * 1-5", input: %{type: "weekday"}],
          fake_env()
        )

      assert schedule == "0 9 * * 1-5"
      assert input == %{type: "weekday"}
    end

    test "raises for missing schedule" do
      assert_raise CompileError, ~r/Missing :schedule in cron option/, fn ->
        Validation.validate_cron_option!([input: %{}], fake_env())
      end
    end

    test "raises for invalid cron schedule" do
      assert_raise CompileError, ~r/Invalid cron schedule/, fn ->
        Validation.validate_cron_option!([schedule: "not a cron"], fake_env())
      end
    end

    test "raises for non-string schedule" do
      assert_raise CompileError, ~r/cron :schedule must be a string/, fn ->
        Validation.validate_cron_option!([schedule: :atom], fake_env())
      end
    end

    test "raises for non-map input" do
      assert_raise CompileError, ~r/cron :input must be a map/, fn ->
        Validation.validate_cron_option!(
          [schedule: "0 * * * *", input: "string"],
          fake_env()
        )
      end
    end

    test "raises for unknown cron keys" do
      assert_raise CompileError, ~r/Unknown cron option\(s\):.*:bogus/, fn ->
        Validation.validate_cron_option!(
          [schedule: "0 * * * *", bogus: true],
          fake_env()
        )
      end
    end

    test "raises for non-string-or-keyword-list cron option" do
      assert_raise CompileError, ~r/cron option must be a string or keyword list/, fn ->
        Validation.validate_cron_option!(123, fake_env())
      end
    end

    test "raises when input contains $$ sequence" do
      assert_raise CompileError, ~r/cannot contain.*\$\$/, fn ->
        Validation.validate_cron_option!(
          [schedule: "0 * * * *", input: %{key: "value$$evil"}],
          fake_env()
        )
      end
    end
  end

  describe "validate_step_opts!/2" do
    test "accepts if map and when_unmet atom" do
      assert :ok ==
               Validation.validate_step_opts!(
                 [if: %{plan: "premium"}, when_unmet: :skip],
                 fake_env()
               )
    end

    test "accepts if and if_not together" do
      assert :ok ==
               Validation.validate_step_opts!(
                 [if: %{status: "active"}, if_not: %{role: "admin"}, when_unmet: :skip],
                 fake_env()
               )
    end

    test "rejects keyword-list if" do
      assert_raise CompileError, ~r/:if must be a map/, fn ->
        Validation.validate_step_opts!([if: [plan: "premium"]], fake_env())
      end
    end

    test "rejects unknown when_unmet" do
      assert_raise CompileError, ~r/when_unmet/, fn ->
        Validation.validate_step_opts!([if: %{}, when_unmet: :maybe], fake_env())
      end
    end

    test "rejects when_unmet without if or if_not" do
      assert_raise CompileError, ~r/when_unmet requires :if or :if_not/, fn ->
        Validation.validate_step_opts!([when_unmet: :skip], fake_env())
      end
    end

    test "rejects unknown step keys" do
      assert_raise CompileError, ~r/Unknown @step option/, fn ->
        Validation.validate_step_opts!([iff: %{}], fake_env())
      end
    end
  end
end
