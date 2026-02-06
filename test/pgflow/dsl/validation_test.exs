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

    test ":expression accepts strings" do
      assert :ok == Validation.validate_option!(:expression, "0 9 * * *", fake_env())
    end

    test ":expression rejects non-strings" do
      assert_raise CompileError, ~r/:expression must be a string/, fn ->
        Validation.validate_option!(:expression, :not_a_string, fake_env())
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
end
