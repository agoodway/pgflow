defmodule PgFlow.CronTest do
  use ExUnit.Case, async: true

  describe "use PgFlow.Cron" do
    test "basic cron module compiles and exposes expected functions" do
      defmodule BasicCron do
        use PgFlow.Cron

        @cron queue: :basic_cron, expression: "0 9 * * *"

        schedule do
          fn input, _ctx -> %{result: input} end
        end
      end

      assert function_exported?(BasicCron, :__pgflow_slug__, 0)
      assert function_exported?(BasicCron, :__pgflow_definition__, 0)
      assert function_exported?(BasicCron, :__pgflow_steps__, 0)
      assert function_exported?(BasicCron, :__pgflow_handler__, 0)
      assert function_exported?(BasicCron, :__pgflow_handler__, 1)
      assert function_exported?(BasicCron, :__pgflow_cron_expression__, 0)
      assert function_exported?(BasicCron, :__pgflow_cron_input__, 0)
      assert function_exported?(BasicCron, :perform, 2)
    end
  end

  describe "missing @cron attribute" do
    test "raises CompileError" do
      assert_raise CompileError, ~r/Missing @cron attribute/, fn ->
        defmodule NoCronAttr do
          use PgFlow.Cron

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end
  end

  describe "missing :queue option" do
    test "raises CompileError" do
      assert_raise CompileError, ~r/Missing :queue/, fn ->
        defmodule NoQueue do
          use PgFlow.Cron

          @cron expression: "0 9 * * *"

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end
  end

  describe "missing :expression option" do
    test "raises CompileError" do
      assert_raise CompileError, ~r/Missing :expression/, fn ->
        defmodule NoExpression do
          use PgFlow.Cron

          @cron queue: :no_expr

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end
  end

  describe "missing schedule block" do
    test "raises CompileError" do
      assert_raise CompileError, ~r/exactly one `schedule` block/, fn ->
        defmodule NoSchedule do
          use PgFlow.Cron

          @cron queue: :no_schedule, expression: "0 9 * * *"
        end
      end
    end
  end

  describe "unknown @cron keys" do
    test "raises CompileError" do
      assert_raise CompileError, ~r/Unknown @cron option/, fn ->
        defmodule UnknownKeys do
          use PgFlow.Cron

          @cron queue: :test, expression: "0 9 * * *", bogus: true

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end
  end

  describe "multiple @cron attributes" do
    test "raises CompileError" do
      assert_raise CompileError, ~r/Multiple @cron attributes defined/, fn ->
        defmodule DuplicateCron do
          use PgFlow.Cron

          @cron queue: :first, expression: "0 9 * * *"
          @cron queue: :second, expression: "0 17 * * *"

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end
  end

  describe "option type validation" do
    test "non-atom :queue rejected" do
      assert_raise CompileError, ~r/:queue must be an atom/, fn ->
        defmodule StringQueue do
          use PgFlow.Cron

          @cron queue: "string_queue", expression: "0 9 * * *"

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end

    test "non-string :expression rejected" do
      assert_raise CompileError, ~r/:expression must be a string/, fn ->
        defmodule AtomExpression do
          use PgFlow.Cron

          @cron queue: :test, expression: :not_a_string

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end

    test "non-map :input rejected" do
      assert_raise CompileError, ~r/:input must be a map/, fn ->
        defmodule StringInput do
          use PgFlow.Cron

          @cron queue: :test, expression: "0 9 * * *", input: "not_a_map"

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end

    test "non-positive :max_attempts rejected" do
      assert_raise CompileError, ~r/:max_attempts must be a positive integer/, fn ->
        defmodule BadMaxAttempts do
          use PgFlow.Cron

          @cron queue: :test, expression: "0 9 * * *", max_attempts: 0

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end

    test "negative :base_delay rejected" do
      assert_raise CompileError, ~r/:base_delay must be a non-negative integer/, fn ->
        defmodule BadBaseDelay do
          use PgFlow.Cron

          @cron queue: :test, expression: "0 9 * * *", base_delay: -1

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end

    test "non-positive :timeout rejected" do
      assert_raise CompileError, ~r/:timeout must be a positive integer/, fn ->
        defmodule BadTimeout do
          use PgFlow.Cron

          @cron queue: :test, expression: "0 9 * * *", timeout: 0

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end
  end

  describe "cron expression validation" do
    test "valid standard expression passes" do
      defmodule ValidCron do
        use PgFlow.Cron

        @cron queue: :valid_cron, expression: "0 9 * * *"

        schedule do
          fn _input, _ctx -> :ok end
        end
      end

      assert ValidCron.__pgflow_cron_expression__() == "0 9 * * *"
    end

    test "valid expression with ranges and steps passes" do
      defmodule RangeCron do
        use PgFlow.Cron

        @cron queue: :range_cron, expression: "*/15 9-17 * * 1-5"

        schedule do
          fn _input, _ctx -> :ok end
        end
      end

      assert RangeCron.__pgflow_cron_expression__() == "*/15 9-17 * * 1-5"
    end

    test "invalid expression raises CompileError" do
      assert_raise CompileError, ~r/Invalid cron expression/, fn ->
        defmodule InvalidCron do
          use PgFlow.Cron

          @cron queue: :test, expression: "invalid"

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end

    test "empty string expression raises CompileError" do
      assert_raise CompileError, ~r/Invalid cron expression/, fn ->
        defmodule EmptyExprCron do
          use PgFlow.Cron

          @cron queue: :test, expression: ""

          schedule do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end
  end

  describe "__pgflow_definition__/0" do
    test "returns a Definition struct with flow_type: :cron" do
      defmodule DefinitionCron do
        use PgFlow.Cron

        @cron queue: :definition_cron,
              expression: "0 9 * * *",
              max_attempts: 5,
              base_delay: 10,
              timeout: 120

        schedule do
          fn _input, _ctx -> :ok end
        end
      end

      definition = DefinitionCron.__pgflow_definition__()

      assert definition.slug == :definition_cron
      assert definition.module == DefinitionCron
      assert definition.flow_type == :cron
      assert definition.opts[:max_attempts] == 5
      assert definition.opts[:base_delay] == 10
      assert definition.opts[:timeout] == 120

      assert [step] = definition.steps
      assert step.slug == :perform
      assert step.step_type == :single
      assert step.depends_on == []
    end

    test "uses default options when not specified" do
      defmodule DefaultsCron do
        use PgFlow.Cron

        @cron queue: :defaults_cron, expression: "* * * * *"

        schedule do
          fn _input, _ctx -> :ok end
        end
      end

      definition = DefaultsCron.__pgflow_definition__()

      assert definition.opts[:max_attempts] == 1
      assert definition.opts[:base_delay] == 1
      assert definition.opts[:timeout] == 30
    end
  end

  describe "__pgflow_cron_input__/0" do
    test "returns empty map when no input specified" do
      defmodule NoInputCron do
        use PgFlow.Cron

        @cron queue: :no_input_cron, expression: "0 9 * * *"

        schedule do
          fn _input, _ctx -> :ok end
        end
      end

      assert NoInputCron.__pgflow_cron_input__() == %{}
    end

    test "returns the provided map when input specified" do
      defmodule WithInputCron do
        use PgFlow.Cron

        @cron queue: :with_input_cron,
              expression: "0 9 * * *",
              input: %{"report_type" => "daily", "format" => "pdf"}

        schedule do
          fn _input, _ctx -> :ok end
        end
      end

      assert WithInputCron.__pgflow_cron_input__() == %{
               "report_type" => "daily",
               "format" => "pdf"
             }
    end
  end

  describe "__pgflow_handler__/0 and __pgflow_handler__/1" do
    test "returns the handler function" do
      defmodule HandlerCron do
        use PgFlow.Cron

        @cron queue: :handler_cron, expression: "0 9 * * *"

        schedule do
          fn input, _ctx -> %{got: input["key"]} end
        end
      end

      handler = HandlerCron.__pgflow_handler__(:perform)
      assert is_function(handler, 2)
      assert handler.(%{"key" => "value"}, nil) == %{got: "value"}

      handler0 = HandlerCron.__pgflow_handler__()
      assert is_function(handler0, 2)
    end

    test "raises for undefined step slugs" do
      defmodule RaiseCron do
        use PgFlow.Cron

        @cron queue: :raise_cron, expression: "0 9 * * *"

        schedule do
          fn _input, _ctx -> :ok end
        end
      end

      assert_raise RuntimeError, ~r/No handler defined/, fn ->
        RaiseCron.__pgflow_handler__(:nonexistent)
      end
    end
  end

  describe "perform/2 convenience wrapper" do
    test "delegates to the schedule handler" do
      defmodule PerformCron do
        use PgFlow.Cron

        @cron queue: :perform_cron, expression: "0 9 * * *"

        schedule do
          fn input, _ctx -> %{doubled: input["value"] * 2} end
        end
      end

      result = PerformCron.perform(%{"value" => 5}, nil)
      assert result == %{doubled: 10}
    end
  end
end
