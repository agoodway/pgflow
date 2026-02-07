defmodule PgFlow.JobTest do
  use ExUnit.Case, async: true

  alias PgFlow.Flow.Definition

  describe "use PgFlow.Job" do
    test "sets up the module correctly" do
      defmodule ValidJob do
        use PgFlow.Job

        @job slug: :valid_job, max_attempts: 3

        perform do
          fn input, _ctx ->
            %{result: input["value"]}
          end
        end
      end

      Code.ensure_loaded!(ValidJob)

      assert function_exported?(ValidJob, :__pgflow_slug__, 0)
      assert function_exported?(ValidJob, :__pgflow_definition__, 0)
      assert function_exported?(ValidJob, :__pgflow_steps__, 0)
      assert function_exported?(ValidJob, :__pgflow_handler__, 0)
      assert function_exported?(ValidJob, :__pgflow_handler__, 1)
      assert function_exported?(ValidJob, :perform, 2)
    end
  end

  describe "@job attribute requirements" do
    test "requires @job attribute" do
      assert_raise CompileError, ~r/Missing @job attribute/, fn ->
        defmodule MissingJobAttribute do
          use PgFlow.Job

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "requires :queue (or :slug) in @job attribute" do
      assert_raise CompileError, ~r/Missing :queue in @job attribute/, fn ->
        defmodule MissingQueue do
          use PgFlow.Job

          @job max_attempts: 3

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "accepts :queue as the identifier" do
      defmodule QueueIdentifierJob do
        use PgFlow.Job

        @job queue: :queue_identifier_job

        perform do
          fn input, _ctx -> input end
        end
      end

      assert QueueIdentifierJob.__pgflow_slug__() == :queue_identifier_job
    end

    test "accepts :slug as an alias for :queue" do
      defmodule SlugAliasJob do
        use PgFlow.Job

        @job slug: :slug_alias_job

        perform do
          fn input, _ctx -> input end
        end
      end

      assert SlugAliasJob.__pgflow_slug__() == :slug_alias_job
    end

    test "requires exactly one perform block" do
      assert_raise CompileError, ~r/Jobs must have exactly one `perform` block/, fn ->
        defmodule NoPerform do
          use PgFlow.Job

          @job slug: :no_perform
        end
      end
    end

    test "rejects multiple perform blocks" do
      assert_raise CompileError, ~r/Jobs must have exactly one `perform` block/, fn ->
        defmodule MultiPerform do
          use PgFlow.Job

          @job slug: :multi_perform

          perform do
            fn input, _ctx -> input end
          end

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end
  end

  describe "multiple @job attributes" do
    test "raises CompileError" do
      assert_raise CompileError, ~r/Multiple @job attributes defined/, fn ->
        defmodule DuplicateJob do
          use PgFlow.Job

          @job slug: :first
          @job slug: :second

          perform do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end
  end

  describe "@job option validation" do
    test "rejects non-atom :slug" do
      assert_raise CompileError, ~r/:slug must be an atom/, fn ->
        defmodule StringSlug do
          use PgFlow.Job

          @job slug: "string_slug"

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "rejects non-positive-integer :max_attempts" do
      assert_raise CompileError, ~r/:max_attempts must be a positive integer/, fn ->
        defmodule NegativeMaxAttempts do
          use PgFlow.Job

          @job slug: :x, max_attempts: -1

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "rejects zero :max_attempts" do
      assert_raise CompileError, ~r/:max_attempts must be a positive integer/, fn ->
        defmodule ZeroMaxAttempts do
          use PgFlow.Job

          @job slug: :x, max_attempts: 0

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "rejects non-integer :max_attempts" do
      assert_raise CompileError, ~r/:max_attempts must be a positive integer/, fn ->
        defmodule FloatMaxAttempts do
          use PgFlow.Job

          @job slug: :x, max_attempts: 1.5

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "rejects negative :base_delay" do
      assert_raise CompileError, ~r/:base_delay must be a non-negative integer/, fn ->
        defmodule NegativeBaseDelay do
          use PgFlow.Job

          @job slug: :x, base_delay: -1

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "allows zero :base_delay" do
      defmodule ZeroBaseDelay do
        use PgFlow.Job

        @job slug: :zero_base_delay, base_delay: 0

        perform do
          fn input, _ctx -> input end
        end
      end

      definition = ZeroBaseDelay.__pgflow_definition__()
      assert definition.opts[:base_delay] == 0
    end

    test "rejects non-positive-integer :timeout" do
      assert_raise CompileError, ~r/:timeout must be a positive integer/, fn ->
        defmodule ZeroTimeout do
          use PgFlow.Job

          @job slug: :x, timeout: 0

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end

    test "rejects unknown keys" do
      assert_raise CompileError, ~r/Unknown @job option/, fn ->
        defmodule UnknownKeys do
          use PgFlow.Job

          @job slug: :x, priority: :high

          perform do
            fn input, _ctx -> input end
          end
        end
      end
    end
  end

  describe "__pgflow_slug__/0" do
    test "returns the job slug" do
      defmodule SlugJob do
        use PgFlow.Job

        @job slug: :my_job_slug

        perform do
          fn input, _ctx -> input end
        end
      end

      assert SlugJob.__pgflow_slug__() == :my_job_slug
    end
  end

  describe "__pgflow_definition__/0" do
    test "returns a Definition struct with flow_type: :job" do
      defmodule DefinitionJob do
        use PgFlow.Job

        @job slug: :definition_job, max_attempts: 5, base_delay: 10, timeout: 120

        perform do
          fn input, _ctx -> %{processed: input} end
        end
      end

      definition = DefinitionJob.__pgflow_definition__()

      assert %Definition{} = definition
      assert definition.slug == :definition_job
      assert definition.module == DefinitionJob
      assert definition.flow_type == :job
      assert definition.opts[:max_attempts] == 5
      assert definition.opts[:base_delay] == 10
      assert definition.opts[:timeout] == 120
    end

    test "has a single :perform step" do
      defmodule SingleStepJob do
        use PgFlow.Job

        @job slug: :single_step_job

        perform do
          fn input, _ctx -> input end
        end
      end

      definition = SingleStepJob.__pgflow_definition__()
      assert length(definition.steps) == 1

      [step] = definition.steps
      assert step.slug == :perform
      assert step.step_type == :single
      assert step.depends_on == []
    end

    test "uses default options when not specified" do
      defmodule DefaultOptsJob do
        use PgFlow.Job

        @job slug: :default_opts_job

        perform do
          fn input, _ctx -> input end
        end
      end

      definition = DefaultOptsJob.__pgflow_definition__()

      assert definition.opts[:max_attempts] == 1
      assert definition.opts[:base_delay] == 1
      assert definition.opts[:timeout] == 30
    end
  end

  describe "__pgflow_handler__/0 and __pgflow_handler__/1" do
    test "returns handler function" do
      defmodule HandlerJob do
        use PgFlow.Job

        @job slug: :handler_job

        perform do
          fn input, _ctx ->
            %{doubled: input["value"] * 2}
          end
        end
      end

      handler = HandlerJob.__pgflow_handler__()
      assert is_function(handler, 2)

      handler_by_slug = HandlerJob.__pgflow_handler__(:perform)
      assert is_function(handler_by_slug, 2)
    end

    test "raises for undefined step slugs" do
      defmodule RaiseJob do
        use PgFlow.Job

        @job slug: :raise_job

        perform do
          fn input, _ctx -> input end
        end
      end

      assert_raise RuntimeError, ~r/No handler defined for step: :nonexistent/, fn ->
        RaiseJob.__pgflow_handler__(:nonexistent)
      end
    end
  end

  describe "perform/2 convenience wrapper" do
    test "calls the handler with input and context" do
      defmodule WrapperJob do
        use PgFlow.Job

        @job slug: :wrapper_job

        perform do
          fn input, _ctx ->
            %{result: input["value"] * 3}
          end
        end
      end

      ctx = %PgFlow.Context{
        run_id: "test-run",
        step_slug: :perform,
        task_index: 0,
        attempt: 1,
        repo: PgFlow.TestRepo
      }

      result = WrapperJob.perform(%{"value" => 10}, ctx)
      assert result == %{result: 30}
    end
  end

  describe "__pgflow_steps__/0" do
    test "returns raw step definitions" do
      defmodule RawStepsJob do
        use PgFlow.Job

        @job slug: :raw_steps_job

        perform do
          fn input, _ctx -> input end
        end
      end

      raw_steps = RawStepsJob.__pgflow_steps__()
      assert is_list(raw_steps)
      assert [{:perform, :step, [], _block}] = raw_steps
    end
  end

  describe "cron option" do
    test "accepts string shorthand" do
      defmodule CronShorthandJob do
        use PgFlow.Job

        @job slug: :cron_shorthand_job, cron: "0 * * * *"

        perform do
          fn _input, _ctx -> :ok end
        end
      end

      assert CronShorthandJob.__pgflow_cron_expression__() == "0 * * * *"
      assert CronShorthandJob.__pgflow_cron_input__() == %{}
    end

    test "accepts @hourly shorthand" do
      defmodule CronHourlyJob do
        use PgFlow.Job

        @job slug: :cron_hourly_job, cron: "@hourly"

        perform do
          fn _input, _ctx -> :ok end
        end
      end

      assert CronHourlyJob.__pgflow_cron_expression__() == "@hourly"
      assert CronHourlyJob.__pgflow_cron_input__() == %{}
    end

    test "accepts valid cron schedule" do
      defmodule CronJob do
        use PgFlow.Job

        @job slug: :cron_job, cron: [schedule: "0 * * * *"]

        perform do
          fn _input, _ctx -> :ok end
        end
      end

      assert CronJob.__pgflow_cron_expression__() == "0 * * * *"
      assert CronJob.__pgflow_cron_input__() == %{}
    end

    test "accepts cron with input" do
      defmodule CronWithInputJob do
        use PgFlow.Job

        @job slug: :cron_with_input_job, cron: [schedule: "*/5 * * * *", input: %{key: "value"}]

        perform do
          fn _input, _ctx -> :ok end
        end
      end

      assert CronWithInputJob.__pgflow_cron_expression__() == "*/5 * * * *"
      assert CronWithInputJob.__pgflow_cron_input__() == %{key: "value"}
    end

    test "job without cron has nil expression" do
      defmodule NoCronJob do
        use PgFlow.Job

        @job slug: :no_cron_job

        perform do
          fn _input, _ctx -> :ok end
        end
      end

      assert NoCronJob.__pgflow_cron_expression__() == nil
      assert NoCronJob.__pgflow_cron_input__() == %{}
    end

    test "rejects cron without schedule" do
      assert_raise CompileError, ~r/Missing :schedule in cron option/, fn ->
        defmodule NoScheduleCron do
          use PgFlow.Job

          @job slug: :x, cron: [input: %{}]

          perform do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end

    test "rejects invalid cron schedule" do
      assert_raise CompileError, ~r/Invalid cron schedule/, fn ->
        defmodule InvalidCronJob do
          use PgFlow.Job

          @job slug: :x, cron: [schedule: "not a cron"]

          perform do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end

    test "rejects non-map input" do
      assert_raise CompileError, ~r/:input must be a map/, fn ->
        defmodule NonMapInputCron do
          use PgFlow.Job

          @job slug: :x, cron: [schedule: "0 * * * *", input: "not a map"]

          perform do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end

    test "rejects unknown cron keys" do
      assert_raise CompileError, ~r/Unknown cron option/, fn ->
        defmodule UnknownCronKeys do
          use PgFlow.Job

          @job slug: :x, cron: [schedule: "0 * * * *", unknown: true]

          perform do
            fn _input, _ctx -> :ok end
          end
        end
      end
    end
  end
end
