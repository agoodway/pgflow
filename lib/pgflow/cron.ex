defmodule PgFlow.Cron do
  @moduledoc """
  A macro-based DSL for defining pgflow recurring scheduled jobs.

  Cron jobs are single-step flows executed on a schedule via pg_cron.
  Use `use PgFlow.Cron` in your cron module.

  ## Example

      defmodule MyApp.Crons.DailyReport do
        use PgFlow.Cron

        @cron queue: :daily_report,
              expression: "0 9 * * *",
              max_attempts: 3,
              input: %{"report_type" => "daily"}

        schedule do
          fn input, _ctx ->
            generate_report(input["report_type"])
            %{generated: true}
          end
        end
      end

  ## Timezone

  pg_cron executes jobs in the PostgreSQL server's timezone (typically UTC).

  ## Cron Options

  The `@cron` module attribute accepts the following options:

    * `:queue` - (required) atom identifier for the cron job queue (becomes the flow slug)
    * `:expression` - (required) cron expression string (e.g., "0 9 * * *")
    * `:max_attempts` - maximum retry attempts for failed executions (default: 1)
    * `:base_delay` - base delay in seconds for exponential backoff (default: 1)
    * `:timeout` - execution timeout in seconds (default: 30)
    * `:input` - static input map passed to each execution (default: %{})

  ## Generated Functions

  Using this module generates the following callback functions:

    * `__pgflow_definition__/0` - returns a `PgFlow.Flow.Definition` struct with `flow_type: :cron`
    * `__pgflow_slug__/0` - returns the cron job queue slug atom
    * `__pgflow_steps__/0` - returns the raw step definitions (single `:perform` step)
    * `__pgflow_handler__/0` - returns the schedule handler function
    * `__pgflow_handler__(:perform)` - returns the schedule handler function
    * `__pgflow_cron_expression__/0` - returns the cron expression string
    * `__pgflow_cron_input__/0` - returns the static input map
    * `perform/2` - convenience wrapper for testing: `perform(input, ctx)`

  """

  @doc """
  Defines the cron job's schedule block.

  The block must return a 2-arity function that receives the job input
  and a `PgFlow.Context` struct.

  ## Examples

      schedule do
        fn input, _ctx ->
          %{result: process(input["data"])}
        end
      end

  """
  defmacro schedule(do: block) do
    quote do
      @pgflow_steps {:perform, :step, [], unquote(Macro.escape(block))}
    end
  end

  defmacro __using__(_opts) do
    quote do
      import PgFlow.Cron, only: [schedule: 1]

      Module.register_attribute(__MODULE__, :cron, accumulate: true, persist: false)
      Module.register_attribute(__MODULE__, :pgflow_steps, accumulate: true, persist: false)

      @before_compile PgFlow.Cron
    end
  end

  @valid_keys [:queue, :expression, :max_attempts, :base_delay, :timeout, :input]

  defmacro __before_compile__(env) do
    cron_attrs_list = Module.get_attribute(env.module, :cron)
    steps = Module.get_attribute(env.module, :pgflow_steps) |> Enum.reverse()

    cron_attrs = validate_cron_attr_count!(cron_attrs_list, env)
    validate_cron_attrs!(cron_attrs, env)
    validate_steps!(steps, env)

    slug = Keyword.fetch!(cron_attrs, :queue)
    expression = Keyword.fetch!(cron_attrs, :expression)
    max_attempts = Keyword.get(cron_attrs, :max_attempts, 1)
    base_delay = Keyword.get(cron_attrs, :base_delay, 1)
    timeout = Keyword.get(cron_attrs, :timeout, 30)
    input = Keyword.get(cron_attrs, :input, %{})

    validate_cron_expression!(expression, env)

    flow_opts = [
      max_attempts: max_attempts,
      base_delay: base_delay,
      timeout: timeout
    ]

    [{:perform, :step, _opts, block}] = steps

    step_def = %PgFlow.Flow.Step{
      slug: :perform,
      step_type: :single,
      depends_on: [],
      max_attempts: max_attempts,
      base_delay: base_delay,
      timeout: timeout,
      start_delay: 0
    }

    quote do
      @doc """
      Returns the cron job queue slug.
      """
      def __pgflow_slug__, do: unquote(slug)

      @doc """
      Returns the raw step definitions.
      """
      def __pgflow_steps__, do: unquote(Macro.escape(steps))

      @doc """
      Returns the cron expression string.
      """
      def __pgflow_cron_expression__, do: unquote(expression)

      @doc """
      Returns the static input map.
      """
      def __pgflow_cron_input__, do: unquote(Macro.escape(input))

      @doc """
      Returns the flow definition struct (with flow_type: :cron).
      """
      def __pgflow_definition__ do
        %PgFlow.Flow.Definition{
          slug: unquote(slug),
          module: __MODULE__,
          steps: [unquote(Macro.escape(step_def))],
          opts: unquote(Macro.escape(flow_opts)),
          flow_type: :cron
        }
      end

      @doc """
      Returns the schedule handler function.
      """
      def __pgflow_handler__(:perform) do
        unquote(block)
      end

      def __pgflow_handler__ do
        unquote(block)
      end

      def __pgflow_handler__(slug) do
        raise "No handler defined for step: #{inspect(slug)}"
      end

      @doc """
      Convenience wrapper for calling the cron handler directly.

      Useful for testing:

          result = MyCron.perform(%{"key" => "value"}, ctx)

      """
      def perform(input, ctx) do
        handler = __pgflow_handler__(:perform)
        handler.(input, ctx)
      end
    end
  end

  # --- Validation helpers ---

  alias PgFlow.DSL.Validation

  defp validate_cron_attr_count!([], _env), do: nil
  defp validate_cron_attr_count!([single], _env), do: single

  defp validate_cron_attr_count!(_multiple, env),
    do:
      Validation.compile_error!(
        env,
        "Multiple @cron attributes defined. Only one @cron attribute is allowed per module."
      )

  defp validate_cron_attrs!(nil, env),
    do:
      Validation.compile_error!(
        env,
        "Missing @cron attribute. You must define @cron with :queue and :expression options."
      )

  defp validate_cron_attrs!(attrs, env) do
    Validation.validate_required_keys!(attrs, [:queue, :expression], :cron, env)
    Validation.validate_unknown_keys!(attrs, @valid_keys, :cron, env)
    validate_option_values!(attrs, env)
  end

  defp validate_steps!(steps, env),
    do:
      Validation.validate_single_step!(
        steps,
        "Cron jobs must have exactly one `schedule` block.",
        env
      )

  defp validate_option_values!(attrs, env) do
    Validation.validate_option!(:queue, Keyword.fetch!(attrs, :queue), env)
    Validation.validate_option!(:expression, Keyword.fetch!(attrs, :expression), env)

    [:max_attempts, :base_delay, :timeout]
    |> Enum.filter(&Keyword.has_key?(attrs, &1))
    |> Enum.each(&Validation.validate_option!(&1, Keyword.fetch!(attrs, &1), env))

    if Keyword.has_key?(attrs, :input),
      do: Validation.validate_option!(:input, Keyword.fetch!(attrs, :input), env)
  end

  defp validate_cron_expression!(expression, env) do
    case Crontab.CronExpression.Parser.parse(expression) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Validation.compile_error!(
          env,
          "Invalid cron expression #{inspect(expression)}: #{inspect(reason)}"
        )
    end
  end
end
