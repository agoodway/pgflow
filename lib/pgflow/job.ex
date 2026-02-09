defmodule PgFlow.Job do
  @moduledoc """
  A macro-based DSL for defining pgflow background jobs.

  Jobs are single-step flows under the hood, providing a simpler API for
  one-off background processing. Use `use PgFlow.Job` in your job module.

  ## Example

      defmodule MyApp.Jobs.SendEmail do
        use PgFlow.Job

        @job queue: :send_email, max_attempts: 3, base_delay: 5, timeout: 60

        perform :send do
          fn input, _ctx ->
            Mailer.send(input["to"], input["subject"], input["body"])
            %{sent: true}
          end
        end
      end

  The optional name in `perform :name do` sets the step slug in the database.
  When omitted, the step slug defaults to the `@job` queue/slug value:

      @job queue: :cleanup
      perform do  # step slug will be :cleanup
        fn _input, _ctx -> :ok end
      end

  ## Job Options

  The `@job` module attribute accepts the following options:

    * `:queue` - (required) atom identifier for the job queue (also accepts `:slug` as alias)
    * `:max_attempts` - maximum retry attempts for failed jobs (default: 1)
    * `:base_delay` - base delay in seconds for exponential backoff (default: 1)
    * `:timeout` - job execution timeout in seconds (default: 30)

  ## Generated Functions

  Using this module generates the following callback functions:

    * `__pgflow_definition__/0` - returns a `PgFlow.Flow.Definition` struct with `flow_type: :job`
    * `__pgflow_slug__/0` - returns the job queue slug atom
    * `__pgflow_steps__/0` - returns the raw step definitions
    * `__pgflow_handler__/0` - returns the handler function
    * `__pgflow_handler__(:step_name)` - returns the handler function by step name
    * `perform/2` - convenience wrapper for testing: `perform(input, ctx)`

  """

  @doc """
  Defines the job's perform block with an optional step name.

  When called with a name (`perform :step_name do`), that name becomes
  the step slug in the database. When called without a name (`perform do`),
  the step slug defaults to the `@job` queue/slug value.

  The block must return a 2-arity function that receives the job input
  and a `PgFlow.Context` struct.

  ## Examples

      # Explicit step name
      perform :send_email do
        fn input, _ctx ->
          %{result: process(input["data"])}
        end
      end

      # Step name defaults to @job queue/slug
      perform do
        fn input, _ctx ->
          %{result: process(input["data"])}
        end
      end

  """
  defmacro perform(name, do: block) do
    quote do
      @pgflow_steps {unquote(name), :step, [], unquote(Macro.escape(block))}
    end
  end

  defmacro perform(do: block) do
    quote do
      @pgflow_steps {nil, :step, [], unquote(Macro.escape(block))}
    end
  end

  @doc false
  defmacro __using__(_opts) do
    quote do
      import PgFlow.Job, only: [perform: 1, perform: 2]

      Module.register_attribute(__MODULE__, :job, accumulate: true, persist: false)
      Module.register_attribute(__MODULE__, :pgflow_steps, accumulate: true, persist: false)

      @before_compile PgFlow.Job
    end
  end

  alias PgFlow.DSL.Validation
  alias PgFlow.Flow.{Definition, Step}

  @valid_keys [:queue, :slug, :max_attempts, :base_delay, :timeout, :cron]

  @doc false
  defmacro __before_compile__(env) do
    job_attrs_list = Module.get_attribute(env.module, :job)
    steps = Module.get_attribute(env.module, :pgflow_steps) |> Enum.reverse()

    job_attrs = validate_and_extract_attrs!(job_attrs_list, steps, env)
    {slug, flow_opts, cron_expression, cron_input} = extract_job_config(job_attrs, env)
    {step_name, block} = extract_step(steps, slug)
    step_def = build_step_def(step_name, flow_opts)

    generate_job_functions(
      slug,
      steps,
      step_def,
      flow_opts,
      step_name,
      block,
      cron_expression,
      cron_input
    )
  end

  # Extract the step name and block; nil name defaults to flow slug
  defp extract_step([{nil, :step, _opts, block}], slug), do: {slug, block}
  defp extract_step([{name, :step, _opts, block}], _slug), do: {name, block}

  defp generate_job_functions(
         slug,
         steps,
         step_def,
         flow_opts,
         step_name,
         block,
         cron_expression,
         cron_input
       ) do
    quote do
      def __pgflow_slug__, do: unquote(slug)
      def __pgflow_steps__, do: unquote(Macro.escape(steps))

      def __pgflow_definition__ do
        %unquote(Definition){
          slug: unquote(slug),
          module: __MODULE__,
          steps: [unquote(Macro.escape(step_def))],
          opts: unquote(Macro.escape(flow_opts)),
          flow_type: :job
        }
      end

      def __pgflow_handler__(unquote(step_name)), do: unquote(block)
      def __pgflow_handler__, do: unquote(block)
      def __pgflow_handler__(slug), do: raise("No handler defined for step: #{inspect(slug)}")

      def perform(input, ctx) do
        handler = __pgflow_handler__(unquote(step_name))
        handler.(input, ctx)
      end

      def __pgflow_cron_expression__, do: unquote(cron_expression)
      def __pgflow_cron_input__, do: unquote(Macro.escape(cron_input))
    end
  end

  # --- Validation helpers ---

  # Combined validation to reduce __before_compile__ complexity
  defp validate_and_extract_attrs!(job_attrs_list, steps, env) do
    job_attrs = validate_job_attr_count!(job_attrs_list, env)
    validate_job_attrs!(job_attrs, env)
    validate_steps!(steps, env)
    job_attrs
  end

  defp validate_job_attr_count!([], _env), do: nil
  defp validate_job_attr_count!([single], _env), do: single

  defp validate_job_attr_count!(_multiple, env),
    do:
      Validation.compile_error!(
        env,
        "Multiple @job attributes defined. Only one @job attribute is allowed per module."
      )

  defp validate_job_attrs!(nil, env),
    do:
      Validation.compile_error!(
        env,
        "Missing @job attribute. You must define @job with at least a :queue option."
      )

  defp validate_job_attrs!(attrs, env) do
    validate_has_queue_or_slug!(attrs, env)
    Validation.validate_unknown_keys!(attrs, @valid_keys, :job, env)
    validate_option_values!(attrs, env)
  end

  defp validate_has_queue_or_slug!(attrs, env) do
    unless Keyword.has_key?(attrs, :queue) or Keyword.has_key?(attrs, :slug) do
      Validation.compile_error!(
        env,
        "Missing :queue in @job attribute. You must define @job with a :queue option."
      )
    end
  end

  # Extract all job configuration from attrs
  defp extract_job_config(attrs, env) do
    slug = Keyword.get(attrs, :queue) || Keyword.fetch!(attrs, :slug)
    max_attempts = Keyword.get(attrs, :max_attempts, 1)
    base_delay = Keyword.get(attrs, :base_delay, 1)
    timeout = Keyword.get(attrs, :timeout, 30)

    {cron_expression, cron_input} = extract_cron_config(attrs, env)

    flow_opts = [
      max_attempts: max_attempts,
      base_delay: base_delay,
      timeout: timeout
    ]

    {slug, flow_opts, cron_expression, cron_input}
  end

  defp extract_cron_config(attrs, env) do
    case Keyword.get(attrs, :cron) do
      nil -> {nil, %{}}
      opts -> Validation.validate_cron_option!(opts, env)
    end
  end

  defp build_step_def(step_name, flow_opts) do
    %Step{
      slug: step_name,
      step_type: :single,
      depends_on: [],
      max_attempts: flow_opts[:max_attempts],
      base_delay: flow_opts[:base_delay],
      timeout: flow_opts[:timeout],
      start_delay: 0
    }
  end

  defp validate_steps!(steps, env),
    do:
      Validation.validate_single_step!(steps, "Jobs must have exactly one `perform` block.", env)

  defp validate_option_values!(attrs, env) do
    # Validate :queue if present
    if Keyword.has_key?(attrs, :queue) do
      Validation.validate_option!(:queue, Keyword.fetch!(attrs, :queue), env)
    end

    # Validate :slug if present
    if Keyword.has_key?(attrs, :slug) do
      Validation.validate_option!(:slug, Keyword.fetch!(attrs, :slug), env)
    end

    [:max_attempts, :base_delay, :timeout]
    |> Enum.filter(&Keyword.has_key?(attrs, &1))
    |> Enum.each(&Validation.validate_option!(&1, Keyword.fetch!(attrs, &1), env))
  end
end
