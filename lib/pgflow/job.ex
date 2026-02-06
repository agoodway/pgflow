defmodule PgFlow.Job do
  @moduledoc """
  A macro-based DSL for defining pgflow background jobs.

  Jobs are single-step flows under the hood, providing a simpler API for
  one-off background processing. Use `use PgFlow.Job` in your job module.

  ## Example

      defmodule MyApp.Jobs.SendEmail do
        use PgFlow.Job

        @job queue: :send_email, max_attempts: 3, base_delay: 5, timeout: 60

        perform do
          fn input, _ctx ->
            Mailer.send(input["to"], input["subject"], input["body"])
            %{sent: true}
          end
        end
      end

  ## Job Options

  The `@job` module attribute accepts the following options:

    * `:queue` - (required) atom identifier for the job queue (becomes the flow slug)
    * `:max_attempts` - maximum retry attempts for failed jobs (default: 1)
    * `:base_delay` - base delay in seconds for exponential backoff (default: 1)
    * `:timeout` - job execution timeout in seconds (default: 30)

  ## Generated Functions

  Using this module generates the following callback functions:

    * `__pgflow_definition__/0` - returns a `PgFlow.Flow.Definition` struct with `flow_type: :job`
    * `__pgflow_slug__/0` - returns the job queue slug atom
    * `__pgflow_steps__/0` - returns the raw step definitions (single `:perform` step)
    * `__pgflow_handler__/0` - returns the perform handler function
    * `__pgflow_handler__(:perform)` - returns the perform handler function
    * `perform/2` - convenience wrapper for testing: `perform(input, ctx)`

  """

  @doc """
  Defines the job's perform block.

  The block must return a 2-arity function that receives the job input
  and a `PgFlow.Context` struct.

  ## Examples

      perform do
        fn input, _ctx ->
          %{result: process(input["data"])}
        end
      end

  """
  defmacro perform(do: block) do
    quote do
      @pgflow_steps {:perform, :step, [], unquote(Macro.escape(block))}
    end
  end

  defmacro __using__(_opts) do
    quote do
      import PgFlow.Job, only: [perform: 1]

      Module.register_attribute(__MODULE__, :job, persist: false)
      Module.register_attribute(__MODULE__, :pgflow_steps, accumulate: true, persist: false)

      @before_compile PgFlow.Job
    end
  end

  @valid_keys [:queue, :max_attempts, :base_delay, :timeout]

  defmacro __before_compile__(env) do
    job_attrs = Module.get_attribute(env.module, :job)
    steps = Module.get_attribute(env.module, :pgflow_steps) |> Enum.reverse()

    validate_job_attrs!(job_attrs, env)
    validate_steps!(steps, env)

    slug = Keyword.fetch!(job_attrs, :queue)
    max_attempts = Keyword.get(job_attrs, :max_attempts, 1)
    base_delay = Keyword.get(job_attrs, :base_delay, 1)
    timeout = Keyword.get(job_attrs, :timeout, 30)

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
      Returns the job queue slug.
      """
      def __pgflow_slug__, do: unquote(slug)

      @doc """
      Returns the raw step definitions.
      """
      def __pgflow_steps__, do: unquote(Macro.escape(steps))

      @doc """
      Returns the flow definition struct (with flow_type: :job).
      """
      def __pgflow_definition__ do
        %PgFlow.Flow.Definition{
          slug: unquote(slug),
          module: __MODULE__,
          steps: [unquote(Macro.escape(step_def))],
          opts: unquote(Macro.escape(flow_opts)),
          flow_type: :job
        }
      end

      @doc """
      Returns the perform handler function.
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
      Convenience wrapper for calling the job handler directly.

      Useful for testing:

          result = MyJob.perform(%{"key" => "value"}, ctx)

      """
      def perform(input, ctx) do
        handler = __pgflow_handler__(:perform)
        handler.(input, ctx)
      end
    end
  end

  # --- Validation helpers ---

  defp validate_job_attrs!(nil, env),
    do:
      compile_error!(
        env,
        "Missing @job attribute. You must define @job with at least a :queue option."
      )

  defp validate_job_attrs!(attrs, env) do
    validate_required_queue!(attrs, env)
    validate_unknown_keys!(attrs, env)
    validate_option_values!(attrs, env)
  end

  defp validate_steps!([_single], _env), do: :ok

  defp validate_steps!(_steps, env),
    do: compile_error!(env, "Jobs must have exactly one `perform` block.")

  defp validate_required_queue!(attrs, env) do
    unless Keyword.has_key?(attrs, :queue),
      do:
        compile_error!(
          env,
          "Missing :queue in @job attribute. You must define @job with a :queue option."
        )
  end

  defp validate_unknown_keys!(attrs, env) do
    case Keyword.keys(attrs) -- @valid_keys do
      [] ->
        :ok

      unknown ->
        compile_error!(
          env,
          "Unknown @job option(s): #{inspect(unknown)}. Valid options are: #{inspect(@valid_keys)}"
        )
    end
  end

  defp validate_option_values!(attrs, env) do
    validate_option!(:queue, Keyword.fetch!(attrs, :queue), env)

    [:max_attempts, :base_delay, :timeout]
    |> Enum.filter(&Keyword.has_key?(attrs, &1))
    |> Enum.each(&validate_option!(&1, Keyword.fetch!(attrs, &1), env))
  end

  defp validate_option!(:queue, val, _env) when is_atom(val), do: :ok

  defp validate_option!(:queue, val, env),
    do: compile_error!(env, ":queue must be an atom, got: #{inspect(val)}")

  defp validate_option!(:max_attempts, val, _env) when is_integer(val) and val > 0, do: :ok

  defp validate_option!(:max_attempts, val, env),
    do: compile_error!(env, ":max_attempts must be a positive integer, got: #{inspect(val)}")

  defp validate_option!(:base_delay, val, _env) when is_integer(val) and val >= 0, do: :ok

  defp validate_option!(:base_delay, val, env),
    do: compile_error!(env, ":base_delay must be a non-negative integer, got: #{inspect(val)}")

  defp validate_option!(:timeout, val, _env) when is_integer(val) and val > 0, do: :ok

  defp validate_option!(:timeout, val, env),
    do: compile_error!(env, ":timeout must be a positive integer, got: #{inspect(val)}")

  defp compile_error!(env, description),
    do: raise(CompileError, file: env.file, line: env.line, description: description)
end
