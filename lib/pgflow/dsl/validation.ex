defmodule PgFlow.DSL.Validation do
  @moduledoc """
  Compile-time validation helpers for PgFlow DSL macros.

  Used internally by `PgFlow.Flow` and `PgFlow.Job` to
  validate module attributes during compilation.
  """

  alias Crontab.CronExpression.Parser, as: CronParser

  @doc false
  @spec compile_error!(Macro.Env.t(), String.t()) :: no_return()
  def compile_error!(env, description),
    do: raise(CompileError, file: env.file, line: env.line, description: description)

  @doc false
  @spec validate_unknown_keys!(keyword(), [atom()], atom(), Macro.Env.t()) :: :ok | no_return()
  def validate_unknown_keys!(attrs, valid_keys, attr_name, env) do
    case Keyword.keys(attrs) -- valid_keys do
      [] ->
        :ok

      unknown ->
        compile_error!(
          env,
          "Unknown @#{attr_name} option(s): #{inspect(unknown)}. Valid options are: #{inspect(valid_keys)}"
        )
    end
  end

  @doc false
  @spec validate_required_keys!(keyword(), [atom()], atom(), Macro.Env.t()) :: :ok | no_return()
  def validate_required_keys!(attrs, required_keys, attr_name, env) do
    Enum.each(required_keys, fn key ->
      unless Keyword.has_key?(attrs, key) do
        compile_error!(
          env,
          "Missing :#{key} in @#{attr_name} attribute. You must define @#{attr_name} with a :#{key} option."
        )
      end
    end)
  end

  @doc false
  @spec validate_single_step!(list(), String.t(), Macro.Env.t()) :: :ok | no_return()
  def validate_single_step!([_single], _message, _env), do: :ok

  def validate_single_step!(_steps, message, env),
    do: compile_error!(env, message)

  @doc false
  @spec validate_option!(atom(), term(), Macro.Env.t()) :: :ok | no_return()
  def validate_option!(:queue, val, _env) when is_atom(val), do: :ok

  def validate_option!(:queue, val, env),
    do: compile_error!(env, ":queue must be an atom, got: #{inspect(val)}")

  def validate_option!(:slug, val, _env) when is_atom(val), do: :ok

  def validate_option!(:slug, val, env),
    do: compile_error!(env, ":slug must be an atom, got: #{inspect(val)}")

  def validate_option!(:max_attempts, val, _env) when is_integer(val) and val > 0, do: :ok

  def validate_option!(:max_attempts, val, env),
    do: compile_error!(env, ":max_attempts must be a positive integer, got: #{inspect(val)}")

  def validate_option!(:base_delay, val, _env) when is_integer(val) and val >= 0, do: :ok

  def validate_option!(:base_delay, val, env),
    do: compile_error!(env, ":base_delay must be a non-negative integer, got: #{inspect(val)}")

  def validate_option!(:timeout, val, _env) when is_integer(val) and val > 0, do: :ok

  def validate_option!(:timeout, val, env),
    do: compile_error!(env, ":timeout must be a positive integer, got: #{inspect(val)}")

  def validate_option!(:schedule, val, _env) when is_binary(val), do: :ok

  def validate_option!(:schedule, val, env),
    do: compile_error!(env, ":schedule must be a string, got: #{inspect(val)}")

  def validate_option!(:input, val, _env) when is_map(val), do: :ok

  def validate_option!(:input, val, env),
    do: compile_error!(env, ":input must be a map, got: #{inspect(val)}")

  @step_valid_keys [
    :depends_on,
    :handler,
    :max_attempts,
    :base_delay,
    :timeout,
    :start_delay,
    :array,
    :if,
    :if_not,
    :when_unmet,
    :when_exhausted
  ]

  @skip_modes [:fail, :skip, :skip_cascade]

  @doc false
  @spec validate_step_opts!(keyword(), Macro.Env.t()) :: :ok | no_return()
  def validate_step_opts!(opts, env) when is_list(opts) do
    validate_unknown_keys!(opts, @step_valid_keys, :step, env)

    if Keyword.has_key?(opts, :if), do: validate_pattern!(:if, Keyword.fetch!(opts, :if), env)

    if Keyword.has_key?(opts, :if_not),
      do: validate_pattern!(:if_not, Keyword.fetch!(opts, :if_not), env)

    if Keyword.has_key?(opts, :when_unmet) do
      unless Keyword.has_key?(opts, :if) or Keyword.has_key?(opts, :if_not) do
        compile_error!(env, "when_unmet requires :if or :if_not")
      end

      validate_mode!(:when_unmet, Keyword.fetch!(opts, :when_unmet), env)
    end

    if Keyword.has_key?(opts, :when_exhausted) do
      validate_mode!(:when_exhausted, Keyword.fetch!(opts, :when_exhausted), env)
    end

    :ok
  end

  defp validate_pattern!(_key, val, _env) when is_map(val), do: :ok

  defp validate_pattern!(key, val, env),
    do: compile_error!(env, ":#{key} must be a map, got: #{inspect(val)}")

  defp validate_mode!(_key, val, _env) when val in @skip_modes, do: :ok

  defp validate_mode!(key, val, env),
    do: compile_error!(env, ":#{key} must be :fail, :skip, or :skip_cascade, got: #{inspect(val)}")

  @cron_valid_keys [:schedule, :input]

  @doc """
  Validates the cron option.

  Accepts either:
  - A string shorthand: `cron: "@hourly"` (equivalent to `cron: [schedule: "@hourly"]`)
  - A keyword list: `cron: [schedule: "@hourly", input: %{key: "value"}]`

  Options when using keyword list:
  - `:schedule` is required and must be a valid cron expression string
  - `:input` is optional and must be a map (defaults to `%{}`)

  Returns `{schedule, input}` tuple on success.
  """
  @spec validate_cron_option!(String.t() | keyword(), Macro.Env.t()) ::
          {String.t(), map()} | no_return()
  def validate_cron_option!(schedule, env) when is_binary(schedule) do
    # Shorthand: cron: "@hourly" is equivalent to cron: [schedule: "@hourly"]
    validate_cron_option!([schedule: schedule], env)
  end

  def validate_cron_option!(cron_opts, env) when is_list(cron_opts) do
    validate_no_unknown_cron_keys!(cron_opts, env)
    validate_has_schedule!(cron_opts, env)

    schedule = Keyword.fetch!(cron_opts, :schedule)
    input = Keyword.get(cron_opts, :input, %{})

    validate_schedule_type!(schedule, env)
    validate_input_type!(input, env)
    validate_input_sql_safe!(input, env)
    validate_cron_schedule!(schedule, env)

    {schedule, input}
  end

  def validate_cron_option!(cron_opts, env) do
    compile_error!(
      env,
      "cron option must be a string or keyword list, got: #{inspect(cron_opts)}"
    )
  end

  defp validate_no_unknown_cron_keys!(cron_opts, env) do
    case Keyword.keys(cron_opts) -- @cron_valid_keys do
      [] ->
        :ok

      unknown ->
        compile_error!(
          env,
          "Unknown cron option(s): #{inspect(unknown)}. Valid options are: #{inspect(@cron_valid_keys)}"
        )
    end
  end

  defp validate_has_schedule!(cron_opts, env) do
    with false <- Keyword.has_key?(cron_opts, :schedule) do
      compile_error!(
        env,
        "Missing :schedule in cron option. You must provide cron: [schedule: \"...\"]"
      )
    end
  end

  defp validate_schedule_type!(schedule, _env) when is_binary(schedule), do: :ok

  defp validate_schedule_type!(schedule, env) do
    compile_error!(env, "cron :schedule must be a string, got: #{inspect(schedule)}")
  end

  defp validate_input_type!(input, _env) when is_map(input), do: :ok

  defp validate_input_type!(input, env) do
    compile_error!(env, "cron :input must be a map, got: #{inspect(input)}")
  end

  defp validate_input_sql_safe!(input, env) do
    json_input = Jason.encode!(input)

    with true <- String.contains?(json_input, "$$") do
      compile_error!(
        env,
        "cron :input values cannot contain the sequence '$$' as it breaks SQL dollar-quoting"
      )
    end
  end

  defp validate_cron_schedule!(schedule, env) do
    case CronParser.parse(schedule) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        compile_error!(
          env,
          "Invalid cron schedule #{inspect(schedule)}: #{inspect(reason)}"
        )
    end
  end
end
