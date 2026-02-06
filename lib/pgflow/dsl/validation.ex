defmodule PgFlow.DSL.Validation do
  @moduledoc """
  Compile-time validation helpers for PgFlow DSL macros.

  Used internally by `PgFlow.Flow`, `PgFlow.Job`, and `PgFlow.Cron` to
  validate module attributes during compilation.
  """

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

  def validate_option!(:max_attempts, val, _env) when is_integer(val) and val > 0, do: :ok

  def validate_option!(:max_attempts, val, env),
    do: compile_error!(env, ":max_attempts must be a positive integer, got: #{inspect(val)}")

  def validate_option!(:base_delay, val, _env) when is_integer(val) and val >= 0, do: :ok

  def validate_option!(:base_delay, val, env),
    do: compile_error!(env, ":base_delay must be a non-negative integer, got: #{inspect(val)}")

  def validate_option!(:timeout, val, _env) when is_integer(val) and val > 0, do: :ok

  def validate_option!(:timeout, val, env),
    do: compile_error!(env, ":timeout must be a positive integer, got: #{inspect(val)}")

  def validate_option!(:expression, val, _env) when is_binary(val), do: :ok

  def validate_option!(:expression, val, env),
    do: compile_error!(env, ":expression must be a string, got: #{inspect(val)}")

  def validate_option!(:input, val, _env) when is_map(val), do: :ok

  def validate_option!(:input, val, env),
    do: compile_error!(env, ":input must be a map, got: #{inspect(val)}")
end
