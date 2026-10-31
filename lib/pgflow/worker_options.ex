defmodule PgFlow.WorkerOptions do
  @moduledoc """
  Validation and resolution of per-worker `max_concurrency` and `batch_size` options,
  and of the `:timeout` option that worker check and scale calls accept.
  """

  @worker_keys [:max_concurrency, :batch_size]

  @schema [
    max_concurrency: [type: :pos_integer],
    batch_size: [type: :pos_integer]
  ]

  @default_call_timeout 5_000

  @call_schema [
    timeout: [type: :timeout, default: @default_call_timeout]
  ]

  @doc """
  Returns the allowlisted worker option keys.
  """
  @spec keys() :: [:max_concurrency | :batch_size]
  def keys, do: @worker_keys

  @doc """
  Validates worker option keywords.

  Raises `ArgumentError` on unknown keys or invalid values. An empty keyword list is valid.
  """
  @spec validate!(keyword()) :: keyword()
  def validate!(opts) when is_list(opts) do
    case NimbleOptions.validate(opts, @schema) do
      {:ok, validated} -> validated
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc """
  Validates scale options; at least one of `:max_concurrency` or `:batch_size` is required.
  """
  @spec validate_scale!(keyword()) :: keyword()
  def validate_scale!([]),
    do: raise(ArgumentError, "scale_worker requires :max_concurrency or :batch_size")

  def validate_scale!(opts) when is_list(opts), do: validate!(opts)

  @doc """
  Validates call options for `check_worker` and returns the call timeout.

  Accepts only `:timeout` (milliseconds or `:infinity`, default `#{@default_call_timeout}`).
  Raises `ArgumentError` on unknown keys or invalid values.
  """
  @spec validate_call!(keyword()) :: timeout()
  def validate_call!(opts) when is_list(opts) do
    case NimbleOptions.validate(opts, @call_schema) do
      {:ok, validated} -> Keyword.fetch!(validated, :timeout)
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc """
  Removes and validates `:timeout` from `opts`, returning `{timeout, rest}`.

  The timeout defaults to `#{@default_call_timeout}` milliseconds. Raises `ArgumentError`
  when it is not a non-negative integer or `:infinity`.
  """
  @spec pop_call_timeout!(keyword()) :: {timeout(), keyword()}
  def pop_call_timeout!(opts) when is_list(opts) do
    {call_opts, rest} = Keyword.split(opts, [:timeout])
    {validate_call!(call_opts), rest}
  end

  @doc """
  Splits worker options from other start options (for example `:repo`).
  """
  @spec split(keyword()) :: {keyword(), keyword()}
  def split(opts) when is_list(opts) do
    Enum.split_with(opts, fn {key, _value} -> key in @worker_keys end)
  end

  @doc """
  Resolves effective worker limits.

  Precedence: global config, then `worker_options[module]`, then `overrides`.
  `global` must carry both keys, which `PgFlow.Config.validate!/1` guarantees.
  """
  @spec resolve(keyword(), module(), keyword()) :: %{
          max_concurrency: pos_integer(),
          batch_size: pos_integer()
        }
  def resolve(global, module, overrides) when is_list(global) and is_list(overrides) do
    per_worker = global |> Keyword.get(:worker_options, %{}) |> Map.get(module, [])

    global
    |> Keyword.take(@worker_keys)
    |> Keyword.merge(per_worker)
    |> Keyword.merge(overrides)
    |> Keyword.take(@worker_keys)
    |> Map.new()
  end
end
