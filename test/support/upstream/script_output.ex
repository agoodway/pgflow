defmodule PgFlow.Test.UpstreamScriptOutput do
  @moduledoc false
  # Parses the output of the upstream Node scripts the harness and interop runner start.

  @excerpt_length 800

  @doc """
  Decodes the last line of `output` that starts with `{` as JSON.

  Scripts log to stderr, which may be mixed into stdout, so only the JSON line
  is read. Returns `:error` when no such line exists.
  """
  @spec decode_json(String.t()) :: {:ok, term()} | {:error, Jason.DecodeError.t()} | :error
  def decode_json(output) do
    case extract_json_line(output) do
      nil -> :error
      json -> Jason.decode(json)
    end
  end

  @doc "Returns at most the last #{@excerpt_length} characters of the trimmed `output`."
  @spec excerpt(String.t()) :: String.t()
  def excerpt(output) do
    output = String.trim(output)

    if String.length(output) <= @excerpt_length,
      do: output,
      else: String.slice(output, -@excerpt_length, @excerpt_length)
  end

  defp extract_json_line(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.reverse()
    |> Enum.find(&String.starts_with?(&1, "{"))
  end
end
