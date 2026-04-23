defmodule PgFlow.Sql.Splitter do
  @moduledoc """
  Splits a raw PostgreSQL script into individual top-level statements.

  Needed because `Ecto.Migration.execute/1` dispatches through Postgrex
  prepared-statement protocol, which rejects multi-command input with
  `42601 syntax_error`. EctoEvolver runs each `--SPLIT--`-delimited chunk
  as a single `execute/1` call, so the vendored SQL has to be pre-split
  one-statement-per-chunk.

  ## State machine

  Top-level splitting ignores semicolons inside:

    * single-quoted strings (`'...'` with `''` escape)
    * double-quoted identifiers (`"..."` with `""` escape)
    * dollar-quoted bodies (`$$ ... $$` or `$tag$ ... $tag$`)
    * single-line comments (`-- ...\\n`)
    * block comments (`/* ... */`, non-nested per Postgres grammar)

  Only a `;` seen in the top-level state ends a statement. Leading
  whitespace and trailing whitespace on each statement are stripped; empty
  chunks (whitespace / comments only) are discarded.

  ## Output shape

      iex> PgFlow.Sql.Splitter.split("SELECT 1; SELECT 2;")
      ["SELECT 1", "SELECT 2"]

      iex> PgFlow.Sql.Splitter.split("-- comment\\nSELECT 1;")
      ["-- comment\\nSELECT 1"]

  Returns a list of trimmed statement strings in source order.
  """

  @doc "Split a SQL script into top-level statements."
  @spec split(binary()) :: [String.t()]
  def split(sql) when is_binary(sql) do
    sql
    |> scan("", [], :top)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&empty_or_comment_only?/1)
  end

  @doc "Join statements back into a single script with `--SPLIT--` markers."
  @spec join([String.t()]) :: String.t()
  def join(statements) when is_list(statements) do
    Enum.map_join(statements, "\n\n--SPLIT--\n\n", & &1)
  end

  # ── scanner ──────────────────────────────────────────────────────────────

  # Top-level: look for comments, strings, dollar quotes, or `;`.
  defp scan("", acc, stmts, :top), do: Enum.reverse(finalize(acc, stmts))

  defp scan("--" <> rest, acc, stmts, :top) do
    scan(rest, acc <> "--", stmts, :line_comment)
  end

  defp scan("/*" <> rest, acc, stmts, :top) do
    scan(rest, acc <> "/*", stmts, :block_comment)
  end

  defp scan("'" <> rest, acc, stmts, :top) do
    scan(rest, acc <> "'", stmts, :single_quote)
  end

  defp scan("\"" <> rest, acc, stmts, :top) do
    scan(rest, acc <> "\"", stmts, :double_quote)
  end

  defp scan(";" <> rest, acc, stmts, :top) do
    scan(rest, "", [acc | stmts], :top)
  end

  defp scan(<<"$", tail::binary>> = input, acc, stmts, :top) do
    case take_dollar_tag(tail) do
      {:ok, tag, rest} ->
        opener = "$" <> tag <> "$"
        scan(rest, acc <> opener, stmts, {:dollar_quote, tag})

      :no_match ->
        <<c::utf8, rest::binary>> = input
        scan(rest, acc <> <<c::utf8>>, stmts, :top)
    end
  end

  defp scan(<<c::utf8, rest::binary>>, acc, stmts, :top) do
    scan(rest, acc <> <<c::utf8>>, stmts, :top)
  end

  # Line comment: ends at newline. Keep the newline in the statement.
  defp scan("", acc, stmts, :line_comment), do: Enum.reverse(finalize(acc, stmts))

  defp scan("\n" <> rest, acc, stmts, :line_comment) do
    scan(rest, acc <> "\n", stmts, :top)
  end

  defp scan(<<c::utf8, rest::binary>>, acc, stmts, :line_comment) do
    scan(rest, acc <> <<c::utf8>>, stmts, :line_comment)
  end

  # Block comment: terminated by `*/`. Postgres does NOT nest these.
  defp scan("", acc, stmts, :block_comment), do: Enum.reverse(finalize(acc, stmts))

  defp scan("*/" <> rest, acc, stmts, :block_comment) do
    scan(rest, acc <> "*/", stmts, :top)
  end

  defp scan(<<c::utf8, rest::binary>>, acc, stmts, :block_comment) do
    scan(rest, acc <> <<c::utf8>>, stmts, :block_comment)
  end

  # Single-quoted string: `''` is an escape for a literal `'`.
  defp scan("", acc, stmts, :single_quote), do: Enum.reverse(finalize(acc, stmts))

  defp scan("''" <> rest, acc, stmts, :single_quote) do
    scan(rest, acc <> "''", stmts, :single_quote)
  end

  defp scan("'" <> rest, acc, stmts, :single_quote) do
    scan(rest, acc <> "'", stmts, :top)
  end

  defp scan(<<c::utf8, rest::binary>>, acc, stmts, :single_quote) do
    scan(rest, acc <> <<c::utf8>>, stmts, :single_quote)
  end

  # Double-quoted identifier: `""` escapes a literal `"`.
  defp scan("", acc, stmts, :double_quote), do: Enum.reverse(finalize(acc, stmts))

  defp scan(~s("") <> rest, acc, stmts, :double_quote) do
    scan(rest, acc <> ~s(""), stmts, :double_quote)
  end

  defp scan("\"" <> rest, acc, stmts, :double_quote) do
    scan(rest, acc <> "\"", stmts, :top)
  end

  defp scan(<<c::utf8, rest::binary>>, acc, stmts, :double_quote) do
    scan(rest, acc <> <<c::utf8>>, stmts, :double_quote)
  end

  # Dollar-quoted body: ends only at the matching `$tag$`.
  defp scan("", acc, stmts, {:dollar_quote, _}), do: Enum.reverse(finalize(acc, stmts))

  defp scan(<<"$", rest::binary>> = input, acc, stmts, {:dollar_quote, tag}) do
    closer = tag <> "$"

    if String.starts_with?(rest, closer) do
      remainder = binary_part(rest, byte_size(closer), byte_size(rest) - byte_size(closer))
      scan(remainder, acc <> "$" <> closer, stmts, :top)
    else
      <<c::utf8, tail::binary>> = input
      scan(tail, acc <> <<c::utf8>>, stmts, {:dollar_quote, tag})
    end
  end

  defp scan(<<c::utf8, rest::binary>>, acc, stmts, {:dollar_quote, tag}) do
    scan(rest, acc <> <<c::utf8>>, stmts, {:dollar_quote, tag})
  end

  # ── helpers ──────────────────────────────────────────────────────────────

  # A dollar tag is `[A-Za-z_][A-Za-z0-9_]*` optionally empty, followed by `$`.
  # Example tokens: `$$`, `$body$`, `$BODY$`, `$_$`.
  defp take_dollar_tag(bin) do
    case Regex.run(~r/^([A-Za-z_][A-Za-z0-9_]*)?\$/, bin, return: :index) do
      [{0, len} | _] ->
        full = binary_part(bin, 0, len)
        # full is `<tag>$`; strip trailing `$` to get just the tag.
        tag = binary_part(full, 0, byte_size(full) - 1)
        rest = binary_part(bin, len, byte_size(bin) - len)
        {:ok, tag, rest}

      _ ->
        :no_match
    end
  end

  defp finalize("", acc), do: acc
  defp finalize(partial, acc), do: [partial | acc]

  # Chunks that are only whitespace or `--` line / `/* */` block comments
  # aren't real statements — discard them.
  defp empty_or_comment_only?(""), do: true

  defp empty_or_comment_only?(str) do
    stripped =
      str
      |> String.replace(~r|/\*.*?\*/|s, "")
      |> String.replace(~r|^\s*--[^\n]*\n?|m, "")
      |> String.trim()

    stripped == ""
  end
end
