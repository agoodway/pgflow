defmodule PgFlowDashboard.Migrations.Versions.V01DownTest do
  @moduledoc """
  Contract checks that `v01_down.sql` is a complete uninstall of `v01_up.sql`.

  The DB-backed dashboard migration suite is tagged `:migration` and skipped
  wherever Postgres is absent, and it only exercises the v02 -> v01 hop (not
  the full teardown), so "v01_up creates an object that v01_down never drops"
  and "the DROP names a signature the CREATE never used" are asserted against
  the files here.

  This matters beyond tidiness: `v01_down.sql` ends with a bare
  `DROP SCHEMA IF EXISTS $SCHEMA$` (no CASCADE), which errors out if any
  object is left behind - a missed DROP breaks uninstall entirely.
  """
  use ExUnit.Case, async: true

  @up_path "priv/pgflow_dashboard/sql/versions/v01/v01_up.sql"
  @down_path "priv/pgflow_dashboard/sql/versions/v01/v01_down.sql"

  defp up_sql, do: File.read!(@up_path)
  defp down_sql, do: File.read!(@down_path)

  describe "v01_down.sql fully reverses v01_up.sql" do
    test "every created function is dropped with a matching signature" do
      created = created_functions(up_sql())
      dropped = dropped_functions(down_sql())

      assert created != [], "expected v01_up.sql to create functions"

      missing =
        Enum.reject(created, fn {name, arg_types} ->
          {name, arg_types} in dropped
        end)

      assert missing == [],
             """
             v01_down.sql does not drop these v01_up.sql functions with a matching signature:

             #{Enum.map_join(missing, "\n", fn {name, args} -> "  #{name}(#{Enum.join(args, ", ")})" end)}

             DROP statements present in v01_down.sql:

             #{Enum.map_join(dropped, "\n", fn {name, args} -> "  #{name}(#{Enum.join(args, ", ")})" end)}
             """
    end

    test "no DROP FUNCTION targets a signature v01_up.sql never created" do
      created = created_functions(up_sql())

      stray =
        down_sql()
        |> dropped_functions()
        |> Enum.reject(&(&1 in created))

      assert stray == [],
             """
             v01_down.sql drops signatures that v01_up.sql never creates (these
             DROPs are silent no-ops, leaving the real function installed):

             #{Enum.map_join(stray, "\n", fn {name, args} -> "  #{name}(#{Enum.join(args, ", ")})" end)}
             """
    end

    test "every created view is dropped" do
      created = created_objects(up_sql(), "VIEW")
      dropped = dropped_objects(down_sql(), "VIEW")

      assert created != []
      assert Enum.sort(created) == Enum.sort(dropped)
    end

    test "the schema itself is dropped last" do
      assert String.trim_trailing(down_sql()) =~ ~r/DROP SCHEMA IF EXISTS \$SCHEMA\$;\s*\z/
    end
  end

  # --- SQL parsing helpers -------------------------------------------------
  #
  # Deliberately minimal: enough to pull `name(argtypes)` out of the CREATE /
  # DROP statements this chain actually writes, with balanced-paren handling
  # (arg lists contain `DEFAULT (NOW() - INTERVAL '24 hours')`) and comment
  # stripping (arg lists contain trailing `-- 'next' or 'prev'` notes).

  defp created_functions(sql) do
    signatures(
      sql,
      ~r/CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+\$SCHEMA\$\.(\w+)\s*(\()/i,
      &declared_arg_type/1
    )
  end

  defp dropped_functions(sql) do
    signatures(
      sql,
      ~r/DROP\s+FUNCTION\s+(?:IF\s+EXISTS\s+)?\$SCHEMA\$\.(\w+)\s*(\()/i,
      &normalize_type/1
    )
  end

  defp signatures(sql, regex, arg_mapper) do
    sql = strip_comments(sql)

    regex
    |> Regex.scan(sql, return: :index)
    |> Enum.map(fn [_full, {name_start, name_len}, {paren_start, _}] ->
      name = binary_part(sql, name_start, name_len)
      args = sql |> balanced_args(paren_start) |> split_top_level() |> Enum.map(arg_mapper)
      {name, args}
    end)
  end

  defp created_objects(sql, keyword) do
    ~r/CREATE\s+(?:OR\s+REPLACE\s+)?#{keyword}\s+\$SCHEMA\$\.(\w+)/i
    |> Regex.scan(strip_comments(sql))
    |> Enum.map(fn [_, name] -> name end)
  end

  defp dropped_objects(sql, keyword) do
    ~r/DROP\s+#{keyword}\s+(?:IF\s+EXISTS\s+)?\$SCHEMA\$\.(\w+)/i
    |> Regex.scan(strip_comments(sql))
    |> Enum.map(fn [_, name] -> name end)
  end

  defp strip_comments(sql), do: Regex.replace(~r/--[^\n]*/, sql, "")

  # Returns the text between the `(` at `open_index` and its matching `)`.
  defp balanced_args(sql, open_index) do
    rest = binary_part(sql, open_index + 1, byte_size(sql) - open_index - 1)
    take_until_close(rest, 0, "")
  end

  defp take_until_close(<<")", _rest::binary>>, 0, acc), do: acc

  defp take_until_close(<<")", rest::binary>>, depth, acc),
    do: take_until_close(rest, depth - 1, acc <> ")")

  defp take_until_close(<<"(", rest::binary>>, depth, acc),
    do: take_until_close(rest, depth + 1, acc <> "(")

  defp take_until_close(<<c::utf8, rest::binary>>, depth, acc),
    do: take_until_close(rest, depth, acc <> <<c::utf8>>)

  defp take_until_close(<<>>, _depth, acc), do: acc

  defp split_top_level(args) do
    args
    |> do_split(0, "", [])
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end

  defp do_split(<<",", rest::binary>>, 0, current, acc),
    do: do_split(rest, 0, "", [current | acc])

  defp do_split(<<"(", rest::binary>>, depth, current, acc),
    do: do_split(rest, depth + 1, current <> "(", acc)

  defp do_split(<<")", rest::binary>>, depth, current, acc),
    do: do_split(rest, depth - 1, current <> ")", acc)

  defp do_split(<<c::utf8, rest::binary>>, depth, current, acc),
    do: do_split(rest, depth, current <> <<c::utf8>>, acc)

  defp do_split(<<>>, _depth, current, acc), do: Enum.reverse([current | acc])

  # `p_limit integer DEFAULT 50` -> "integer"
  defp declared_arg_type(arg) do
    arg
    |> String.split(~r/\s+DEFAULT\s+/i, parts: 2)
    |> hd()
    |> String.split(~r/\s+/, trim: true)
    |> case do
      [_name | [_ | _] = type] -> Enum.join(type, " ")
      [type] -> type
    end
    |> normalize_type()
  end

  defp normalize_type(type) do
    type
    |> String.downcase()
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
    |> case do
      "int" -> "integer"
      "int4" -> "integer"
      "int8" -> "bigint"
      "timestamp with time zone" -> "timestamptz"
      other -> other
    end
  end
end
