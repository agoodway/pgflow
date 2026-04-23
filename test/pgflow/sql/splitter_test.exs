defmodule PgFlow.Sql.SplitterTest do
  use ExUnit.Case, async: true

  alias PgFlow.Sql.Splitter

  describe "split/1 — basics" do
    test "splits simple multi-statement input" do
      assert Splitter.split("SELECT 1; SELECT 2;") == ["SELECT 1", "SELECT 2"]
    end

    test "handles missing trailing semicolon" do
      assert Splitter.split("SELECT 1; SELECT 2") == ["SELECT 1", "SELECT 2"]
    end

    test "drops whitespace-only chunks" do
      assert Splitter.split("SELECT 1;   ;  SELECT 2;") == ["SELECT 1", "SELECT 2"]
    end

    test "drops comment-only chunks" do
      assert Splitter.split("-- just a comment\n;SELECT 1;") == ["SELECT 1"]
    end
  end

  describe "split/1 — comments" do
    test "ignores `;` inside line comments" do
      sql = """
      SELECT 1; -- comment with ; inside
      SELECT 2;
      """

      # The trailing `-- comment` stays attached to the next statement (Postgres
      # considers it part of the lead-in for SELECT 2). What matters is we get
      # exactly 2 statements and the `;` inside the comment didn't cut one in half.
      [s1, s2] = Splitter.split(sql)
      assert s1 == "SELECT 1"
      assert s2 =~ "SELECT 2"
      refute s1 =~ "comment"
    end

    test "ignores `;` inside block comments" do
      sql = "SELECT 1 /* block ; comment */; SELECT 2;"
      [s1, s2] = Splitter.split(sql)
      assert s1 =~ "SELECT 1"
      assert s1 =~ "/* block ; comment */"
      assert s2 == "SELECT 2"
    end
  end

  describe "split/1 — strings" do
    test "ignores `;` inside single-quoted strings" do
      sql = "INSERT INTO t VALUES ('a;b'); SELECT 1;"
      [s1, s2] = Splitter.split(sql)
      assert s1 == "INSERT INTO t VALUES ('a;b')"
      assert s2 == "SELECT 1"
    end

    test "respects `''` escape inside single quotes" do
      sql = "INSERT INTO t VALUES ('it''s; ok'); SELECT 2;"
      [s1, s2] = Splitter.split(sql)
      assert s1 == "INSERT INTO t VALUES ('it''s; ok')"
      assert s2 == "SELECT 2"
    end

    test "ignores `;` inside double-quoted identifiers" do
      sql = ~s|SELECT "weird;col" FROM t; SELECT 1;|
      assert Splitter.split(sql) == [~s|SELECT "weird;col" FROM t|, "SELECT 1"]
    end
  end

  describe "split/1 — dollar quotes" do
    test "handles bare `$$`" do
      sql = """
      CREATE FUNCTION f() RETURNS void AS $$
      BEGIN
        PERFORM 1;
        PERFORM 2;
      END;
      $$ LANGUAGE plpgsql;
      SELECT 1;
      """

      [fn_def, sel] = Splitter.split(sql)
      assert fn_def =~ "CREATE FUNCTION"
      assert fn_def =~ "PERFORM 1;"
      assert fn_def =~ "PERFORM 2;"
      assert sel == "SELECT 1"
    end

    test "handles tagged `$body$`" do
      sql = """
      CREATE FUNCTION f() RETURNS void AS $body$
      BEGIN
        PERFORM 1; PERFORM 2;
      END;
      $body$ LANGUAGE plpgsql;
      SELECT 9;
      """

      assert [fn_def, "SELECT 9"] = Splitter.split(sql)
      assert fn_def =~ "$body$"
    end

    test "distinguishes `$outer$` from `$inner$` (nested-looking tags)" do
      sql = """
      CREATE FUNCTION f() RETURNS void AS $outer$
        SELECT 'this $inner$ looks nested but is a string';
      $outer$ LANGUAGE sql;
      SELECT 1;
      """

      assert [_, "SELECT 1"] = Splitter.split(sql)
    end
  end

  describe "join/1" do
    test "wraps statements with --SPLIT-- delimiters" do
      assert Splitter.join(["SELECT 1", "SELECT 2"]) ==
               "SELECT 1\n\n--SPLIT--\n\nSELECT 2"
    end
  end

  describe "round-trip" do
    test "split then join preserves statements (modulo whitespace)" do
      sql = """
      CREATE TABLE t (id int);
      CREATE INDEX idx_t ON t(id);
      """

      statements = Splitter.split(sql)
      assert length(statements) == 2
      rejoined = Splitter.join(statements)
      assert rejoined =~ "--SPLIT--"
      assert rejoined =~ "CREATE TABLE t (id int)"
      assert rejoined =~ "CREATE INDEX idx_t ON t(id)"
    end
  end
end
