defmodule PgFlow.Upstream.BaselineTest do
  use ExUnit.Case, async: false

  alias PgFlow.Test.{DatabaseHelpers, UpstreamFixture}

  @moduletag :integration

  setup_all do
    fixture = UpstreamFixture.setup!()
    on_exit(fn -> UpstreamFixture.teardown!(fixture) end)
    %{fixture: fixture}
  end

  describe "isolated fixture lifecycle" do
    @tag :fixture_lifecycle
    test "setup transfers repository ownership to explicit teardown" do
      fixture = UpstreamFixture.setup!()
      on_exit(fn -> UpstreamFixture.teardown!(fixture) end)

      {:links, links} = Process.info(self(), :links)

      refute fixture.repo_pid in links
    end

    @tag :fixture_lifecycle
    test "resolves non-default helper connection values at call time" do
      config = Application.fetch_env!(:pgflow, PgFlow.TestRepo)
      real_port = Integer.to_string(config[:port])

      database =
        "pgflow_upstream_param_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

      admin_url = replace_database(UpstreamFixture.application_database_url(), "postgres")
      database_url = replace_database(admin_url, database)
      helper_path = Path.join(System.tmp_dir!(), "#{database}.sql")
      previous_database = System.get_env("PGFLOW_TEST_DATABASE")
      previous_port = System.get_env("PGFLOW_TEST_PORT")

      on_exit(fn ->
        restore_env("PGFLOW_TEST_DATABASE", previous_database)
        restore_env("PGFLOW_TEST_PORT", previous_port)
        File.rm(helper_path)
        drop_database(admin_url, database)
      end)

      admin_query!(admin_url, ~s(CREATE DATABASE "#{database}"))
      System.put_env("PGFLOW_TEST_DATABASE", database)
      System.put_env("PGFLOW_TEST_PORT", "65432")

      assert DatabaseHelpers.resolve_database(config) == database
      assert DatabaseHelpers.resolve_port(config) == 65_432

      assert DatabaseHelpers.psql_args(config, helper_path) == [
               "-h",
               config[:hostname],
               "-p",
               "65432",
               "-U",
               config[:username],
               "-d",
               database,
               "-v",
               "db_name=#{database}",
               "-v",
               "ON_ERROR_STOP=1",
               "-q",
               "-f",
               helper_path
             ]

      System.put_env("PGFLOW_TEST_PORT", real_port)

      File.write!(
        helper_path,
        ~s(ALTER DATABASE :"db_name" SET app.settings.jwt_secret = 'parameterized-secret';)
      )

      assert {_, 0} =
               System.cmd(
                 "psql",
                 DatabaseHelpers.psql_args(config, helper_path),
                 env: [{"PGPASSWORD", config[:password]}],
                 stderr_to_stdout: true
               )

      assert [["parameterized-secret"]] =
               query_rows!(database_url, "SHOW app.settings.jwt_secret")

      refute database == "pgflow_test"
    end

    @tag :fixture_lifecycle
    test "refuses to administer the ordinary application database" do
      assert_raise ArgumentError, ~r/ordinary test database/, fn ->
        UpstreamFixture.setup!(admin_url: UpstreamFixture.application_database_url())
      end
    end

    @tag :fixture_lifecycle
    test "teardown drops only its owned database and leaves the ordinary database intact" do
      fixture = UpstreamFixture.setup!()

      assert UpstreamFixture.database_exists?(fixture.admin_url, fixture.database)
      assert UpstreamFixture.database_exists?(fixture.admin_url, fixture.application_database)

      UpstreamFixture.teardown!(fixture)

      refute UpstreamFixture.database_exists?(fixture.admin_url, fixture.database)
      assert UpstreamFixture.database_exists?(fixture.admin_url, fixture.application_database)
    end

    @tag :fixture_lifecycle
    test "installs explicit baseline versions and representative data", %{fixture: fixture} do
      assert fixture.versions == %{core: 1, helpers: 4}
      assert fixture.database =~ ~r/^pgflow_upstream_fixture_[0-9a-f]{32}$/

      assert Atom.to_string(fixture.repo_name) ==
               String.replace_prefix(
                 fixture.database,
                 "pgflow_upstream_fixture_",
                 "pgflow_upstream_fixture_repo_"
               )

      assert UpstreamFixture.manifest()["target"]["upstream_sha"] ==
               "94490709f79ebf366141dd925b047f0c1013e759"

      assert %{
               queued: queued_run,
               started: started_run,
               completed: completed_run,
               failed: failed_run
             } = fixture.runs

      assert [
               [^queued_run, "queued"],
               [^started_run, "started"],
               [^completed_run, "completed"],
               [^failed_run, "failed"]
             ] =
               UpstreamFixture.query_rows!(
                 fixture,
                 """
                 SELECT run_id, scenario
                 FROM (
                   SELECT run_id, 'queued' AS scenario FROM pgflow.step_tasks WHERE run_id = $1 AND status = 'queued'
                   UNION ALL
                   SELECT run_id, 'started' FROM pgflow.step_tasks WHERE run_id = $2 AND status = 'started'
                   UNION ALL
                   SELECT run_id, 'completed' FROM pgflow.step_tasks WHERE run_id = $3 AND status = 'completed'
                   UNION ALL
                   SELECT run_id, 'failed' FROM pgflow.step_tasks WHERE run_id = $4 AND status = 'failed'
                 ) scenarios
                 ORDER BY array_position(ARRAY['queued', 'started', 'completed', 'failed'], scenario)
                 """,
                 [queued_run, started_run, completed_run, failed_run]
               )

      assert [[1]] =
               UpstreamFixture.query_rows!(
                 fixture,
                 "SELECT count(*) FROM pgflow.flows WHERE flow_slug = $1 AND flow_type = 'job'",
                 [fixture.names.job_flow]
               )

      assert [[2]] =
               UpstreamFixture.query_rows!(
                 fixture,
                 "SELECT count(*) FROM pgflow.step_tasks WHERE run_id = $1 AND message_id IS NOT NULL",
                 [fixture.runs.map]
               )

      assert [[1]] =
               UpstreamFixture.query_rows!(
                 fixture,
                 "SELECT count(*) FROM pgflow.flows WHERE flow_slug = $1",
                 [fixture.names.mixed_case_flow]
               )

      assert [[1]] =
               UpstreamFixture.query_rows!(
                 fixture,
                 "SELECT count(*) FROM pgflow.step_tasks WHERE run_id = $1 AND message_id IS NULL",
                 [fixture.runs.map]
               )
    end
  end

  defp restore_env(name, nil), do: System.delete_env(name)
  defp restore_env(name, value), do: System.put_env(name, value)

  defp replace_database(url, database) do
    url
    |> URI.parse()
    |> Map.put(:path, "/#{database}")
    |> URI.to_string()
  end

  defp drop_database(admin_url, database) do
    admin_query!(
      admin_url,
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
      [database]
    )

    admin_query!(admin_url, ~s(DROP DATABASE IF EXISTS "#{database}"))
  end

  defp query_rows!(url, sql, params \\ []) do
    %{rows: rows} = admin_query!(url, sql, params)
    rows
  end

  defp admin_query!(url, sql, params \\ []) do
    uri = URI.parse(url)
    [username, password] = String.split(uri.userinfo || "", ":", parts: 2)

    {:ok, connection} =
      Postgrex.start_link(
        hostname: uri.host,
        port: uri.port || 5432,
        username: URI.decode(username),
        password: URI.decode(password),
        database: String.trim_leading(uri.path, "/")
      )

    try do
      Postgrex.query!(connection, sql, params)
    after
      GenServer.stop(connection)
    end
  end

  describe "post-upgrade catalog contract" do
    setup %{fixture: fixture} do
      previous = PgFlow.TestRepo.put_dynamic_repo(fixture.repo_name)

      try do
        Ecto.Migrator.up(
          PgFlow.TestRepo,
          9_000_000_102,
          PgFlow.Upstream.CoreUpgradeMigration,
          log: false
        )

        Ecto.Migrator.up(
          PgFlow.TestRepo,
          9_000_000_105,
          PgFlow.Upstream.HelpersUpgradeMigration,
          log: false
        )
      after
        PgFlow.TestRepo.put_dynamic_repo(previous)
      end

      :ok
    end

    @tag :post_upgrade_catalog
    test "steps and step_tasks persist queue identity", %{fixture: fixture} do
      assert [["step_tasks", "queue_name"], ["steps", "queue_name"]] =
               UpstreamFixture.query_rows!(
                 fixture,
                 """
                 SELECT table_name, column_name
                 FROM information_schema.columns
                 WHERE table_schema = 'pgflow'
                   AND (table_name, column_name) IN (('steps', 'queue_name'), ('step_tasks', 'queue_name'))
                 ORDER BY table_name
                 """
               )
    end

    @tag :post_upgrade_catalog
    test "step tasks support all six lifecycle statuses", %{fixture: fixture} do
      [[constraint]] =
        UpstreamFixture.query_rows!(
          fixture,
          """
          SELECT pg_get_constraintdef(oid)
          FROM pg_constraint
          WHERE conrelid = 'pgflow.step_tasks'::regclass
            AND conname = 'valid_status'
          """
        )

      accepted_statuses =
        ~r/'([^']+)'/
        |> Regex.scan(constraint, capture: :all_but_first)
        |> List.flatten()
        |> Enum.sort()

      assert accepted_statuses == ~w(cancelled completed failed queued skipped started)
    end

    @tag :post_upgrade_catalog
    test "only the queue-aware claim signature exists", %{fixture: fixture} do
      assert [[true, true]] =
               UpstreamFixture.query_rows!(
                 fixture,
                 """
                 SELECT
                   to_regprocedure('pgflow.start_tasks(text,bigint[],uuid,text)') IS NOT NULL,
                   to_regprocedure('pgflow.start_tasks(text,bigint[],uuid)') IS NULL
                 """
               )
    end

    @tag :post_upgrade_catalog
    test "two-argument startup compilation exists", %{fixture: fixture} do
      assert [[true]] =
               UpstreamFixture.query_rows!(
                 fixture,
                 "SELECT to_regprocedure('pgflow.ensure_flow_compiled(text,jsonb)') IS NOT NULL"
               )
    end

    @tag :post_upgrade_catalog
    test "the Elixir claim record keeps attempts_count as its eighth field", %{fixture: fixture} do
      assert [
               ["flow_slug"],
               ["run_id"],
               ["step_slug"],
               ["input"],
               ["msg_id"],
               ["task_index"],
               ["flow_input"],
               ["attempts_count"]
             ] =
               UpstreamFixture.query_rows!(
                 fixture,
                 """
                 SELECT attname
                 FROM pg_attribute
                 WHERE attrelid = (
                   SELECT typrelid
                   FROM pg_type
                   WHERE oid = 'pgflow.step_task_record'::regtype
                 )
                   AND attnum > 0
                   AND NOT attisdropped
                 ORDER BY attnum
                 """
               )
    end
  end
end
