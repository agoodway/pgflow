defmodule PgflowDemo.UpstreamUpgradeFixture do
  @moduledoc false

  alias EctoEvolver.Adapters.Postgres, as: EvolverPostgres

  @database_prefix "pgflow_demo_upstream_fixture_"
  @pre_upgrade_version 20_260_817_114_824
  @migrations_path Path.expand("../../priv/repo/migrations", __DIR__)

  @extensions_version 20_260_423_202_240
  @setup_pgflow_version 20_260_423_202_246
  @setup_dashboard_version 20_260_423_202_247
  @article_flow_version 20_260_423_202_248
  @cleanup_version 20_260_423_202_249

  @enforce_keys [
    :admin_url,
    :application_database,
    :database,
    :database_url,
    :repo_name,
    :repo_pid,
    :snapshot
  ]
  defstruct @enforce_keys

  @type snapshot :: %{
          article_run_id: binary(),
          onboarding_run_id: binary(),
          article_output: term(),
          run_count: integer(),
          task_count: integer(),
          queued_message_count: integer(),
          dashboard_comment: String.t() | nil,
          cleanup_job_type: String.t() | nil,
          cron_count: integer()
        }

  @type t :: %__MODULE__{
          admin_url: String.t(),
          application_database: String.t(),
          database: String.t(),
          database_url: String.t(),
          repo_name: atom(),
          repo_pid: pid(),
          snapshot: snapshot()
        }

  @spec setup!(keyword()) :: t()
  @doc false
  def setup!(opts \\ []) do
    admin_url = Keyword.get(opts, :admin_url, default_admin_url())
    application_database = application_database()
    refuse_application_database!(admin_url, application_database)

    suffix = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    database = @database_prefix <> suffix
    database_url = replace_database(admin_url, database)
    repo_name = :"pgflow_demo_upstream_fixture_#{suffix}"

    admin_query!(admin_url, "CREATE DATABASE #{quote_identifier(database)}")

    repo_pid =
      case PgflowDemo.Repo.start_link(
             name: repo_name,
             url: database_url,
             pool: DBConnection.ConnectionPool,
             pool_size: 2
           ) do
        {:ok, pid} ->
          pid

        {:error, reason} ->
          drop_database(admin_url, database)
          raise "could not start upstream upgrade fixture repository: #{inspect(reason)}"
      end

    fixture = %__MODULE__{
      admin_url: admin_url,
      application_database: application_database,
      database: database,
      database_url: database_url,
      repo_name: repo_name,
      repo_pid: repo_pid,
      snapshot: %{}
    }

    try do
      migrate_pre_upgrade!(fixture)
      snapshot = seed!(fixture)
      %{fixture | snapshot: snapshot}
    rescue
      exception ->
        if Process.alive?(repo_pid), do: Supervisor.stop(repo_pid)
        drop_database(admin_url, database)
        reraise exception, __STACKTRACE__
    end
  end

  @spec teardown!(t()) :: :ok
  @doc false
  def teardown!(%__MODULE__{} = fixture) do
    ensure_owned_database!(fixture.database)

    if Process.alive?(fixture.repo_pid) do
      Supervisor.stop(fixture.repo_pid)
    end

    drop_database(fixture.admin_url, fixture.database)
  end

  @spec migrate_pre_upgrade!(t()) :: :ok
  @doc false
  def migrate_pre_upgrade!(fixture) do
    run_migrations!(fixture, @pre_upgrade_version)
  end

  @spec migrate_all!(t()) :: :ok
  @doc false
  def migrate_all!(fixture) do
    run_migrations!(fixture, :all)
  end

  @spec migrate_upgrade!(t()) :: :ok
  @doc false
  def migrate_upgrade!(fixture) do
    with_fixture_repo(fixture, fn ->
      Ecto.Migrator.up(PgflowDemo.Repo, upgrade_migration_version!(), upgrade_migration_module(),
        log: false
      )
    end)
  end

  defp run_migrations!(fixture, target) do
    with_fixture_repo(fixture, fn ->
      Ecto.Migrator.up(
        PgflowDemo.Repo,
        @extensions_version,
        PgflowDemo.UpstreamUpgradeFixture.BaselineExtensions,
        log: false
      )

      Ecto.Migrator.run(PgflowDemo.Repo, @migrations_path, :up, to: 20_260_423_202_244)

      Ecto.Migrator.up(
        PgflowDemo.Repo,
        @setup_pgflow_version,
        PgflowDemo.UpstreamUpgradeFixture.BaselineSetupPgflow,
        log: false
      )

      install_cron_shim!()
      # Historical wrappers used up/0; explicitly replay their released V03
      # target instead of allowing today's V04 default into the baseline.
      Ecto.Migrator.up(
        PgflowDemo.Repo,
        @setup_dashboard_version,
        PgflowDemo.UpstreamUpgradeFixture.BaselineDashboard,
        log: false
      )

      Ecto.Migrator.run(PgflowDemo.Repo, @migrations_path, :up, to: @article_flow_version)

      Ecto.Migrator.up(
        PgflowDemo.Repo,
        @cleanup_version,
        PgflowDemo.UpstreamUpgradeFixture.BaselineArticleFlowCleanup,
        log: false
      )

      Ecto.Migrator.up(
        PgflowDemo.Repo,
        @pre_upgrade_version,
        PgflowDemo.UpstreamUpgradeFixture.BaselineDashboard,
        log: false
      )

      stop = if target == :all, do: upgrade_migration_version!() - 1, else: target
      Ecto.Migrator.run(PgflowDemo.Repo, @migrations_path, :up, to: stop)

      if target == :all do
        Ecto.Migrator.up(
          PgflowDemo.Repo,
          upgrade_migration_version!(),
          upgrade_migration_module(),
          log: false
        )
      end
    end)
  end

  defp install_cron_shim! do
    PgflowDemo.Repo.query!("CREATE SCHEMA IF NOT EXISTS cron")

    PgflowDemo.Repo.query!("""
    CREATE TABLE IF NOT EXISTS cron.job (
      jobid bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      jobname text UNIQUE,
      schedule text,
      command text,
      active boolean DEFAULT true
    )
    """)
  end

  @spec upgrade_migration_version!() :: integer()
  @doc false
  def upgrade_migration_version! do
    Path.wildcard(Path.join(@migrations_path, "*_upgrade_pgflow_upstream_alignment.exs"))
    |> List.first()
    |> Path.basename()
    |> String.split("_")
    |> List.first()
    |> String.to_integer()
  end

  @spec upgrade_migration_module() :: module()
  @doc false
  def upgrade_migration_module do
    case Path.wildcard(Path.join(@migrations_path, "*_upgrade_pgflow_upstream_alignment.exs")) do
      [path | _] ->
        [{module, _}] = Code.compile_file(path)
        module

      [] ->
        raise "missing upgrade_pgflow_upstream_alignment migration"
    end
  end

  @spec with_fixture_repo(t(), (-> term())) :: term()
  @doc false
  def with_fixture_repo(fixture, fun) do
    previous = PgflowDemo.Repo.put_dynamic_repo(fixture.repo_name)

    try do
      fun.()
    after
      PgflowDemo.Repo.put_dynamic_repo(previous)
    end
  end

  @spec query_rows!(t(), String.t(), list()) :: list(list())
  @doc false
  def query_rows!(fixture, sql, params \\ []) do
    with_fixture_repo(fixture, fn ->
      %{rows: rows} = PgflowDemo.Repo.query!(sql, params)
      rows
    end)
  end

  @spec scalar!(t(), String.t(), list()) :: term()
  @doc false
  def scalar!(fixture, sql, params \\ []) do
    with_fixture_repo(fixture, fn ->
      %{rows: [[value]]} = PgflowDemo.Repo.query!(sql, params)
      value
    end)
  end

  @spec core_version(t()) :: non_neg_integer()
  @doc false
  def core_version(fixture) do
    with_fixture_repo(fixture, fn ->
      EvolverPostgres.get_version(PgflowDemo.Repo, "pgflow", {:view, "pgflow_version"})
    end)
  end

  @spec helpers_version(t()) :: non_neg_integer()
  @doc false
  def helpers_version(fixture) do
    with_fixture_repo(fixture, fn ->
      EvolverPostgres.get_version(PgflowDemo.Repo, "pgflow", {:view, "extensions_version"})
    end)
  end

  defp seed!(fixture) do
    with_fixture_repo(fixture, fn ->
      article_run_id = start_run!("article_flow", %{"url" => "https://example.com/article"})
      onboarding_run_id = start_run!("onboarding_flow", %{"plan" => "premium"})

      PgflowDemo.Repo.query!("""
      INSERT INTO cron.job (jobname, schedule, command)
      VALUES ('pgflow:article_flow_cleanup', '0 * * * *', 'SELECT 1')
      ON CONFLICT (jobname) DO NOTHING
      """)

      article_output = [%{"title" => "Upstream fixture article"}]

      PgflowDemo.Repo.query!(
        """
        UPDATE pgflow.step_states
        SET status = 'completed',
            completed_at = now(),
            remaining_tasks = 0,
            output = $2::jsonb
        WHERE run_id = $1 AND step_slug = 'fetch_article'
        """,
        [article_run_id, Jason.encode!(article_output)]
      )

      %{
        article_run_id: article_run_id,
        onboarding_run_id: onboarding_run_id,
        article_output: article_output,
        run_count: scalar_in_repo!("SELECT count(*) FROM pgflow.runs"),
        task_count: scalar_in_repo!("SELECT count(*) FROM pgflow.step_tasks"),
        queued_message_count:
          scalar_in_repo!("SELECT count(*) FROM pgflow.step_tasks WHERE message_id IS NOT NULL"),
        dashboard_comment: dashboard_comment!(),
        cleanup_job_type:
          scalar_in_repo!(
            "SELECT flow_type FROM pgflow.flows WHERE flow_slug = 'article_flow_cleanup'"
          ),
        cron_count: cron_job_count!()
      }
    end)
  end

  defp start_run!(flow_slug, input) do
    %{rows: [[run_id]]} =
      PgflowDemo.Repo.query!(
        "SELECT run_id FROM pgflow.start_flow($1, $2::jsonb)",
        [flow_slug, Jason.encode!(input)]
      )

    run_id
  end

  defp dashboard_comment! do
    %{rows: [[comment]]} =
      PgflowDemo.Repo.query!(
        "SELECT obj_description('pgflow_dashboard.runs_with_progress'::regclass, 'pg_class')"
      )

    comment
  end

  defp scalar_in_repo!(sql, params \\ []) do
    %{rows: [[value]]} = PgflowDemo.Repo.query!(sql, params)
    value
  end

  defp default_admin_url do
    System.get_env(
      "PGFLOW_DEMO_ADMIN_URL",
      replace_database(application_database_url(), "postgres")
    )
  end

  defp application_database_url do
    config = Application.fetch_env!(:pgflow_demo, PgflowDemo.Repo)

    %URI{
      scheme: "postgres",
      userinfo: "#{config[:username]}:#{config[:password]}",
      host: config[:hostname],
      port: config[:port],
      path: "/#{config[:database]}"
    }
    |> URI.to_string()
  end

  defp application_database do
    Application.fetch_env!(:pgflow_demo, PgflowDemo.Repo)
    |> Keyword.fetch!(:database)
  end

  defp refuse_application_database!(admin_url, application_database) do
    if database_from_url(admin_url) == application_database do
      raise ArgumentError,
            "upstream upgrade fixture admin_url points at the ordinary demo database #{inspect(application_database)}"
    end
  end

  defp database_from_url(url) do
    url
    |> URI.parse()
    |> Map.fetch!(:path)
    |> String.trim_leading("/")
  end

  defp replace_database(url, database) do
    url
    |> URI.parse()
    |> Map.put(:path, "/#{database}")
    |> URI.to_string()
  end

  defp ensure_owned_database!(database) do
    unless String.starts_with?(database, @database_prefix) do
      raise ArgumentError,
            "refusing to drop database not owned by upstream upgrade fixture: #{inspect(database)}"
    end
  end

  defp quote_identifier(identifier) do
    ~s("#{String.replace(identifier, ~s("), ~s(""))}")
  end

  defp drop_database(admin_url, database) do
    ensure_owned_database!(database)

    admin_query!(
      admin_url,
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
      [database]
    )

    admin_query!(admin_url, "DROP DATABASE IF EXISTS #{quote_identifier(database)}")
    :ok
  end

  defp admin_query!(admin_url, sql, params \\ []) do
    {:ok, connection} = Postgrex.start_link(postgrex_options(admin_url))

    try do
      Postgrex.query!(connection, sql, params)
    after
      GenServer.stop(connection)
    end
  end

  defp cron_job_count! do
    scalar_in_repo!("SELECT count(*) FROM cron.job WHERE jobname = 'pgflow:article_flow_cleanup'")
  end

  defp postgrex_options(url) do
    uri = URI.parse(url)
    [username, password] = String.split(uri.userinfo || "", ":", parts: 2)

    [
      hostname: uri.host,
      port: uri.port || 5432,
      username: URI.decode(username),
      password: URI.decode(password),
      database: database_from_url(url)
    ]
  end
end
