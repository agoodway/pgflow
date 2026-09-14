defmodule PgFlow.Test.UpstreamHarness do
  @moduledoc false

  alias PgFlow.Test.{UpstreamFixture, UpstreamInterop}

  @pinned_sha "94490709f79ebf366141dd925b047f0c1013e759"
  @manifest_path Path.expand("manifest.json", __DIR__)
  @support_dir __DIR__

  @type suite :: :catalog | :pgtap | :typescript | :all
  @type result :: %{
          suite: suite(),
          status: :passed | :failed | :skipped,
          tests: non_neg_integer(),
          failures: non_neg_integer(),
          details: map()
        }

  @doc "Runs the requested upstream compatibility suite and returns a report map."
  @spec run(keyword()) :: {:ok, map()} | {:error, String.t()}
  def run(opts) do
    checkout = Keyword.fetch!(opts, :checkout)
    sha = Keyword.fetch!(opts, :sha)
    database_url = Keyword.fetch!(opts, :database_url)
    suite = Keyword.get(opts, :suite, :all)

    with :ok <- verify_sha!(sha),
         :ok <- verify_checkout_head!(checkout, sha),
         :ok <- verify_fixture_database!(database_url),
         :ok <- verify_prerequisites!(checkout, suite, database_url),
         {:ok, report} <- run_suite(suite, checkout, database_url) do
      {:ok,
       Map.merge(report, %{
         "upstream_sha" => sha,
         "checkout" => Path.expand(checkout),
         "database" => database_from_url(database_url),
         "packages" => package_versions(checkout)
       })}
    end
  end

  @doc "Loads the pinned compatibility-suite manifest."
  @spec manifest() :: map()
  def manifest do
    @manifest_path |> File.read!() |> Jason.decode!()
  end

  @doc "Returns the sole upstream revision accepted by this harness."
  @spec pinned_sha() :: String.t()
  def pinned_sha, do: @pinned_sha

  @doc "Counts TAP assertions and treats TAP, process, empty, and incomplete-plan failures as failures."
  @spec tap_result(String.t(), integer()) :: %{
          tests: non_neg_integer(),
          failures: non_neg_integer()
        }
  def tap_result(output, exit_status) do
    assertions = Regex.scan(~r/^\s*(?:not )?ok\b.*$/m, output)
    failed = Enum.count(assertions, fn [line] -> Regex.match?(~r/^\s*not ok\b/, line) end)
    plans = Regex.scan(~r/^\s*1\.\.(\d+)/m, output)
    planned = Enum.reduce(plans, 0, fn [_, count], total -> total + String.to_integer(count) end)
    tests = length(assertions)

    # Supabase's prove runner summarizes assertions instead of printing each one.
    tests =
      case Regex.run(~r/Tests=(\d+)/, output) do
        [_, count] -> String.to_integer(count)
        _ -> tests
      end

    invalid? =
      exit_status != 0 or tests == 0 or (plans != [] and tests != planned) or
        Regex.match?(~r/(?:^\s*Bail out!|# Looks like you failed)/m, output)

    %{tests: tests, failures: max(failed, if(invalid?, do: 1, else: 0))}
  end

  @doc "Rejects upstream revisions other than the recorded pin."
  @spec verify_sha!(String.t()) :: :ok | {:error, String.t()}
  def verify_sha!(@pinned_sha), do: :ok

  def verify_sha!(sha) do
    {:error, "refusing unpinned upstream SHA #{inspect(sha)}; expected #{@pinned_sha}"}
  end

  @doc "Checks that a local checkout is at the requested upstream revision."
  @spec verify_checkout_head!(Path.t(), String.t()) :: :ok | {:error, String.t()}
  def verify_checkout_head!(checkout, sha) do
    case System.cmd("git", ["-C", checkout, "rev-parse", "HEAD"], stderr_to_stdout: true) do
      {head, 0} ->
        head = String.trim(head)

        if head == sha do
          :ok
        else
          {:error,
           "checkout HEAD #{head} does not match requested SHA #{sha}; refusing mismatched checkout"}
        end

      {output, status} ->
        {:error, "git rev-parse failed (exit #{status}): #{String.trim(output)}"}
    end
  end

  @doc "Refuses database names outside the disposable-fixture prefixes."
  @spec verify_fixture_database!(String.t()) :: :ok | {:error, String.t()}
  def verify_fixture_database!(database_url) do
    database = database_from_url(database_url)
    prefixes = manifest()["harness"]["database_prefixes"]

    if Enum.any?(prefixes, &String.starts_with?(database, &1)) do
      :ok
    else
      {:error,
       "refusing non-fixture database #{inspect(database)}; expected one of #{inspect(prefixes)}"}
    end
  end

  @doc "Checks the commands, database extension, and packages needed by a suite."
  @spec verify_prerequisites!(Path.t(), suite(), String.t()) :: :ok | {:error, String.t()}
  def verify_prerequisites!(checkout, suite, database_url) do
    missing =
      required_commands(suite)
      |> Enum.reject(&command_available?/1)

    with :ok <- ensure_missing_commands(missing),
         :ok <- ensure_pgtap_host_package!(suite, database_url) do
      ensure_packages_built(checkout, suite)
    end
  end

  defp required_commands(:catalog), do: ["psql", "git"]
  defp required_commands(:pgtap), do: ["psql", "git", "supabase"]
  defp required_commands(:typescript), do: ["node", "pnpm", "git"]
  defp required_commands(:all), do: ["psql", "git", "supabase", "node", "pnpm"]

  defp command_available?(command) do
    case System.find_executable(command) do
      nil -> false
      _ -> true
    end
  end

  defp ensure_missing_commands([]), do: :ok

  defp ensure_missing_commands(missing) do
    {:error, "missing harness prerequisites: #{Enum.join(missing, ", ")}"}
  end

  defp ensure_pgtap_host_package!(suite, database_url) when suite in [:pgtap, :all] do
    admin_url = admin_url_for(database_url)

    with {:ok, conn} <- connect(admin_url) do
      available? =
        query_scalar!(
          conn,
          "SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = 'pgtap')"
        )

      GenServer.stop(conn)

      if available? do
        :ok
      else
        {:error,
         "pgtap extension package is not installed on the Postgres host; install postgresql-*-pgtap (see test/support/upstream/run.exs and test/support/db/compose.yaml)"}
      end
    end
  end

  defp ensure_pgtap_host_package!(_suite, _database_url), do: :ok

  defp ensure_packages_built(_checkout, suite)
       when suite in [:catalog, :pgtap],
       do: :ok

  defp ensure_packages_built(checkout, _suite) do
    packages = manifest()["harness"]["typescript"]["packages"]

    missing =
      Enum.reject(packages, fn pkg ->
        dist = Path.join([checkout, "pkgs", pkg, "dist", "index.js"])
        File.exists?(dist)
      end)

    case missing do
      [] ->
        :ok

      _ ->
        case build_upstream_packages!(checkout) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @spec build_upstream_packages!(Path.t()) :: :ok | {:error, String.t()}
  def build_upstream_packages!(checkout) do
    packages = manifest()["harness"]["typescript"]["packages"]
    targets = Enum.map_join(packages, ",", & &1)

    with :ok <- ensure_pnpm_install!(checkout),
         {output, 0} <-
           System.cmd(
             pnpm_cmd(),
             ["nx", "run-many", "--target=build", "--projects=#{targets}"],
             cd: checkout,
             stderr_to_stdout: true
           ) do
      missing =
        Enum.reject(packages, fn pkg ->
          File.exists?(Path.join([checkout, "pkgs", pkg, "dist", "index.js"]))
        end)

      if missing == [] do
        :ok
      else
        {:error,
         "upstream package build finished but dist is still missing for #{inspect(missing)}: #{String.trim(output)}"}
      end
    else
      {_output, status} -> {:error, "failed to build upstream packages (exit #{status})"}
    end
  end

  defp ensure_pnpm_install!(checkout) do
    if File.exists?(Path.join(checkout, "node_modules")) do
      :ok
    else
      case System.cmd(pnpm_cmd(), ["install", "--frozen-lockfile", "--ignore-scripts"],
             cd: checkout,
             stderr_to_stdout: true
           ) do
        {_, 0} ->
          :ok

        {output, status} ->
          {:error, "pnpm install failed (exit #{status}): #{String.slice(output, 0, 400)}"}
      end
    end
  end

  defp pnpm_cmd do
    System.get_env("PGFLOW_PNPM") || System.find_executable("pnpm") || "pnpm"
  end

  defp node_cmd do
    System.get_env("PGFLOW_NODE") || System.find_executable("node") || "node"
  end

  defp pgtap_database_url(database_url) do
    uri = URI.parse(database_url)
    query = URI.decode_query(uri.query || "")

    if Map.has_key?(query, "sslmode") do
      database_url
    else
      query = Map.put(query, "sslmode", "disable")
      uri |> Map.put(:query, URI.encode_query(query)) |> URI.to_string()
    end
  end

  defp run_suite(suite, checkout, database_url) when suite not in [:all] do
    case run_single_suite(suite, checkout, database_url) do
      {:ok, result} ->
        {:ok,
         %{
           "status" => Atom.to_string(result.status),
           "tests" => result.tests,
           "failures" => result.failures,
           "unresolved" => Map.get(result, :unresolved, 0),
           "skipped" => Map.get(result, :skipped, 0),
           "results" => [result_to_map(result)]
         }}

      {:error, reason} ->
        {:ok,
         %{
           "status" => "failed",
           "tests" => 1,
           "failures" => 1,
           "results" => [result_to_map(failure_result(suite, reason))]
         }}
    end
  end

  defp run_suite(:all, checkout, database_url) do
    results =
      for suite <- [:catalog, :pgtap, :typescript] do
        case run_single_suite(suite, checkout, database_url) do
          {:ok, result} -> result
          {:error, reason} -> failure_result(suite, reason)
        end
      end

    totals =
      Enum.reduce(results, %{tests: 0, failures: 0, unresolved: 0, skipped: 0}, fn result, acc ->
        %{
          tests: acc.tests + result.tests,
          failures: acc.failures + result.failures,
          unresolved: acc.unresolved + Map.get(result, :unresolved, 0),
          skipped: acc.skipped + Map.get(result, :skipped, 0)
        }
      end)

    status = if totals.failures == 0, do: "passed", else: "failed"

    {:ok,
     %{
       "status" => status,
       "tests" => totals.tests,
       "failures" => totals.failures,
       "unresolved" => totals.unresolved,
       "skipped" => totals.skipped,
       "results" => Enum.map(results, &result_to_map/1)
     }}
  end

  defp run_single_suite(:catalog, checkout, database_url) do
    admin_url = admin_url_for(database_url)

    with :ok <- install_elixir_adaptations!(database_url),
         {:ok, pristine_url} <- create_pristine_database!(admin_url, checkout),
         :ok <- compare_and_drop_pristine!(pristine_url, database_url, admin_url),
         :ok <- validate_target_catalog!(database_url),
         :ok <- validate_overlay_catalog!(database_url) do
      {:ok,
       %{
         suite: :catalog,
         status: :passed,
         tests: 4,
         failures: 0,
         details: %{
           profile: "plain_postgres",
           note:
             "Catalog gate diffs Elixir-installed schema against a pristine upstream migration replay, allowing only recorded overlays, then validates the target contract."
         }
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp run_single_suite(:pgtap, checkout, database_url) do
    lifecycle_files = manifest()["harness"]["pgtap_lifecycle_files"]
    prune_files = manifest()["harness"]["pgtap_prune_files"]
    admin_url = admin_url_for(database_url)

    with :ok <- install_elixir_adaptations!(database_url),
         :ok <- install_pgtap!(database_url),
         {:ok, lifecycle_result} <-
           run_pgtap_profile!(checkout, database_url, lifecycle_files, "plain_postgres"),
         {:ok, prune_url} <- create_temporary_database!(admin_url, "pgflow_compat_prune_"),
         {:ok, prune_result} <-
           run_prune_pgtap_profile!(checkout, prune_url, prune_files, admin_url) do
      tests = lifecycle_result.tests + prune_result.tests
      failures = lifecycle_result.failures + prune_result.failures

      {:ok,
       %{
         suite: :pgtap,
         status: if(failures == 0, do: :passed, else: :failed),
         tests: tests,
         failures: failures,
         details: %{
           "profiles" => [lifecycle_result.details, prune_result.details]
         }
       }}
    end
  end

  defp run_single_suite(:typescript, checkout, database_url) do
    with :ok <- install_elixir_adaptations!(database_url),
         {:ok, shape_result} <- run_node_script!("shape_cases.mjs", checkout, database_url),
         {:ok, worker_result} <-
           run_node_script!("worker.mjs", checkout, database_url, ["--scenario", "all"]),
         {:ok, reverse_result} <- UpstreamInterop.run_reverse_interop!(checkout, database_url) do
      tests = shape_result.tests + worker_result.tests + reverse_result.tests
      failures = shape_result.failures + worker_result.failures + reverse_result.failures

      status = if failures == 0, do: :passed, else: :failed

      {:ok,
       %{
         suite: :typescript,
         status: status,
         tests: tests,
         failures: failures,
         unresolved: Map.get(worker_result, :unresolved, 0),
         skipped: Map.get(shape_result, :skipped, 0),
         details: %{
           shape: shape_result,
           worker: worker_result,
           reverse_interop: reverse_result,
           json_falsy_probe: manifest()["harness"]["typescript"]["json_falsy_probe"]
         }
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp compare_and_drop_pristine!(pristine_url, elixir_url, admin_url) do
    compare_catalogs!(pristine_url, elixir_url)
  after
    drop_database!(admin_url, database_from_url(pristine_url))
  end

  defp run_prune_pgtap_profile!(checkout, prune_url, prune_files, admin_url) do
    with :ok <- install_upstream_core_only_profile!(prune_url, checkout),
         :ok <- install_pgtap!(prune_url) do
      run_pgtap_profile!(checkout, prune_url, prune_files, "upstream_core_only_prune",
        runner: :psql
      )
    end
  after
    drop_database!(admin_url, database_from_url(prune_url))
  end

  defp run_pgtap_profile!(checkout, database_url, files, profile_name, opts \\ []) do
    tests_dir = Path.join(checkout, "pkgs/core/supabase/tests")

    {output, status} =
      case Keyword.get(opts, :runner, :supabase) do
        :psql -> run_pgtap_psql!(tests_dir, database_url, files)
        :supabase -> run_pgtap_supabase!(tests_dir, database_url, files)
      end

    %{tests: tests, failures: failures} = tap_result(output, status)

    {:ok,
     %{
       tests: tests,
       failures: failures,
       details: %{
         "files" => files,
         "profile" => manifest()["harness"]["pgtap_profiles"][profile_name],
         "profile_name" => profile_name,
         "output_excerpt" => String.slice(String.trim(output), 0, 2_000)
       }
     }}
  end

  defp run_pgtap_supabase!(tests_dir, database_url, files) do
    System.cmd(
      "supabase",
      [
        "db",
        "test",
        "--db-url",
        pgtap_database_url(database_url),
        "--yes"
      ] ++ files,
      cd: tests_dir,
      stderr_to_stdout: true
    )
  end

  defp run_pgtap_psql!(tests_dir, database_url, files) do
    uri = URI.parse(database_url)
    [username, password] = String.split(uri.userinfo || "", ":", parts: 2)

    args_base = [
      "-h",
      uri.host,
      "-p",
      Integer.to_string(uri.port || 5432),
      "-U",
      URI.decode(username),
      "-d",
      database_from_url(database_url),
      "-v",
      "ON_ERROR_STOP=1"
    ]

    env = [{"PGPASSWORD", URI.decode(password)}]

    {chunks, statuses} =
      Enum.map(files, fn file ->
        System.cmd("psql", args_base ++ ["-f", file],
          cd: tests_dir,
          env: env,
          stderr_to_stdout: true
        )
      end)
      |> Enum.unzip()

    status = if Enum.all?(statuses, &(&1 == 0)), do: 0, else: 1
    {Enum.join(chunks, "\n"), status}
  end

  defp install_upstream_core_only_profile!(database_url, checkout) do
    with {:ok, conn} <- connect(database_url),
         :ok <- exec!(conn, "CREATE EXTENSION IF NOT EXISTS pgmq"),
         :ok <- install_realtime_shim!(conn) do
      GenServer.stop(conn)
    end

    with :ok <- apply_upstream_migrations!(database_url, checkout) do
      maybe_load_seed!(database_url, checkout)
    end
  end

  defp create_temporary_database!(admin_url, prefix) do
    suffix = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    database = prefix <> suffix
    database_url = replace_database(admin_url, database)

    with :ok <- exec_admin!(admin_url, "CREATE DATABASE \"#{database}\"") do
      {:ok, database_url}
    end
  end

  defp run_node_script!(script, checkout, database_url, extra_args \\ []) do
    script_path = Path.join(@support_dir, script)

    env = [
      {"PGFLOW_UPSTREAM_CHECKOUT", checkout},
      {"PGFLOW_SHAPE_CASES_PATH", Path.join(@support_dir, "shape_cases.json")},
      {"DATABASE_URL", database_url},
      {"PGFLOW_COMPAT_DATABASE_URL", database_url}
    ]

    case System.cmd(node_cmd(), [script_path | extra_args],
           cd: @support_dir,
           env: env,
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        case Jason.decode(output) do
          {:ok, %{"tests" => tests, "failures" => failures} = decoded} ->
            {:ok,
             %{
               tests: tests,
               failures: failures,
               unresolved: Map.get(decoded, "unresolved", 0),
               skipped: Map.get(decoded, "skipped", 0),
               status: if(failures == 0 and tests > 0, do: :passed, else: :failed),
               details: Map.drop(decoded, ["tests", "failures", "status"])
             }}

          _ ->
            {:error, "invalid JSON from #{script}: #{String.slice(output, 0, 400)}"}
        end

      {output, status} ->
        {:error,
         "#{script} failed (exit #{status}): #{String.slice(String.trim(output), 0, 500)}"}
    end
  end

  @spec install_elixir_adaptations!(String.t()) :: :ok | {:error, String.t()}
  def install_elixir_adaptations!(database_url) do
    checkout = System.get_env("PGFLOW_UPSTREAM_CHECKOUT")

    with {:ok, conn} <- connect(database_url),
         :ok <- exec!(conn, "CREATE EXTENSION IF NOT EXISTS pgmq"),
         :ok <- install_realtime_shim!(conn),
         :ok <- maybe_load_seed!(database_url, checkout) do
      GenServer.stop(conn)
      :ok
    end
  end

  @spec install_pgtap!(String.t()) :: :ok | {:error, String.t()}
  def install_pgtap!(database_url) do
    with {:ok, conn} <- connect(database_url),
         :ok <- exec!(conn, "CREATE EXTENSION IF NOT EXISTS pgtap") do
      GenServer.stop(conn)
      :ok
    end
  end

  defp maybe_load_seed!(_database_url, nil),
    do: {:error, "PGFLOW_UPSTREAM_CHECKOUT is required to load pgflow_tests helpers"}

  defp maybe_load_seed!(database_url, checkout) do
    seed_path = Path.join([checkout, "pkgs/core/supabase/seed.sql"])

    if File.exists?(seed_path) do
      run_psql_file!(database_url, seed_path)
    else
      {:error, "missing upstream seed.sql at #{seed_path}"}
    end
  end

  defp install_realtime_shim!(conn) do
    statements = [
      "CREATE SCHEMA IF NOT EXISTS realtime",
      """
      CREATE TABLE IF NOT EXISTS realtime.messages (
        id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
        topic text,
        event text,
        payload jsonb,
        private boolean DEFAULT false,
        inserted_at timestamptz DEFAULT now()
      )
      """,
      """
      CREATE OR REPLACE FUNCTION realtime.send(
        payload jsonb, event text, topic text, private boolean DEFAULT false
      ) RETURNS void AS $$
        INSERT INTO realtime.messages (topic, event, payload, private)
        VALUES (topic, event, payload, private);
      $$ LANGUAGE sql
      """
    ]

    Enum.reduce_while(statements, :ok, fn sql, :ok ->
      case exec!(conn, sql) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp create_pristine_database!(admin_url, checkout) do
    suffix = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    database = "pgflow_compat_pristine_" <> suffix
    database_url = replace_database(admin_url, database)

    with :ok <- exec_admin!(admin_url, "CREATE DATABASE \"#{database}\""),
         {:ok, conn} <- connect(database_url),
         :ok <- exec!(conn, "CREATE EXTENSION IF NOT EXISTS pgmq"),
         :ok <- install_realtime_shim!(conn),
         :ok <- apply_upstream_migrations!(database_url, checkout) do
      GenServer.stop(conn)
      {:ok, database_url}
    end
  end

  defp apply_upstream_migrations!(database_url, checkout) do
    migrations_dir = Path.join(checkout, "pkgs/core/supabase/migrations")

    skip_modes =
      manifest()["harness"]["catalog_overlays"]["pristine_migration_skips"]
      |> Map.new(fn %{"file" => file} = entry -> {file, Map.get(entry, "mode", "skip")} end)

    migrations_dir
    |> File.ls!()
    |> Enum.filter(&String.match?(&1, ~r/^\d{14}_.+\.sql$/))
    |> Enum.sort()
    |> Enum.reduce_while(:ok, fn file, :ok ->
      path = Path.join(migrations_dir, file)

      case Map.get(skip_modes, file) do
        "skip" ->
          {:cont, :ok}

        mode ->
          apply_migration_file(database_url, path, file, mode)
      end
    end)
  end

  defp apply_migration_file(database_url, path, file, mode) do
    opts =
      if mode == "adapt_without_cron",
        do: [adapt_cron: true, adapt_pristine: file],
        else: [adapt_cron: true]

    case run_psql_file!(database_url, path, opts) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, "failed applying #{file}: #{reason}"}}
    end
  end

  defp validate_target_catalog!(database_url) do
    target = manifest()["target"]

    with {:ok, conn} <- connect(database_url) do
      checks = [
        fn ->
          if query_scalar!(
               conn,
               "SELECT to_regprocedure('pgflow.start_tasks(text,bigint[],uuid,text)') IS NOT NULL"
             ) do
            :ok
          else
            {:error, "missing four-argument start_tasks"}
          end
        end,
        fn ->
          if query_scalar!(
               conn,
               "SELECT to_regprocedure('pgflow.start_tasks(text,bigint[],uuid)') IS NULL"
             ) do
            :ok
          else
            {:error, "obsolete three-argument start_tasks still present"}
          end
        end,
        fn ->
          if query_scalar!(
               conn,
               "SELECT to_regprocedure('pgflow.ensure_flow_compiled(text,jsonb)') IS NOT NULL"
             ) do
            :ok
          else
            {:error, "missing ensure_flow_compiled(text,jsonb)"}
          end
        end,
        fn ->
          fields =
            query_rows!(conn, """
            SELECT attname
            FROM pg_attribute
            WHERE attrelid = (SELECT typrelid FROM pg_type WHERE oid = 'pgflow.step_task_record'::regtype)
              AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum
            """)

          if List.flatten(fields) == target["record_fields"] do
            :ok
          else
            {:error, "unexpected step_task_record fields: #{inspect(fields)}"}
          end
        end
      ]

      result =
        Enum.reduce_while(checks, :ok, &run_catalog_check/2)

      GenServer.stop(conn)
      result
    end
  end

  defp run_catalog_check(check, :ok) do
    case check.() do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp validate_overlay_catalog!(database_url) do
    overlays = manifest()["harness"]["catalog_overlays"]

    with {:ok, conn} <- connect(database_url) do
      result =
        with :ok <- assert_extra_column!(conn, overlays["extra_columns"]),
             :ok <- assert_extra_type_attributes!(conn, overlays["extra_type_attributes"]),
             :ok <- assert_missing_functions!(conn, overlays["missing_functions"]) do
          assert_helper_overrides!(conn, overlays["helper_overridden_functions"])
        end

      GenServer.stop(conn)
      result
    end
  end

  defp assert_extra_column!(conn, columns) do
    Enum.reduce_while(columns, :ok, fn %{"table" => table, "column" => column}, :ok ->
      exists? =
        query_scalar!(
          conn,
          """
          SELECT EXISTS(
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'pgflow' AND table_name = $1 AND column_name = $2
          )
          """,
          [table, column]
        )

      if exists?,
        do: {:cont, :ok},
        else: {:halt, {:error, "missing overlay column #{table}.#{column}"}}
    end)
  end

  defp assert_extra_type_attributes!(conn, attributes) do
    Enum.reduce_while(attributes, :ok, fn %{"type" => type, "attribute" => attribute}, :ok ->
      exists? =
        query_scalar!(
          conn,
          """
          SELECT EXISTS(
            SELECT 1
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_attribute a ON a.attrelid = t.typrelid
            WHERE n.nspname = 'pgflow' AND t.typname = $1 AND a.attname = $2
          )
          """,
          [type, attribute]
        )

      if exists?,
        do: {:cont, :ok},
        else: {:halt, {:error, "missing overlay attribute #{type}.#{attribute}"}}
    end)
  end

  defp assert_missing_functions!(conn, functions) do
    Enum.reduce_while(functions, :ok, fn function, :ok ->
      name = function |> String.replace("()", "") |> String.split("(") |> hd()

      exists? =
        query_rows!(
          conn,
          """
          SELECT p.proname
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
          WHERE n.nspname = 'pgflow' AND p.proname = $1
          """,
          [name]
        )

      if exists? == [],
        do: {:cont, :ok},
        else: {:halt, {:error, "expected missing function #{function}"}}
    end)
  end

  defp assert_helper_overrides!(conn, functions) do
    Enum.reduce_while(functions, :ok, fn function, :ok ->
      exists? =
        query_rows!(
          conn,
          """
          SELECT p.proname
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
          WHERE n.nspname = 'pgflow' AND p.proname = $1
          """,
          [function]
        )

      if exists? != [],
        do: {:cont, :ok},
        else: {:halt, {:error, "missing helper override #{function}"}}
    end)
  end

  defp query_scalar!(conn, sql, params \\ []) do
    {:ok, %{rows: [[value]]}} = Postgrex.query(conn, sql, params)
    value
  end

  defp compare_catalogs!(pristine_url, elixir_url) do
    overlays = manifest()["harness"]["catalog_overlays"]

    with {:ok, pristine} <- fetch_catalog(pristine_url),
         {:ok, elixir} <- fetch_catalog(elixir_url) do
      diff_catalogs(pristine, elixir, overlays)
    end
  end

  defp fetch_catalog(database_url) do
    with {:ok, conn} <- connect(database_url) do
      functions =
        query_rows!(conn, """
        SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS args
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pgflow'
        ORDER BY 1, 2
        """)

      columns =
        query_rows!(conn, """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'pgflow'
        ORDER BY 1, 2
        """)

      types =
        query_rows!(conn, """
        SELECT t.typname, a.attname
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        JOIN pg_attribute a ON a.attrelid = t.typrelid
        WHERE n.nspname = 'pgflow'
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY 1, 2
        """)

      GenServer.stop(conn)

      {:ok,
       %{
         functions: Map.new(functions, fn [name, args] -> {{name, args}, true} end),
         columns: MapSet.new(columns),
         types: MapSet.new(types)
       }}
    end
  end

  defp diff_catalogs(pristine, elixir, overlays) do
    missing_allowed = MapSet.new(overlays["missing_functions"])
    helper_overridden = MapSet.new(overlays["helper_overridden_functions"])
    extra_functions = MapSet.new(overlays["extra_functions"])
    extra_columns = MapSet.new(Enum.map(overlays["extra_columns"], &{&1["table"], &1["column"]}))

    extra_type_attrs =
      MapSet.new(Enum.map(overlays["extra_type_attributes"], &{&1["type"], &1["attribute"]}))

    pristine_canonical = canonical_function_map(pristine.functions)
    elixir_canonical = canonical_function_map(elixir.functions)

    pristine_functions =
      Map.keys(pristine_canonical)
      |> Enum.reject(fn {name, _types} -> MapSet.member?(missing_allowed, name) end)

    elixir_core_functions =
      Map.keys(elixir_canonical)
      |> Enum.reject(fn {name, _types} ->
        MapSet.member?(helper_overridden, name) or MapSet.member?(missing_allowed, name) or
          MapSet.member?(extra_functions, name)
      end)

    missing_in_elixir =
      Enum.reject(pristine_functions, fn key -> Map.has_key?(elixir_canonical, key) end)

    unexpected_in_elixir =
      Enum.reject(elixir_core_functions, fn key -> Map.has_key?(pristine_canonical, key) end)

    extra_elixir_columns =
      MapSet.difference(elixir.columns, pristine.columns)
      |> MapSet.reject(fn [table, column] -> MapSet.member?(extra_columns, {table, column}) end)

    extra_elixir_types =
      MapSet.difference(elixir.types, pristine.types)
      |> MapSet.reject(fn [type, attr] -> MapSet.member?(extra_type_attrs, {type, attr}) end)

    cond do
      missing_in_elixir != [] ->
        {:error, "catalog missing core functions: #{inspect(missing_in_elixir)}"}

      unexpected_in_elixir != [] ->
        {:error, "catalog has unexplained core functions: #{inspect(unexpected_in_elixir)}"}

      MapSet.size(extra_elixir_columns) > 0 ->
        {:error,
         "catalog has unexplained columns: #{inspect(MapSet.to_list(extra_elixir_columns))}"}

      MapSet.size(extra_elixir_types) > 0 ->
        {:error,
         "catalog has unexplained type attributes: #{inspect(MapSet.to_list(extra_elixir_types))}"}

      true ->
        :ok
    end
  end

  defp canonical_function_map(functions) do
    Map.new(functions, fn {{name, args}, _} -> {{name, canonical_type_args(args)}, true} end)
  end

  defp canonical_type_args(args) do
    args
    |> String.split(",")
    |> Enum.map_join(",", fn part ->
      part |> String.trim() |> String.split() |> List.last()
    end)
  end

  @spec setup_compat_database!(keyword()) :: UpstreamFixture.t()
  def setup_compat_database!(opts \\ []) do
    admin_url = Keyword.get(opts, :admin_url, UpstreamFixture.default_admin_url())

    suffix = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    database = "pgflow_compat_" <> suffix
    database_url = replace_database(admin_url, database)

    admin_query!(admin_url, "CREATE DATABASE \"#{database}\"")

    repo_name = :"pgflow_compat_repo_#{suffix}"

    {:ok, repo_pid} =
      PgFlow.TestRepo.start_link(
        name: repo_name,
        url: database_url,
        pool: Ecto.Adapters.SQL.Sandbox,
        pool_size: 4
      )

    fixture = %UpstreamFixture{
      admin_url: admin_url,
      application_database: UpstreamFixture.application_database_name(),
      database: database,
      database_url: database_url,
      names: %{},
      repo_name: repo_name,
      repo_pid: repo_pid,
      runs: %{},
      versions: %{core: 0, helpers: 0}
    }

    try do
      install_target!(fixture)
    rescue
      exception ->
        if Process.alive?(repo_pid), do: Supervisor.stop(repo_pid)
        drop_database!(admin_url, database)
        reraise exception, __STACKTRACE__
    end
  end

  @spec teardown_compat_database!(UpstreamFixture.t()) :: :ok
  def teardown_compat_database!(fixture) do
    if Process.alive?(fixture.repo_pid) do
      try do
        GenServer.stop(fixture.repo_pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    drop_database!(fixture.admin_url, fixture.database)
  end

  @spec install_target!(UpstreamFixture.t()) :: UpstreamFixture.t()
  def install_target!(fixture) do
    previous = PgFlow.TestRepo.put_dynamic_repo(fixture.repo_name)

    try do
      PgFlow.TestRepo.query!("CREATE EXTENSION IF NOT EXISTS pgmq")

      Ecto.Migrator.up(PgFlow.TestRepo, 9_000_000_301, PgFlow.Upstream.CoreOnlyMigration,
        log: false
      )

      Ecto.Migrator.up(PgFlow.TestRepo, 9_000_000_305, PgFlow.Upstream.HelpersUpgradeMigration,
        log: false
      )

      %{fixture | versions: %{core: 2, helpers: 5}}
    after
      PgFlow.TestRepo.put_dynamic_repo(previous)
    end
  end

  defp package_versions(checkout) do
    manifest()["harness"]["typescript"]["packages"]
    |> Enum.into(%{}, fn pkg ->
      path = Path.join([checkout, "pkgs", pkg, "package.json"])
      version = path |> File.read!() |> Jason.decode!() |> Map.fetch!("version")
      {pkg, version}
    end)
  end

  defp failure_result(suite, reason) do
    %{
      suite: suite,
      status: :failed,
      tests: 1,
      failures: 1,
      details: %{"error" => reason}
    }
  end

  defp result_to_map(%{
         suite: suite,
         status: status,
         tests: tests,
         failures: failures,
         details: details
       }) do
    %{
      "suite" => Atom.to_string(suite),
      "status" => Atom.to_string(status),
      "tests" => tests,
      "failures" => failures,
      "details" => stringify_keys(details)
    }
  end

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), stringify_keys(value)}
      {key, value} -> {key, stringify_keys(value)}
    end)
  end

  defp stringify_keys(list) when is_list(list), do: Enum.map(list, &stringify_keys/1)
  defp stringify_keys(other), do: other

  defp admin_url_for(database_url), do: replace_database(database_url, "postgres")

  defp database_from_url(url) do
    url |> URI.parse() |> Map.fetch!(:path) |> String.trim_leading("/")
  end

  defp replace_database(url, database) do
    url |> URI.parse() |> Map.put(:path, "/#{database}") |> URI.to_string()
  end

  defp connect(database_url) do
    Postgrex.start_link(postgrex_options(database_url))
  end

  defp run_psql_file!(database_url, path, opts \\ []) do
    uri = URI.parse(database_url)
    [username, password] = String.split(uri.userinfo || "", ":", parts: 2)

    sql_path =
      cond do
        Keyword.get(opts, :adapt_pristine) ->
          write_adapted_pristine_sql!(path, opts[:adapt_pristine])

        Keyword.get(opts, :adapt_cron, false) ->
          write_adapted_sql!(path)

        true ->
          path
      end

    args = [
      "-h",
      uri.host,
      "-p",
      Integer.to_string(uri.port || 5432),
      "-U",
      URI.decode(username),
      "-d",
      database_from_url(database_url),
      "-v",
      "ON_ERROR_STOP=1",
      "-q",
      "-f",
      sql_path
    ]

    result =
      case System.cmd("psql", args,
             env: [{"PGPASSWORD", URI.decode(password)}],
             stderr_to_stdout: true
           ) do
        {_, 0} -> :ok
        {output, status} -> {:error, "psql exited #{status}: #{String.slice(output, 0, 400)}"}
      end

    if Keyword.get(opts, :adapt_cron, false) or Keyword.get(opts, :adapt_pristine) do
      File.rm(sql_path)
    end

    result
  end

  defp write_adapted_sql!(path) do
    adapted =
      path
      |> File.read!()
      |> adapt_cron_sql()

    temp = Path.join(System.tmp_dir!(), "pgflow_upstream_#{:erlang.phash2(path)}.sql")
    File.write!(temp, adapted)
    temp
  end

  defp write_adapted_pristine_sql!(path, file) do
    adapted =
      path
      |> File.read!()
      |> adapt_pristine_migration_sql(file)

    temp = Path.join(System.tmp_dir!(), "pgflow_pristine_#{:erlang.phash2(path)}.sql")
    File.write!(temp, adapted)
    temp
  end

  defp adapt_pristine_migration_sql(sql, "20251209074533_pgflow_worker_management.sql") do
    sql
    |> String.replace("CREATE EXTENSION IF NOT EXISTS \"pg_cron\";\n", "")
    |> drop_sql_object(
      "-- Create \"cleanup_ensure_workers_logs\" function",
      "-- Create \"is_local\" function"
    )
    |> drop_sql_object(
      "-- Create \"ensure_workers\" function",
      "-- Create \"mark_worker_stopped\" function"
    )
    |> drop_sql_object(
      "-- Create \"setup_ensure_workers_cron\" function",
      "-- Create \"track_worker_function\" function"
    )
    |> String.replace("SELECT pgflow.setup_ensure_workers_cron('1 second');\n", "")
  end

  defp adapt_pristine_migration_sql(sql, "20260607175525_pgflow_worker_start_mode.sql") do
    sql
    |> drop_sql_object(
      "-- Modify \"ensure_workers\" function",
      "-- Drop \"track_worker_function\" function"
    )
    |> adapt_cron_sql()
  end

  defp adapt_pristine_migration_sql(sql, _file), do: adapt_cron_sql(sql)

  defp drop_sql_object(sql, start_marker, end_marker) do
    case String.split(sql, start_marker, parts: 2) do
      [before, rest] ->
        case String.split(rest, end_marker, parts: 2) do
          [_dropped, after_object] -> before <> end_marker <> after_object
          _ -> sql
        end

      _ ->
        sql
    end
  end

  defp adapt_cron_sql(sql) do
    sql
    |> String.split("-- Create \"setup_", parts: 2)
    |> List.first()
    |> Kernel.<>("\n")
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

  defp exec!(conn, sql) do
    case Postgrex.query(conn, sql, []) do
      {:ok, _} -> :ok
      {:error, %Postgrex.Error{} = error} -> {:error, Exception.message(error)}
    end
  end

  defp exec_admin!(admin_url, sql) do
    case admin_query!(admin_url, sql) do
      _ -> :ok
    end
  rescue
    exception -> {:error, Exception.message(exception)}
  end

  defp query_rows!(conn, sql, params \\ []) do
    {:ok, %{rows: rows}} = Postgrex.query(conn, sql, params)
    rows
  end

  defp admin_query!(admin_url, sql, params \\ []) do
    {:ok, conn} = connect(admin_url)

    try do
      Postgrex.query!(conn, sql, params)
    after
      GenServer.stop(conn)
    end
  end

  defp drop_database!(admin_url, database) do
    admin_query!(
      admin_url,
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
      [database]
    )

    admin_query!(admin_url, "DROP DATABASE IF EXISTS \"#{database}\"")
    :ok
  end
end
