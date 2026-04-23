defmodule Mix.Tasks.Pgflow.Gen.PgmqMigration do
  @shortdoc "Downloads pgmq SQL from upstream and generates an Ecto migration"

  @moduledoc """
  Downloads the pgmq SQL-only install bundle from the official pgmq/pgmq
  repository at task-runtime, persists it into the consuming app's `priv/`
  tree, and generates an Ecto migration that applies it.

  pgflow requires pgmq 1.8+ for `LISTEN/NOTIFY` dispatch. This task is the
  recommended way to install pgmq for apps using pgflow on hosts where pgmq
  is not available as a native extension (Neon, Supabase, self-hosted
  without build tools).

  ## Usage

      mix pgflow.gen.pgmq_migration
      mix pgflow.gen.pgmq_migration --version 1.11.0
      mix pgflow.gen.pgmq_migration --url https://... --sha256 <hash>
      mix pgflow.gen.pgmq_migration --migrations-path priv/repo/migrations

  ## Options

    * `--version` - pgmq version tag to fetch. Default: `1.11.0`.
      Must be a key in the internal known-versions map, OR paired with
      `--url` and `--sha256` for custom versions.

    * `--url` - Override the upstream URL. Requires `--sha256`.

    * `--sha256` - Expected sha256 of the downloaded bytes. Required when
      `--url` is used; otherwise looked up from the known-versions map.

    * `--migrations-path` - Where to write the generated migration.
      Default: `priv/repo/migrations`.

    * `--skip-sha256` - Skip integrity check. Emits a loud warning. Not
      recommended outside of local experimentation.

  ## Files written

    * `priv/pgflow/pgmq/pgmq-<version>.sql` — the bundled SQL, committed to
      your repo so `mix ecto.migrate` never needs network access.

    * `priv/repo/migrations/<timestamp>_install_pgmq.exs` — the Ecto
      migration wrapper.

  ## Generated migration shape

      defmodule MyApp.Repo.Migrations.InstallPgmq do
        use Ecto.Migration

        @disable_ddl_transaction true
        @disable_migration_lock true

        @sql_relpath "pgflow/pgmq/pgmq-1.11.0.sql"

        def up do
          path = Path.join(:code.priv_dir(:my_app), @sql_relpath)
          execute(File.read!(path))
        end

        def down do
          execute("DROP SCHEMA IF EXISTS pgmq CASCADE")
        end
      end

  ## Known versions

  #{inspect(%{"1.11.0" => "89f9d8a3adc43434afcf814c4afc3ef8957a20418eb120801922435238714e7a"}, pretty: true)}

  ## Why fetch at task-runtime

  Bundling the ~2000-line pgmq SQL in pgflow's `priv/` would bloat the dep
  and couple pgflow releases to pgmq releases. Fetching once at generation
  time and persisting to the consuming repo keeps pgflow slim and makes
  the SQL reviewable in the consumer's git history. `mix ecto.migrate`
  never needs network; it reads the persisted file.
  """

  use Mix.Task

  @default_version "1.11.0"
  @upstream_url_template "https://raw.githubusercontent.com/pgmq/pgmq/v~s/pgmq-extension/sql/pgmq.sql"

  @known_versions %{
    "1.11.0" => "89f9d8a3adc43434afcf814c4afc3ef8957a20418eb120801922435238714e7a"
  }

  @impl Mix.Task
  def run(args) do
    {:ok, _} = Application.ensure_all_started(:inets)
    {:ok, _} = Application.ensure_all_started(:ssl)

    {opts, _, _} =
      OptionParser.parse(args,
        switches: [
          version: :string,
          url: :string,
          sha256: :string,
          migrations_path: :string,
          skip_sha256: :boolean
        ]
      )

    version = Keyword.get(opts, :version, @default_version)
    url_override = Keyword.get(opts, :url)
    sha_override = Keyword.get(opts, :sha256)
    skip_sha? = Keyword.get(opts, :skip_sha256, false)
    migrations_path = Keyword.get(opts, :migrations_path, "priv/repo/migrations")

    {url, expected_sha} =
      resolve_source(version, url_override, sha_override, skip_sha?)

    app =
      Mix.Project.config()[:app] ||
        Mix.raise("Could not determine consuming app name from Mix.Project")

    sql_relpath = "pgflow/pgmq/pgmq-#{version}.sql"
    sql_abspath = Path.join("priv", sql_relpath)

    Mix.shell().info("Fetching pgmq #{version} from #{url}")
    bytes = fetch!(url)
    verify_integrity!(bytes, expected_sha, skip_sha?)

    sql_abspath |> Path.dirname() |> File.mkdir_p!()
    File.write!(sql_abspath, bytes)
    Mix.shell().info("  Wrote #{sql_abspath} (#{byte_size(bytes)} bytes)")

    File.mkdir_p!(migrations_path)
    timestamp = generate_timestamp()
    migration_filename = "#{timestamp}_install_pgmq.exs"
    migration_path = Path.join(migrations_path, migration_filename)
    migration_content = render_migration(app, sql_relpath)
    File.write!(migration_path, migration_content)

    Mix.shell().info("""

    Generated migration: #{migration_path}

    Run:
        mix ecto.migrate

    This will apply pgmq #{version} to your database via the bundled SQL.
    """)
  end

  defp resolve_source(version, nil, nil, false = _skip?) do
    case Map.fetch(@known_versions, version) do
      {:ok, sha} ->
        url = :io_lib.format(@upstream_url_template, [version]) |> IO.iodata_to_binary()
        {url, sha}

      :error ->
        Mix.raise("""
        Unknown pgmq version: #{version}.

        Either pass `--version` matching a known version (#{@known_versions |> Map.keys() |> Enum.join(", ")}),
        or override the source with `--url <url> --sha256 <hex>`.
        """)
    end
  end

  defp resolve_source(version, nil, nil, true = _skip?) do
    url = :io_lib.format(@upstream_url_template, [version]) |> IO.iodata_to_binary()
    {url, nil}
  end

  defp resolve_source(_version, url, sha, _skip?) when is_binary(url) and is_binary(sha) do
    {url, sha}
  end

  defp resolve_source(_version, url, _sha, skip?) when is_binary(url) and not skip? do
    Mix.raise("--url requires --sha256 (or --skip-sha256 to bypass, not recommended)")
  end

  defp resolve_source(_version, url, _sha, true = _skip?) when is_binary(url) do
    {url, nil}
  end

  defp fetch!(url) do
    headers = [{~c"User-Agent", ~c"pgflow-elixir"}]

    case :httpc.request(:get, {String.to_charlist(url), headers}, [], body_format: :binary) do
      {:ok, {{_, 200, _}, _, body}} when is_binary(body) ->
        body

      {:ok, {{_, status, _}, _, body}} ->
        Mix.raise("Failed to fetch #{url}: HTTP #{status}: #{body}")

      {:error, reason} ->
        Mix.raise("Failed to fetch #{url}: #{inspect(reason)}")
    end
  end

  defp verify_integrity!(_bytes, _expected, true = _skip?) do
    Mix.shell().info("  sha256 check SKIPPED — integrity not verified")
  end

  defp verify_integrity!(bytes, expected, false = _skip?) do
    actual = :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

    if actual == expected do
      Mix.shell().info("  sha256 OK (#{String.slice(expected, 0, 12)}...)")
    else
      Mix.raise("""
      SHA256 mismatch!

        expected: #{expected}
        actual:   #{actual}

      The downloaded pgmq SQL does not match the pinned hash. Either the
      upstream file changed or the connection was tampered with. Refusing
      to write the file.
      """)
    end
  end

  defp render_migration(app, sql_relpath) do
    module_name = app |> to_string() |> Macro.camelize()

    """
    defmodule #{module_name}.Repo.Migrations.InstallPgmq do
      @moduledoc \"\"\"
      Installs pgmq via the bundled SQL file.

      Generated by: `mix pgflow.gen.pgmq_migration`
      SQL source:   priv/#{sql_relpath}

      The pgmq SQL contains many top-level statements. Postgrex rejects
      multi-statement prepared queries (`42601 syntax_error`), so we use
      `PgFlow.Sql.Splitter` to split the SQL into single statements and
      run each via `execute/1`. This keeps the migration portable — no
      `psql` on PATH required at migrate time, which matters for slim
      release images and managed CI runners.

      The splitter is dollar-quote aware, so plpgsql function bodies with
      internal semicolons (`BEGIN ... END;` inside `$$ ... $$`) split
      correctly.
      \"\"\"
      use Ecto.Migration

      @disable_ddl_transaction true
      @disable_migration_lock true

      @otp_app #{inspect(app)}
      @sql_relpath #{inspect(sql_relpath)}

      def up do
        sql_path = Path.join(:code.priv_dir(@otp_app), @sql_relpath)

        sql_path
        |> File.read!()
        |> PgFlow.Sql.Splitter.split()
        |> Enum.each(&execute/1)
      end

      def down do
        execute("DROP SCHEMA IF EXISTS pgmq CASCADE")
      end
    end
    """
  end

  defp generate_timestamp do
    {{year, month, day}, {hour, minute, second}} = :calendar.universal_time()

    :io_lib.format("~4..0B~2..0B~2..0B~2..0B~2..0B~2..0B", [
      year,
      month,
      day,
      hour,
      minute,
      second
    ])
    |> IO.iodata_to_binary()
  end
end
