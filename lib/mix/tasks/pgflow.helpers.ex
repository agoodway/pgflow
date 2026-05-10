defmodule Mix.Tasks.Pgflow.Helpers do
  @moduledoc false

  @doc false
  def resolve_repo(nil) do
    app = Mix.Project.config()[:app]

    case Application.get_env(app, :ecto_repos, []) do
      [repo] ->
        repo

      [repo | _] = repos ->
        Mix.shell().info(
          "Multiple repos configured (#{inspect(repos)}); using #{inspect(repo)}. " <>
            "Pass `--repo` to choose a specific one."
        )

        repo

      [] ->
        Mix.raise("No Ecto repos configured. Add `:ecto_repos` to your app config.")
    end
  end

  def resolve_repo(repo_string) do
    Module.concat([repo_string])
  end

  @doc false
  def get_app_module do
    case Mix.Project.config()[:app] do
      nil ->
        Mix.raise("Could not determine app name from Mix.Project")

      app ->
        app |> to_string() |> Macro.camelize()
    end
  end

  @doc false
  def generate_timestamp do
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

  @doc false
  def camelize(string) do
    string
    |> String.split("_")
    |> Enum.map_join(&String.capitalize/1)
  end

  @doc """
  Shared driver for the migration-generator mix tasks.

  Parses the standard `--migrations-path/-p` switch, ensures the directory
  exists, builds a timestamped filename like `<ts>_<suffix>.exs`, writes the
  migration body produced by `content_fn.(app_module)`, and prints the message
  produced by `message_fn.(filepath)`.

  Each generator task can stay a thin shim around this — keep the per-task
  content and message functions local for grep-ability and easier testing.
  """
  @spec write_migration([String.t()], String.t(), (String.t() -> String.t()), (String.t() ->
                                                                                 String.t())) ::
          :ok
  def write_migration(args, suffix, content_fn, message_fn \\ &default_migration_message/1)
      when is_binary(suffix) and is_function(content_fn, 1) and is_function(message_fn, 1) do
    {opts, _, _} =
      OptionParser.parse(args,
        switches: [migrations_path: :string],
        aliases: [p: :migrations_path]
      )

    migrations_path = Keyword.get(opts, :migrations_path, "priv/repo/migrations")
    File.mkdir_p!(migrations_path)

    timestamp = generate_timestamp()
    app_module = get_app_module()
    filepath = Path.join(migrations_path, "#{timestamp}_#{suffix}.exs")

    File.write!(filepath, content_fn.(app_module))
    Mix.shell().info(message_fn.(filepath))
    :ok
  end

  defp default_migration_message(filepath) do
    """
    Generated migration: #{filepath}

    Run the migration with:
        mix ecto.migrate
    """
  end

  @doc false
  def priv_path(repo) do
    case repo.config()[:priv] do
      nil -> "priv/repo"
      priv when is_binary(priv) -> priv
    end
  rescue
    _ -> "priv/repo"
  end

  @doc false
  def build_down_statements(flow_slug, has_cron, unschedule_sql)

  def build_down_statements(flow_slug, true, unschedule_sql) do
    """
        execute "#{unschedule_sql}"
    #{base_down_statements(flow_slug)}
    """
    |> String.trim_trailing()
  end

  def build_down_statements(flow_slug, false, _unschedule_sql) do
    base_down_statements(flow_slug)
  end

  defp base_down_statements(flow_slug) do
    """
        execute "DELETE FROM pgflow.deps WHERE flow_slug = '#{flow_slug}'"
        execute "DELETE FROM pgflow.steps WHERE flow_slug = '#{flow_slug}'"
        execute "DELETE FROM pgflow.flows WHERE flow_slug = '#{flow_slug}'"
        execute "SELECT pgmq.drop_queue('#{flow_slug}')"
    """
    |> String.trim_trailing()
  end
end
