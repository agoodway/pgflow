defmodule PgflowDemo.Release do
  @moduledoc """
  Used for executing DB release tasks when run in production without Mix
  installed.
  """
  @app :pgflow_demo

  @doc """
  Runs all pending migrations for every configured repo.
  """
  @spec migrate() :: [{:ok, term(), term()}]
  def migrate do
    load_app()

    for repo <- repos() do
      {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, all: true))
    end
  end

  @doc """
  Loads `priv/repo/seeds.exs` (when present) for every configured repo.
  """
  @spec seed() :: [{:ok, term(), term()}]
  def seed do
    load_app()

    for repo <- repos() do
      {:ok, _, _} = Ecto.Migrator.with_repo(repo, &load_seeds/1)
    end
  end

  @doc """
  Rolls the given repo back to `version`.
  """
  @spec rollback(module(), integer()) :: {:ok, term(), term()}
  def rollback(repo, version) do
    load_app()
    {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :down, to: version))
  end

  defp load_seeds(repo) do
    seed_file = Application.app_dir(@app, "priv/repo/seeds.exs")

    if File.exists?(seed_file) do
      Code.eval_file(seed_file)
      IO.puts("Seeds loaded for #{inspect(repo)}")
    else
      IO.puts("No seed file found at #{seed_file}")
    end
  end

  defp repos do
    Application.fetch_env!(@app, :ecto_repos)
  end

  defp load_app do
    Application.ensure_all_started(:ssl)
    Application.ensure_loaded(@app)
  end
end
