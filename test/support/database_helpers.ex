defmodule PgFlow.Test.DatabaseHelpers do
  @moduledoc """
  Builds commands that target the configured ordinary test database.
  """

  @doc "Builds the psql arguments used to load the internal SQL test helpers."
  @spec psql_args(keyword(), String.t()) :: [String.t()]
  def psql_args(repo_config, helper_path) do
    port = resolve_port(repo_config)
    database = resolve_database(repo_config)

    [
      "-h",
      Keyword.fetch!(repo_config, :hostname),
      "-p",
      Integer.to_string(port),
      "-U",
      Keyword.fetch!(repo_config, :username),
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
  end

  @doc "Builds Postgrex connection options for the ordinary test database."
  @spec connection_opts(keyword()) :: keyword()
  def connection_opts(repo_config) do
    [
      hostname: Keyword.fetch!(repo_config, :hostname),
      port: resolve_port(repo_config),
      database: resolve_database(repo_config),
      username: Keyword.fetch!(repo_config, :username),
      password: Keyword.fetch!(repo_config, :password)
    ]
  end

  @doc "Resolves the ordinary test port from the current environment or repository configuration."
  @spec resolve_port(keyword()) :: pos_integer()
  def resolve_port(repo_config) do
    case System.get_env("PGFLOW_TEST_PORT") do
      nil -> Keyword.fetch!(repo_config, :port)
      port -> String.to_integer(port)
    end
  end

  @doc "Resolves the ordinary test database from the current environment or repository configuration."
  @spec resolve_database(keyword()) :: String.t()
  def resolve_database(repo_config) do
    System.get_env("PGFLOW_TEST_DATABASE") || Keyword.fetch!(repo_config, :database)
  end
end
