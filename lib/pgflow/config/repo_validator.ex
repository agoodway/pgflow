defmodule PgFlow.Config.RepoValidator do
  @moduledoc false

  @doc false
  def validate_repo!(repo) do
    unless Code.ensure_loaded?(repo) do
      raise ArgumentError, "repo module #{inspect(repo)} is not loaded"
    end

    unless function_exported?(repo, :__adapter__, 0) do
      raise ArgumentError,
            "repo module #{inspect(repo)} does not implement Ecto.Repo behaviour"
    end

    :ok
  end
end
