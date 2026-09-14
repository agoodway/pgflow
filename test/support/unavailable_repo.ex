defmodule PgFlow.Test.UnavailableRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :pgflow, adapter: Ecto.Adapters.Postgres

  @impl true
  def init(_type, _opts) do
    config =
      PgFlow.TestRepo.config()
      |> Keyword.merge(
        name: __MODULE__,
        database: "pgflow_missing_bootstrap_fixture",
        pool: DBConnection.ConnectionPool,
        pool_size: 1,
        queue_target: 1,
        queue_interval: 10,
        timeout: 100
      )

    {:ok, config}
  end
end
