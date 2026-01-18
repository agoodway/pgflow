defmodule PgFlow.TestRepo do
  @moduledoc """
  Ecto repository for PgFlow tests.
  """
  use Ecto.Repo,
    otp_app: :pgflow,
    adapter: Ecto.Adapters.Postgres
end
