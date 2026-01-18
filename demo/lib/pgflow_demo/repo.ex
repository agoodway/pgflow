defmodule PgflowDemo.Repo do
  use Ecto.Repo,
    otp_app: :pgflow_demo,
    adapter: Ecto.Adapters.Postgres
end
