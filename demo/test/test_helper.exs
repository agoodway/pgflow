ExUnit.start()

unless System.get_env("PGFLOW_DEMO_SKIP_DB") == "1" do
  Ecto.Adapters.SQL.Sandbox.mode(PgflowDemo.Repo, :manual)
end
