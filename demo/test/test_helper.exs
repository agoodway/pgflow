ExUnit.start()

# `mix test` starts the app, and its PgFlow workers begin polling before this
# file runs. Switching the sandbox to :manual here would revoke connections those
# pollers hold mid-query, so the pool stays in :auto. Per-case DataCase
# sandboxes still work via explicit checkout.
{:ok, _} = Application.ensure_all_started(:pgflow_demo)
Ecto.Adapters.SQL.Sandbox.mode(PgflowDemo.Repo, :auto)

ExUnit.after_suite(fn _ ->
  # Stop background pollers before their sandbox pool closes.
  Application.stop(:pgflow_demo)
end)
