ExUnit.start()
Ecto.Adapters.SQL.Sandbox.mode(PgflowDemo.Repo, :manual)

# PgFlow workers poll during application startup; auto mode avoids ownership noise
# for integration scenario tests. Per-case DataCase sandboxes still work via checkout.
{:ok, _} = Application.ensure_all_started(:pgflow_demo)
Ecto.Adapters.SQL.Sandbox.mode(PgflowDemo.Repo, :auto)

ExUnit.after_suite(fn _ ->
  # Stop background pollers before their sandbox pool closes.
  Application.stop(:pgflow_demo)
end)
