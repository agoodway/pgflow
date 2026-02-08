defmodule PgflowDemo.Jobs.ArticleFlowCleanup do
  @moduledoc """
  Hourly cleanup job that prunes old article_flow run records.

  Compiles to: `cron.schedule(...)` in Postgres via pg_cron extension.
  """
  use PgFlow.Job

  alias PgFlow.Queries.Flows, as: FlowQueries
  alias PgflowDemo.Repo

  # Require to ensure module is compiled before macro expansion
  require FlowQueries

  @job queue: :article_flow_cleanup,
       max_attempts: 3,
       timeout: 60,
       cron: [
         schedule: "@hourly",
         input: %{"retention_hours" => 24}
       ]

  perform do
    fn input, _ctx ->
      retention_hours = input["retention_hours"] || 24

      {:ok, result} =
        FlowQueries.prune_data(
          Repo,
          retention_hours,
          flow_slugs: ["article_flow"]
        )

      Map.merge(result, %{
        retention_hours: retention_hours,
        flow_slugs: ["article_flow"],
        completed_at: DateTime.utc_now() |> DateTime.to_iso8601()
      })
    end
  end
end
