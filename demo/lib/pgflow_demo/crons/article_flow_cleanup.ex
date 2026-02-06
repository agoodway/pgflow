defmodule PgflowDemo.Crons.ArticleFlowCleanup do
  @moduledoc """
  Hourly cleanup cron that prunes old article_flow run records.

  Demonstrates PgFlow's cron functionality by calling `PgFlow.Queries.prune_data/3`
  to remove only article_flow completed/failed runs older than the retention period.
  """
  use PgFlow.Cron

  # Require to ensure module is compiled before macro expansion
  require PgFlow.Queries

  @cron queue: :article_flow_cleanup,
        expression: "0 * * * *",
        max_attempts: 3,
        timeout: 60,
        input: %{"retention_hours" => 24}

  schedule do
    fn input, _ctx ->
      retention_hours = input["retention_hours"] || 24

      {:ok, result} =
        PgFlow.Queries.prune_data(
          PgflowDemo.Repo,
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
