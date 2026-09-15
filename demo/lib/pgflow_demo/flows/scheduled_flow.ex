defmodule PgflowDemo.Flows.ScheduledFlow do
  @moduledoc """
  Cron-capable flow used by the manual-tick preset. Starting this worker does not
  install a schedule; cron enablement is an explicit deployment operation.
  """

  use PgFlow.Flow

  @flow slug: :scheduled_flow,
        max_attempts: 1,
        timeout: 30,
        cron: [schedule: "0 * * * *", input: %{"source" => "cron"}]

  step :tick do
    fn input, _ctx ->
      %{
        "source" => input["source"] || "manual",
        "at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }
    end
  end
end
