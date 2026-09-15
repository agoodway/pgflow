defmodule PgflowDemo.Jobs.RecordJob do
  @moduledoc """
  Local no-side-effect job that echoes payload and attempt count.
  """

  use PgFlow.Job

  @job slug: :record_job, max_attempts: 1, timeout: 30

  perform do
    fn input, ctx ->
      %{
        "payload" => input["payload"] || %{},
        "attempt" => ctx.attempt,
        "recorded_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }
    end
  end
end
