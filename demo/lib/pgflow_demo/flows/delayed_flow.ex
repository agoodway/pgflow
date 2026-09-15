defmodule PgflowDemo.Flows.DelayedFlow do
  @moduledoc """
  Start delay before a dependent step becomes visible.
  """

  use PgFlow.Flow

  @flow slug: :delayed_flow, max_attempts: 1, timeout: 30

  step :prepare do
    fn input, _ctx -> %{"ready" => true, "label" => input["label"] || "demo"} end
  end

  step :delayed, depends_on: [:prepare], start_delay: 1 do
    fn deps, _ctx ->
      %{"label" => deps["prepare"]["label"], "delayed" => true}
    end
  end
end
