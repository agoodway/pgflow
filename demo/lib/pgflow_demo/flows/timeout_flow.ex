defmodule PgflowDemo.Flows.TimeoutFlow do
  @moduledoc """
  Step-level timeout override demonstration.
  """

  use PgFlow.Flow

  @flow slug: :timeout_flow, max_attempts: 1, timeout: 30

  step :fast do
    fn input, _ctx -> %{"ok" => true, "should_timeout" => input["should_timeout"]} end
  end

  step :slow, depends_on: [:fast], timeout: 1 do
    fn input, _ctx ->
      if input["fast"]["should_timeout"] do
        receive do
        end
      else
        %{"ok" => true}
      end
    end
  end
end
