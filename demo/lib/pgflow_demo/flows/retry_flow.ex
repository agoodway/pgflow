defmodule PgflowDemo.Flows.RetryFlow do
  @moduledoc """
  Deterministic retry demonstration using handler attempt context.
  """

  use PgFlow.Flow

  @flow slug: :retry_flow, max_attempts: 5, base_delay: 1, timeout: 30

  step :retry_step do
    fn input, ctx ->
      if ctx.attempt < input["succeed_on_attempt"] do
        raise "Demonstration retry"
      else
        %{"attempt" => ctx.attempt}
      end
    end
  end
end
