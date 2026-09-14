defmodule PgflowDemo.Flows.Exhaustion.SkipFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :exhaustion_skip_flow, max_attempts: 1, base_delay: 0, timeout: 30

  step :fail_soft, when_exhausted: :skip do
    fn _input, _ctx -> raise "exhaustion skip demo" end
  end

  step :finish, depends_on: [:fail_soft] do
    fn deps, _ctx -> %{"continued" => not Map.has_key?(deps, "fail_soft")} end
  end
end
