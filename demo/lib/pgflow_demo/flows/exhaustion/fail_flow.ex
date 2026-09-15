defmodule PgflowDemo.Flows.Exhaustion.FailFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :exhaustion_fail_flow, max_attempts: 2, base_delay: 0, timeout: 30

  step :always_fails, when_exhausted: :fail do
    fn _input, _ctx -> raise "exhaustion fail demo" end
  end
end
