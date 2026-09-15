defmodule PgflowDemo.Flows.Exhaustion.SkipCascadeFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :exhaustion_skip_cascade_flow, max_attempts: 1, base_delay: 0, timeout: 30

  step :fail_soft, when_exhausted: :skip_cascade do
    fn _input, _ctx -> raise "exhaustion skip cascade demo" end
  end

  step :downstream, depends_on: [:fail_soft] do
    fn _deps, _ctx -> %{"downstream" => true} end
  end
end
