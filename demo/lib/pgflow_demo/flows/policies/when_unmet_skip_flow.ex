defmodule PgflowDemo.Flows.Policies.WhenUnmetSkipFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :policy_when_unmet_skip_flow, max_attempts: 1, timeout: 30

  step :seed do
    fn input, _ctx -> %{"enabled" => input["enabled"] || false} end
  end

  step :gated,
    depends_on: [:seed],
    if: %{"seed" => %{"enabled" => true}},
    when_unmet: :skip do
    fn _deps, _ctx -> %{"ok" => true} end
  end

  step :after_gate, depends_on: [:gated] do
    fn deps, _ctx -> %{"after" => Map.has_key?(deps, "gated")} end
  end
end
