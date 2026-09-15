defmodule PgflowDemo.Flows.Policies.IfNotFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :policy_if_not_flow, max_attempts: 1, timeout: 30

  step :seed do
    fn input, _ctx -> %{"mode" => input["mode"] || "inactive"} end
  end

  step :conditional,
    depends_on: [:seed],
    if_not: %{"seed" => %{"mode" => "blocked"}},
    when_unmet: :skip do
    fn deps, _ctx -> %{"ran" => true, "mode" => deps["seed"]["mode"]} end
  end
end
