defmodule PgflowDemo.Flows.QueueIdentityFlow do
  @moduledoc """
  Mixed-case slug with canonical lowercase PGMQ route.
  """

  use PgFlow.Flow

  @flow slug: :MixedCaseDemo, max_attempts: 1, timeout: 30

  step :work do
    fn input, _ctx -> %{"slug" => "MixedCaseDemo", "value" => input["value"] || 1} end
  end
end
