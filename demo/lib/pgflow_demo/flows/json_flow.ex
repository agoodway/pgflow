defmodule PgflowDemo.Flows.JsonFlow do
  @moduledoc """
  Preserves JSON scalar false and null without coercion.
  """

  use PgFlow.Flow

  @flow slug: :json_flow, max_attempts: 1, timeout: 30

  step :emit do
    fn input, _ctx ->
      %{
        "flag" => Map.get(input, "flag"),
        "empty" => Map.get(input, "empty"),
        "scalar" => Map.get(input, "scalar"),
        "list" => Map.get(input, "list", []),
        "object" => Map.get(input, "object", %{})
      }
    end
  end
end
