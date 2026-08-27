defmodule PgflowDemo.Flows.ApprovalFlow do
  @moduledoc """
  Demo flow that parks for a human approval signal.

  DAG Structure:
  ```
  create_order → await_approval → charge
  ```
  """

  use PgFlow.Flow

  @flow queue: :approval_flow, max_attempts: 3, base_delay: 1, timeout: 30

  step :create_order do
    fn input, _ctx ->
      %{
        "order_id" => input["order_id"],
        "amount" => input["amount"]
      }
    end
  end

  step :await_approval, depends_on: [:create_order], max_attempts: 1 do
    fn deps, ctx ->
      order = deps["create_order"]

      case PgFlow.Context.await_signal(ctx, wait_for: {1, :hour}, wait_timeout: 0) do
        {:ok, %{"decision" => "approved"}} -> Map.put(order, "decision", "approved")
        {:ok, _} -> raise "rejected"
        {:error, :timeout} -> raise "no decision"
      end
    end
  end

  step :charge, depends_on: [:await_approval] do
    fn deps, _ctx ->
      approved = deps["await_approval"]

      %{
        "charged" => true,
        "order_id" => approved["order_id"],
        "amount" => approved["amount"],
        "decision" => approved["decision"]
      }
    end
  end
end
