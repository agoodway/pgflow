defmodule PgflowDemo.ApprovalFlowTest do
  use ExUnit.Case, async: true

  alias PgflowDemo.Flows.ApprovalFlow

  test "defines a three-step approval chain" do
    defn = ApprovalFlow.__pgflow_definition__()

    assert ApprovalFlow.__pgflow_slug__() == :approval_flow
    assert Enum.map(defn.steps, & &1.slug) == [:create_order, :await_approval, :charge]

    await = Enum.find(defn.steps, &(&1.slug == :await_approval))
    assert await.depends_on == [:create_order]
    assert await.max_attempts == 1

    charge = Enum.find(defn.steps, &(&1.slug == :charge))
    assert charge.depends_on == [:await_approval]
  end

  test "await_approval carries order data to charge" do
    charge_handler = ApprovalFlow.__pgflow_handler__(:charge)

    assert ApprovalFlow.__pgflow_definition__().steps
           |> Enum.find(&(&1.slug == :charge))
           |> Map.fetch!(:depends_on) == [:await_approval]

    approved = %{
      "order_id" => "ord_demo",
      "amount" => 42,
      "decision" => "approved"
    }

    assert charge_handler.(%{"await_approval" => approved}, nil) == %{
             "charged" => true,
             "order_id" => "ord_demo",
             "amount" => 42,
             "decision" => "approved"
           }
  end
end
