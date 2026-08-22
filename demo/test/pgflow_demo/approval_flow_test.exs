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
end
