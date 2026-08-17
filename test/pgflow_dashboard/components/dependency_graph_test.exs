defmodule PgFlowDashboard.Components.DependencyGraphTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias PgFlowDashboard.Components.DependencyGraph

  test "renders interactive nodes as keyboard-focusable controls" do
    html =
      render_component(&DependencyGraph.dependency_graph/1,
        steps: [%{step_slug: "send_welcome", deps: []}],
        step_states: %{"send_welcome" => "skipped"},
        highlighted_step: "send_welcome",
        on_click: "select_step",
        on_keydown: "select_step_keydown"
      )

    assert html =~ ~s(role="button")
    assert html =~ ~s(tabindex="0")
    assert html =~ ~s(phx-keydown="select_step_keydown")
    assert html =~ ~s(phx-hook="GraphNodeKeyboard")
    assert html =~ ~s(aria-pressed="true")
    assert html =~ ~r/<svg[^>]+role="group"[^>]+aria-label="Flow dependency graph"/
  end

  test "distinguishes skipped nodes from pending nodes without relying on the icon" do
    skipped_html =
      render_component(&DependencyGraph.dependency_graph/1,
        steps: [%{step_slug: "skipped_step", deps: []}],
        step_states: %{"skipped_step" => "skipped"}
      )

    pending_html =
      render_component(&DependencyGraph.dependency_graph/1,
        steps: [%{step_slug: "pending_step", deps: []}]
      )

    assert skipped_html =~ "fill-orange-600"
    assert skipped_html =~ "stroke-orange-800"
    assert skipped_html =~ "dark:fill-amber-400"
    assert skipped_html =~ "dark:stroke-amber-200"
    refute pending_html =~ "fill-orange-600"
  end

  test "renders full humanized labels without shrinking the SVG to the card" do
    html =
      render_component(&DependencyGraph.dependency_graph/1,
        steps: [%{step_slug: "send_welcome_message_to_new_customer", deps: []}]
      )

    assert html =~ "Send Welcome Message To New Customer"
    assert html =~ ~s(role="region")
    assert html =~ ~s(aria-label="Scrollable flow dependency graph")
    assert html =~ ~s(tabindex="0")
    assert html =~ "max-w-none"
    assert html =~ "font-mono"
    refute html =~ "max-w-2xl"
  end

  test "allocates enough intrinsic width for adjacent long labels" do
    html =
      render_component(&DependencyGraph.dependency_graph/1,
        steps: [
          %{step_slug: "www_wide_customer_notification", deps: []},
          %{
            step_slug: "www_wide_subscription_confirmation",
            deps: ["www_wide_customer_notification"]
          }
        ]
      )

    assert html =~ "Www Wide Customer Notification"
    assert html =~ "Www Wide Subscription Confirmation"
    assert html =~ ~r/<svg[^>]+width="([6-9]\d\d|\d{4,})"/
  end

  test "shows a node status tooltip on hover and keyboard focus" do
    html =
      render_component(&DependencyGraph.dependency_graph/1,
        steps: [%{step_slug: "send_welcome", deps: []}],
        step_states: %{"send_welcome" => "skipped"},
        on_click: "select_step"
      )

    assert html =~ "Skipped"
    assert html =~ "group-hover:opacity-100"
    assert html =~ "group-focus-visible:opacity-100"
  end

  test "renders an explicit non-focusable empty state" do
    html = render_component(&DependencyGraph.dependency_graph/1, steps: [])

    assert html =~ "No workflow steps"
    refute html =~ ~s(aria-label="Scrollable flow dependency graph")
  end
end
