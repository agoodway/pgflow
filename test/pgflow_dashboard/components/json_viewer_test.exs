defmodule PgFlowDashboard.Components.JsonViewerTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias PgFlowDashboard.Components.JsonViewer

  describe "json_viewer/1" do
    test "renders structured values as semantic syntax tokens" do
      html =
        render_component(&JsonViewer.json_viewer/1,
          id: "run-input",
          data: %{"active" => true, "count" => 3, "name" => "Ada", "value" => nil}
        )

      assert html =~ ~s(id="run-input")
      assert html =~ "json-token-key"
      assert html =~ "json-token-string"
      assert html =~ "json-token-number"
      assert html =~ "json-token-boolean"
      assert html =~ "json-token-null"
    end

    test "escapes string values instead of emitting raw HTML" do
      html =
        render_component(&JsonViewer.json_viewer/1,
          id: "unsafe-json",
          data: %{"markup" => "<script>alert('nope')</script>"}
        )

      assert html =~ "&lt;script&gt;"
      refute html =~ "<script>alert"
    end

    test "wires copy to the rendered code and labels the action" do
      html =
        render_component(&JsonViewer.json_viewer/1,
          id: "copyable-json",
          data: %{"ok" => true}
        )

      assert html =~ ~s(id="copyable-json-copy")
      assert html =~ ~s(phx-hook="CopyToClipboard")
      assert html =~ ~s(phx-update="ignore")
      assert html =~ ~s(data-copy-target="copyable-json-code")
      assert html =~ ~s(aria-label="Copy JSON to clipboard")
      assert html =~ "data-copy-default"
      assert html =~ "data-copy-success"
      assert html =~ "data-copy-announcement"
    end

    test "renders an explicit empty value for nil" do
      html = render_component(&JsonViewer.json_viewer/1, id: "empty-json", data: nil)

      assert html =~ "No data"
      refute html =~ ~s(phx-hook="CopyToClipboard")
    end

    test "falls back safely for map keys that are not JSON-compatible" do
      html =
        render_component(&JsonViewer.json_viewer/1,
          id: "non-json-key",
          data: %{{:tuple, :key} => "value"}
        )

      assert html =~ "{:tuple, :key}"
      assert html =~ "value"
    end
  end
end
