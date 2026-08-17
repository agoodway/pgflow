defmodule PgFlowDashboard.Live.LiveHelpersTest do
  use ExUnit.Case, async: true

  alias PgFlowDashboard.Live.LiveHelpers

  describe "format_percent/1" do
    test "renders a Decimal as a string templates can interpolate" do
      assert LiveHelpers.format_percent(Decimal.new("100.0")) == "100.0"
    end

    test "rounds to a single decimal place" do
      assert LiveHelpers.format_percent(Decimal.new("66.6667")) == "66.7"
    end

    test "accepts plain numbers" do
      assert LiveHelpers.format_percent(0) == "0.0"
      assert LiveHelpers.format_percent(42.25) == "42.3"
    end

    test "a missing percentage reads as zero rather than blank" do
      assert LiveHelpers.format_percent(nil) == "0"
    end

    test "anything unrecognized degrades to zero instead of raising" do
      assert LiveHelpers.format_percent("nonsense") == "0"
    end
  end

  describe "local_day_bounds/2" do
    test "accepts the dashboard's documented UTC alias" do
      now = ~U[2026-08-18 02:00:00Z]

      assert LiveHelpers.local_day_bounds(now, "UTC") ==
               {~U[2026-08-18 00:00:00Z], ~U[2026-08-18 23:59:59.999999Z]}
    end

    test "returns UTC bounds for today in the configured time zone" do
      now = ~U[2026-08-18 02:00:00Z]

      assert LiveHelpers.local_day_bounds(
               now,
               "America/New_York",
               PgFlow.Test.FixedTimeZoneDatabase
             ) == {~U[2026-08-17 04:00:00Z], ~U[2026-08-18 03:59:59.999999Z]}
    end
  end
end
