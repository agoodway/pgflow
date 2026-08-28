defmodule PgFlowDashboard.Live.RunsLive.IndexTest do
  use ExUnit.Case, async: true

  alias LiveFilter.Filter
  alias PgFlowDashboard.Live.RunsLive.Index

  @lower_bound ~U[2026-08-28 12:00:00.000000Z]
  @upper_bound ~U[2026-08-28 13:00:00.000000Z]

  test "preserves either endpoint of a half-open started-at range" do
    config = LiveFilter.datetime_range(:started_at)

    lower_only =
      Filter.new(config, :gte_lte, {DateTime.to_iso8601(@lower_bound), nil})

    upper_only =
      Filter.new(config, :gte_lte, {nil, DateTime.to_iso8601(@upper_bound)})

    assert Index.operational_filters([lower_only]) == [started_after: @lower_bound]
    assert Index.operational_filters([upper_only]) == [started_before: @upper_bound]
  end
end
