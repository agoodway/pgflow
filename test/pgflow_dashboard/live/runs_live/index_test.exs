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

  describe "operational_filters/1 with list values" do
    test "passes in-lists through for status, queue, and type" do
      filters =
        for {field, values} <- [
              status: ["completed", "failed"],
              flow_slug: ["a", "b"],
              flow_type: ["flow", "job"]
            ] do
          Filter.new(LiveFilter.select(field, options: []), :in, values)
        end

      assert Index.operational_filters(filters) ==
               [
                 flow_type: ["flow", "job"],
                 flow_slug: ["a", "b"],
                 status: ["completed", "failed"]
               ]
    end

    test "treats an empty in-list as matching nothing" do
      # `status=in.()` parses to [""]
      for values <- [[""], []] do
        filter = Filter.new(LiveFilter.select(:status, options: []), :in, values)
        assert Index.operational_filters([filter]) == [status: []]
      end
    end
  end

  describe "operational_filters/1 with started-at values that are not full timestamps" do
    setup do
      {:ok, config: LiveFilter.datetime_range(:started_at)}
    end

    test "ignores unparseable endpoints", %{config: config} do
      filter = Filter.new(config, :gte_lte, {"garbage", "also garbage"})
      assert Index.operational_filters([filter]) == []
    end

    test "keeps the valid endpoint when the other is unparseable", %{config: config} do
      filter = Filter.new(config, :gte_lte, {"garbage", DateTime.to_iso8601(@upper_bound)})
      assert Index.operational_filters([filter]) == [started_before: @upper_bound]
    end

    test "reads a date-only value as the start of that UTC day for gte and its end for lte",
         %{config: config} do
      filter = Filter.new(config, :gte_lte, {"2026-11-18", "2026-11-18"})

      opts = Index.operational_filters([filter])

      assert opts[:started_after] == ~U[2026-11-18 00:00:00Z]
      assert opts[:started_before] == ~U[2026-11-18 23:59:59.999999Z]
    end
  end

  describe "operational_filters/1 honors the filter operator" do
    defp select_filter(field, operator, value),
      do: Filter.new(LiveFilter.select(field, options: []), operator, value)

    test "eq and in include" do
      assert Index.operational_filters([select_filter(:status, :eq, "failed")]) == [
               status: "failed"
             ]

      assert Index.operational_filters([select_filter(:flow_slug, :in, ["a", "b"])]) ==
               [flow_slug: ["a", "b"]]
    end

    test "neq and not_in exclude" do
      assert Index.operational_filters([select_filter(:status, :neq, "failed")]) ==
               [status: {:not, "failed"}]

      assert Index.operational_filters([select_filter(:flow_type, :not_in, ["job"])]) ==
               [flow_type: {:not, ["job"]}]
    end

    test "an empty not_in excludes nothing" do
      for values <- [[""], []] do
        assert Index.operational_filters([select_filter(:status, :not_in, values)]) ==
                 [status: {:not, []}]
      end
    end

    test "other operators are ignored instead of misapplied" do
      for operator <- [:like, :ilike, :is_null, :gt] do
        assert Index.operational_filters([select_filter(:status, operator, "failed")]) == []
      end
    end
  end
end
