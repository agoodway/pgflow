defmodule PgFlowDashboard.DecimalRenderingTest do
  @moduledoc """
  Guards the dashboard against `Protocol.UndefinedError` at render time.

  Postgres hands back the dashboard's `numeric` columns as `Decimal`, and
  `Phoenix.HTML.Safe` has no implementation for that struct — interpolating one
  into a template raises, and only on the page that happens to show it. These
  columns are spread over eleven LiveViews, so the failure mode is a page that
  works until a value is non-null.

  This reads the templates as source rather than rendering them: every LiveView
  here needs a live database and a mounted socket, which would make the same
  assertion cost a fixture per page.
  """
  use ExUnit.Case, async: true

  @live_dir "lib/pgflow_dashboard/live"

  # Columns the v01 SQL declares `numeric`. `p95_duration_ms` is `double
  # precision`, which Postgrex decodes to a float and templates can render.
  @decimal_columns ~w(avg_duration_ms duration_ms progress_percent success_rate_24h)

  # `progress` is `ProgressBar.progress_bar/1`'s attr; the component normalizes
  # a Decimal itself, so handing it one raw is correct rather than a bug.
  @safe_attrs ~w(progress)

  describe "numeric columns reaching the markup" do
    test "every Decimal-valued interpolation goes through a formatter" do
      offenders =
        @live_dir
        |> Path.join("**/*.ex")
        |> Path.wildcard()
        |> Enum.flat_map(&raw_interpolations/1)

      assert offenders == [],
             """
             These interpolate a Decimal straight into the template, which raises
             Protocol.UndefinedError when the value is not null. Wrap each in
             LiveHelpers.format_percent/1 or LiveHelpers.format_duration/1:

             #{Enum.map_join(offenders, "\n", &"  #{&1}")}
             """
    end
  end

  defp raw_interpolations(path) do
    path
    |> File.read!()
    |> String.split("\n")
    |> Enum.with_index(1)
    |> Enum.filter(fn {line, _no} -> raw_decimal?(line) end)
    |> Enum.map(fn {line, no} -> "#{path}:#{no}: #{String.trim(line)}" end)
  end

  defp raw_decimal?(line) do
    Enum.any?(@decimal_columns, fn column ->
      Regex.match?(~r/\{\s*@?\w+\.#{column}\s*\}/, line) and
        not Enum.any?(@safe_attrs, &String.contains?(line, "#{&1}={"))
    end)
  end
end
