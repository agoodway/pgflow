defmodule PgFlow.Test.FixedTimeZoneDatabase do
  @moduledoc false

  @behaviour Calendar.TimeZoneDatabase

  @period %{std_offset: 0, utc_offset: -14_400, zone_abbr: "EDT"}

  @impl true
  def time_zone_period_from_utc_iso_days(_iso_days, "America/New_York"),
    do: {:ok, @period}

  def time_zone_period_from_utc_iso_days(iso_days, time_zone),
    do: Calendar.UTCOnlyTimeZoneDatabase.time_zone_period_from_utc_iso_days(iso_days, time_zone)

  @impl true
  def time_zone_periods_from_wall_datetime(_naive_datetime, "America/New_York"),
    do: {:ok, @period}

  def time_zone_periods_from_wall_datetime(naive_datetime, time_zone),
    do:
      Calendar.UTCOnlyTimeZoneDatabase.time_zone_periods_from_wall_datetime(
        naive_datetime,
        time_zone
      )
end
