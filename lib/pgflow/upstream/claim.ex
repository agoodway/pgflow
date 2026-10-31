defmodule PgFlow.Upstream.Claim do
  @moduledoc """
  Recorded eight-column `start_tasks` bodies.

  Helpers V05 installs the core V02 four-argument fragment. Helpers V07
  installs the core V03 five-argument fragment. Each checksum guards its
  file; the importer determinism check separately verifies the relationship
  to the core bundle.
  """

  @fragment_path "priv/pgflow_helpers/sql/fragments/start_tasks_eight_column_v02.sql"
  @checksum "41dcf9970da6d194fdd66606b11f516d40a2edeb2fec074eeb47ae8259f9ca3c"

  @fragment_v03_path "priv/pgflow_helpers/sql/fragments/start_tasks_eight_column_v03.sql"
  @checksum_v03 "0d2a2dea13364ba4fade896011ba3e88e414519e2c891e1735f2e1badadc6df1"

  @doc "Returns the recorded V02 SQL fragment with `$SCHEMA$` placeholders."
  @spec eight_column_start_tasks_sql() :: String.t()
  def eight_column_start_tasks_sql do
    read_fragment!(@fragment_path)
  end

  @doc "Returns the recorded V03 SQL fragment with `$SCHEMA$` placeholders."
  @spec eight_column_start_tasks_v03_sql() :: String.t()
  def eight_column_start_tasks_v03_sql do
    read_fragment!(@fragment_v03_path)
  end

  @doc "SHA-256 of the recorded V02 fragment bytes."
  @spec checksum() :: String.t()
  def checksum, do: @checksum

  @doc "SHA-256 of the recorded V03 fragment bytes."
  @spec checksum_v03() :: String.t()
  def checksum_v03, do: @checksum_v03

  @doc "Verifies the on-disk V02 fragment still matches the recorded checksum."
  @spec verify!() :: :ok
  def verify!, do: verify_checksum!(eight_column_start_tasks_sql(), @checksum, "V02")

  @doc "Verifies the on-disk V03 fragment still matches the recorded checksum."
  @spec verify_v03!() :: :ok
  def verify_v03!, do: verify_checksum!(eight_column_start_tasks_v03_sql(), @checksum_v03, "V03")

  defp read_fragment!(relative_path) do
    :pgflow
    |> Application.app_dir(relative_path)
    |> File.read!()
  end

  defp verify_checksum!(sql, expected, label) do
    actual =
      sql
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    if actual == expected do
      :ok
    else
      raise "recorded start_tasks #{label} fragment checksum mismatch: expected #{expected}, got #{actual}"
    end
  end
end
