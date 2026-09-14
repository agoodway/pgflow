defmodule PgFlow.Migrations.Core.V02 do
  @moduledoc "Upstream core delta through 94490709; see the bundled manifest."
  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "02",
    sql_path: "pgflow_core/sql/versions"
end
