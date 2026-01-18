defmodule PgFlow.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # FlowRegistry is always started (ETS table for flow definitions)
      PgFlow.FlowRegistry
    ]

    opts = [strategy: :one_for_one, name: PgFlow.ApplicationSupervisor]
    Supervisor.start_link(children, opts)
  end
end
