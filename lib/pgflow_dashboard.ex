defmodule PgFlowDashboard do
  @moduledoc """
  A Phoenix LiveView dashboard for monitoring PgFlow workflow execution.

  PgFlowDashboard provides real-time visibility into workflow execution with
  a read-only, security-first approach.

  ## Features

  - Real-time overview of workers, runs, and queue depth
  - Detailed run inspection with step states and task details
  - Flow visualization with dependency graphs
  - Worker health monitoring
  - GitHub-style run history grid

  ## Installation

  1. Add the dashboard to your router:

      defmodule MyAppWeb.Router do
        use MyAppWeb, :router

        import PgFlowDashboard.Router

        scope "/" do
          pipe_through [:browser]
          pgflow_dashboard "/pgflow",
            repo: MyApp.Repo,
            pubsub: MyApp.PubSub,
            auth_handler: MyAppWeb.PgFlowAuth
        end
      end

  2. Generate and run the dashboard migration:

      mix pgflow_dashboard.gen.migration
      mix ecto.migrate

  ## Configuration Options

  See `PgFlowDashboard.Config` for all available options.
  """

  @doc """
  Returns the current version of PgFlowDashboard.
  """
  @spec version() :: String.t()
  def version, do: "0.1.0"

  @doc """
  Returns the child specification for starting PgFlowDashboard's supervision tree.

  This allows adding PgFlowDashboard to your application's supervision tree:

      children = [
        # ... other children
        PgFlowDashboard
      ]

  Or with options:

      {PgFlowDashboard, name: MyApp.PgFlowDashboardSupervisor}

  """
  defdelegate child_spec(opts), to: PgFlowDashboard.Supervisor
end
