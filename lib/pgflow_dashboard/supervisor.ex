defmodule PgFlowDashboard.Supervisor do
  @moduledoc """
  Supervisor for PgFlowDashboard processes.

  Manages the MetricsCache GenServer and ensures it restarts on failure.

  ## Usage

  Add to your application's supervision tree:

      children = [
        # ... other children
        {PgFlowDashboard.Supervisor, []}
      ]

  Or with options:

      {PgFlowDashboard.Supervisor, name: MyApp.PgFlowDashboardSupervisor}

  """

  use Supervisor

  @doc """
  Starts the PgFlowDashboard supervisor.

  ## Options

    * `:name` - Optional name for the supervisor. Defaults to `PgFlowDashboard.Supervisor`.

  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(_opts) do
    children = [
      PgFlowDashboard.Cache.MetricsCache
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Returns the child specification for this supervisor.

  This allows the supervisor to be added to a parent supervision tree:

      children = [
        PgFlowDashboard.Supervisor
      ]

  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end
end
