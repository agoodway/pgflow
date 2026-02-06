defmodule PgFlowDashboard.Router do
  @moduledoc """
  Router macro for mounting the PgFlow Dashboard.

  ## Usage

      defmodule MyAppWeb.Router do
        use MyAppWeb, :router

        import PgFlowDashboard.Router

        scope "/" do
          pipe_through [:browser]

          pgflow_dashboard "/pgflow",
            repo: MyApp.Repo,
            pubsub: MyApp.PubSub
        end
      end

  ## Options

  See `PgFlowDashboard.Config` for all available options.

  Required:
    * `:repo` - The Ecto repository module
    * `:pubsub` - The Phoenix.PubSub module

  Optional:
    * `:refresh_interval` - Polling interval (default: 5000ms)
    * `:time_zone` - Time zone for timestamps (default: "UTC")
    * `:default_time_range` - Default filter (default: :last_24h)
    * `:max_grid_runs` - Max runs in history grid (default: 50)

  """

  alias PgFlowDashboard.Live.LiveHelpers

  @doc """
  Generates routes for the PgFlow Dashboard.

  ## Authentication

  In production, you should protect the dashboard with authentication.
  Use the `:on_mount` option to add an authentication hook:

      pgflow_dashboard "/pgflow",
        repo: MyApp.Repo,
        pubsub: MyApp.PubSub,
        on_mount: [{MyAppWeb.Auth, :ensure_admin}]

  The `:on_mount` hooks are added to the LiveView session alongside
  the dashboard's own mount hook. See `Phoenix.LiveView.Router` for
  more information on `on_mount` hooks.

  """
  defmacro pgflow_dashboard(path, opts \\ []) do
    quote bind_quoted: [path: path, opts: opts] do
      scope path, alias: false, as: false do
        import Phoenix.LiveView.Router, only: [live: 3, live: 4, live_session: 3]

        # Extract user-provided on_mount hooks, if any
        {user_hooks, config_opts} = Keyword.pop(opts, :on_mount, [])

        # Combine dashboard mount with any user-provided hooks
        all_hooks = [{PgFlowDashboard.Router, :mount_dashboard} | List.wrap(user_hooks)]

        live_session :pgflow_dashboard,
          on_mount: all_hooks,
          session: {PgFlowDashboard.Router, :session, [config_opts, path]} do
          live("/", PgFlowDashboard.Live.OverviewLive, :index)
          live("/runs", PgFlowDashboard.Live.RunsLive.Index, :index)
          live("/runs/:id", PgFlowDashboard.Live.RunsLive.Show, :show)
          live("/flows", PgFlowDashboard.Live.FlowsLive.Index, :index)
          live("/flows/:slug", PgFlowDashboard.Live.FlowsLive.Show, :show)
          live("/jobs", PgFlowDashboard.Live.JobsLive.Index, :index)
          live("/jobs/:id", PgFlowDashboard.Live.JobsLive.Show, :show)
          live("/crons", PgFlowDashboard.Live.CronsLive.Index, :index)
          live("/crons/:id", PgFlowDashboard.Live.CronsLive.Show, :show)
          live("/workers", PgFlowDashboard.Live.WorkersLive, :index)
          live("/workers/:id", PgFlowDashboard.Live.WorkersLive.Show, :show)
        end
      end
    end
  end

  @doc false
  def session(_conn, opts, path) do
    config = PgFlowDashboard.Config.validate!(opts)

    %{
      "pgflow_dashboard_config" => Enum.into(config, %{}),
      "base_path" => path
    }
  end

  @doc false
  def on_mount(:mount_dashboard, _params, session, socket) do
    LiveHelpers.on_mount(session, socket)
  end
end
