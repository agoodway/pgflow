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
  @spec pgflow_dashboard(String.t(), keyword()) :: Macro.t()
  defmacro pgflow_dashboard(path, opts \\ []) do
    PgFlowDashboard.Router.ensure_dashboard_dependencies!()

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

  @doc """
  Builds the LiveView session map used by PgFlowDashboard routes.

  The session contains validated dashboard configuration and the resolved base
  path for links inside the dashboard.
  """
  @spec session(Plug.Conn.t(), keyword(), String.t()) :: map()
  def session(conn, opts, path) do
    config = PgFlowDashboard.Config.validate!(opts)

    %{
      "pgflow_dashboard_config" => Enum.into(config, %{}),
      "base_path" => compute_base_path(conn.request_path, path)
    }
  end

  @doc """
  Derive the full base path by finding where the dashboard path appears
  in the request URL. This handles outer scopes (e.g., "/admin/pgflow"
  when `pgflow_dashboard("/pgflow")` is mounted inside `scope "/admin"`).
  """
  @spec compute_base_path(String.t(), String.t()) :: String.t()
  def compute_base_path(request_path, path) do
    suffix = String.trim_trailing(path, "/")

    case String.split(request_path, suffix, parts: 2) do
      [prefix, _rest] -> prefix <> suffix
      _ -> path
    end
  end

  @doc """
  Mounts dashboard configuration into each PgFlowDashboard LiveView socket.
  """
  @spec on_mount(:mount_dashboard, map(), map(), Phoenix.LiveView.Socket.t()) ::
          {:cont, Phoenix.LiveView.Socket.t()}
  def on_mount(:mount_dashboard, _params, session, socket) do
    LiveHelpers.on_mount(session, socket)
  end

  @doc """
  Ensures optional dashboard dependencies are available before routes are mounted.

  PgFlow can be used without PgFlowDashboard. When an application imports and
  mounts `pgflow_dashboard/2`, the dashboard requires Phoenix LiveView and
  LiveFilter to be installed in the host project.

  No timezone database is required here. Timestamps are shifted through the
  host's configured `:time_zone_database`, so an app that already has one keeps
  using it, and an app with none renders timestamps in UTC.
  """
  @spec ensure_dashboard_dependencies!() :: :ok
  def ensure_dashboard_dependencies! do
    check_dashboard_dependencies!([
      {:phoenix_live_view, Phoenix.LiveView},
      {:livefilter, LiveFilter}
    ])
  end

  @doc false
  @spec check_dashboard_dependencies!([{atom(), module()}]) :: :ok
  def check_dashboard_dependencies!(deps) when is_list(deps) do
    case missing_dependencies(deps) do
      [] ->
        :ok

      missing ->
        raise ArgumentError, missing_dependencies_message(missing)
    end
  end

  @doc false
  @spec missing_dependencies([{atom(), module()}]) :: [atom()]
  def missing_dependencies(deps) when is_list(deps) do
    deps
    |> Enum.reject(fn {_app, module} -> Code.ensure_loaded?(module) end)
    |> Enum.map(fn {app, _module} -> app end)
  end

  defp missing_dependencies_message(missing) do
    deps = Enum.map_join(missing, ", ", &inspect/1)

    "PgFlowDashboard requires #{deps}; add the missing dependencies before mounting pgflow_dashboard/2"
  end
end
