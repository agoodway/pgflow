defmodule PgflowDemo.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Attach telemetry broadcaster for real-time LiveView updates
    PgflowDemoWeb.TelemetryBroadcaster.attach()

    children = [
      PgflowDemoWeb.Telemetry,
      PgflowDemo.Repo,
      {DNSCluster, query: Application.get_env(:pgflow_demo, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: PgflowDemo.PubSub},
      # PgFlow Supervisor - processes flows with our repo
      {PgFlow.Supervisor, repo: PgflowDemo.Repo, flows: [PgflowDemo.Flows.ArticleFlow]},
      # Start to serve requests, typically the last entry
      PgflowDemoWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: PgflowDemo.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Clean up telemetry handlers on application stop
  @impl true
  def stop(_state) do
    PgflowDemoWeb.TelemetryBroadcaster.detach()
    :ok
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    PgflowDemoWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
