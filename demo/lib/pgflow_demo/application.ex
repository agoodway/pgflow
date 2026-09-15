defmodule PgflowDemo.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    children = [
      PgflowDemoWeb.Telemetry,
      PgflowDemo.Repo,
      PgflowDemo.ScenarioControls.ReleaseRegistry,
      {DNSCluster, query: Application.get_env(:pgflow_demo, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: PgflowDemo.PubSub},
      # PgFlow Supervisor - processes flows and jobs with our repo
      # pubsub: enables the telemetry-to-PubSub bridge for real-time LiveView updates
      {PgFlow.Supervisor,
       repo: PgflowDemo.Repo,
       flows: [
         PgflowDemo.Flows.ArticleFlow,
         PgflowDemo.Flows.OnboardingFlow,
         PgflowDemo.Flows.ParallelFlow,
         PgflowDemo.Flows.MapFlow,
         PgflowDemo.Flows.RootMapFlow,
         PgflowDemo.Flows.RetryFlow,
         PgflowDemo.Flows.JsonFlow,
         PgflowDemo.Flows.TimeoutFlow,
         PgflowDemo.Flows.DelayedFlow,
         PgflowDemo.Flows.RecoveryFlow,
         PgflowDemo.Flows.QueueIdentityFlow,
         PgflowDemo.Flows.ScheduledFlow,
         PgflowDemo.Flows.Policies.IfMetFlow,
         PgflowDemo.Flows.Policies.IfNotFlow,
         PgflowDemo.Flows.Policies.WhenUnmetSkipFlow,
         PgflowDemo.Flows.Policies.WhenUnmetSkipCascadeFlow,
         PgflowDemo.Flows.Policies.WhenUnmetFailFlow,
         PgflowDemo.Flows.Exhaustion.FailFlow,
         PgflowDemo.Flows.Exhaustion.SkipFlow,
         PgflowDemo.Flows.Exhaustion.SkipCascadeFlow
       ],
       jobs: [PgflowDemo.Jobs.ArticleFlowCleanup, PgflowDemo.Jobs.RecordJob],
       signal_strategy: Application.get_env(:pgflow_demo, :signal_strategy, :notify),
       notify_throttle_ms: 50,
       min_poll_interval: Application.get_env(:pgflow_demo, :min_poll_interval, 100),
       max_poll_interval: Application.get_env(:pgflow_demo, :max_poll_interval, 1_000),
       pubsub: PgflowDemo.PubSub},
      # PgFlowDashboard Supervisor - manages dashboard processes (MetricsCache)
      PgFlowDashboard,
      # Start to serve requests, typically the last entry
      PgflowDemoWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: PgflowDemo.Supervisor]

    with {:ok, pid} <- Supervisor.start_link(children, opts) do
      case PgFlow.FlowStarter.await_ready(30_000) do
        :ok ->
          :ok

        result ->
          Logger.warning(
            "PgFlow startup is not ready: #{inspect(result)}; #{inspect(PgFlow.FlowStarter.status())}"
          )
      end

      {:ok, pid}
    end
  end

  @impl true
  def stop(_state) do
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
