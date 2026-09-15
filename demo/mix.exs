defmodule PgflowDemo.MixProject do
  use Mix.Project

  def project do
    [
      app: :pgflow_demo,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      dialyzer: dialyzer(),
      compilers: [:phoenix_live_view] ++ Mix.compilers(),
      listeners: [Phoenix.CodeReloader]
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {PgflowDemo.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test, quality: :test]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      pgflow_dependency(),
      {:livefilter, "~> 0.2.0"},

      # Tz - required by PgFlowDashboard for time_zone support
      {:tz, "~> 0.28.2"},

      # Phoenix
      {:phoenix, "~> 1.8.14"},
      {:phoenix_ecto, "~> 4.7.0"},
      {:ecto_sql, "~> 3.14.0"},
      {:postgrex, "~> 0.22.4"},
      {:phoenix_html, "~> 4.3.0"},
      {:phoenix_live_reload, "~> 1.7.0", only: :dev},
      {:phoenix_live_view, "~> 1.2.11"},
      {:lazy_html, "~> 0.1.12", only: :test},
      {:phoenix_live_dashboard, "~> 0.9.1"},
      {:esbuild, "~> 0.10.0", runtime: Mix.env() == :dev},
      {:tailwind, "~> 0.5.1", runtime: Mix.env() == :dev},
      {:heroicons,
       github: "tailwindlabs/heroicons",
       tag: "v2.2.0",
       sparse: "optimized",
       app: false,
       compile: false,
       depth: 1},
      {:telemetry_metrics, "~> 1.2.0"},
      {:telemetry_poller, "~> 1.3.0"},
      {:jason, "~> 1.4.5"},
      {:dns_cluster, "~> 0.3.0"},
      {:bandit, "~> 1.12.5"},

      # HTTP client and content processing
      {:req, "~> 0.7.4"},
      {:floki, "~> 0.38.4"},

      # LLM integration
      {:req_llm, "~> 1.22.0"},

      # Loads .env files in dev/test
      {:dotenvy, "~> 1.2.1"},

      # Syntax highlighting for code display
      {:makeup_elixir, "~> 1.0.1"},

      # Development tools
      {:tidewave, "~> 0.9.0", only: :dev},

      # Code quality
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4.4", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.8", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.15.0", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.23.0", only: [:dev, :test], runtime: false},
      {:ex_dna, "~> 1.5.4", only: [:dev, :test], runtime: false}
    ]
  end

  # Local development exercises the parent checkout directly. The production
  # Docker context contains only this demo app, so it uses the published release.
  defp pgflow_dependency do
    if Mix.env() != :prod or System.get_env("PGFLOW_DEMO_LOCAL") == "1" do
      {:pgflow, path: ".."}
    else
      {:pgflow, "~> 0.4.0"}
    end
  end

  defp dialyzer do
    [
      plt_add_apps: [:ex_unit, :mix],
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
      flags: [:error_handling, :unknown]
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to install project dependencies and perform other setup tasks, run:
  #
  #     $ mix setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      setup: ["deps.get", "ecto.setup", "assets.setup", "assets.build"],
      "ecto.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      "test.setup": ["pgflow_demo.test.setup"],
      test: ["test.setup", "test"],
      "assets.setup": ["tailwind.install --if-missing", "esbuild.install --if-missing"],
      "assets.build": ["compile", "tailwind pgflow_demo", "esbuild pgflow_demo"],
      "assets.deploy": [
        "tailwind pgflow_demo --minify",
        "esbuild pgflow_demo --minify",
        "phx.digest"
      ],
      precommit: ["compile --warnings-as-errors", "deps.unlock --unused", "format", "test"],
      quality: [
        "compile --warnings-as-errors",
        "deps.unlock --unused",
        "format --check-formatted",
        "sobelow --config",
        "ex_dna",
        "doctor",
        "credo --strict"
      ]
    ]
  end
end
