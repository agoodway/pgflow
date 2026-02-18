defmodule PgFlow.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/pgflow-dev/pgflow"

  def project do
    [
      app: :pgflow,
      version: @version,
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),

      # Hex
      description: "Elixir implementation of pgflow workflow engine",
      package: package(),

      # Dialyzer
      dialyzer: [plt_add_apps: [:mix, :inets]],

      # Docs
      name: "PgFlow",
      source_url: @source_url,
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {PgFlow.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Core
      {:ecto_sql, "~> 3.10"},
      {:postgrex, "~> 0.17"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:nimble_options, "~> 1.0"},
      {:crontab, "~> 1.1"},

      # Dashboard (optional)
      {:phoenix_live_view, "~> 1.0", optional: true},
      {:phoenix, "~> 1.7", optional: true},
      {:live_filter, github: "agoodway/livefilter", optional: true},
      {:timex, "~> 3.7"},
      {:pg_evolver, github: "agoodway/pg_evolver"},

      # Dev/Test
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get"],
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      # Run to check the quality of your code
      quality: [
        "compile --warnings-as-errors",
        "deps.unlock --unused",
        "format --check-formatted",
        "credo --strict",
        "doctor",
        "dialyzer"
      ]
    ]
  end

  defp package do
    [
      maintainers: ["PgFlow Team"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_ref: "v#{@version}"
    ]
  end
end
