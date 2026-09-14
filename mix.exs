defmodule PgFlow.MixProject do
  use Mix.Project

  @version "0.3.4"
  @source_url "https://github.com/agoodway/pgflow"

  def project do
    [
      app: :pgflow,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),

      # Hex
      description: description(),
      package: package(),

      # Dialyzer
      dialyzer: [plt_add_apps: [:mix, :inets, :ex_unit]],

      # Docs
      name: "PgFlow",
      source_url: @source_url,
      docs: docs()
    ]
  end

  def cli do
    [preferred_envs: [quality: :test]]
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
      {:ecto_sql, "~> 3.14.0"},
      {:postgrex, "~> 0.22.4"},
      {:jason, "~> 1.4.5"},
      {:telemetry, "~> 1.4.2"},
      {:nimble_options, "~> 1.1.1"},
      {:crontab, "~> 1.2.0"},

      # Dashboard (optional)
      {:phoenix_live_view, "~> 1.2.11", optional: true},
      {:phoenix, "~> 1.8.14", optional: true},
      {:livefilter, "~> 0.2.0", optional: true},
      # {:livefilter, path: "../livefilter", optional: true},
      {:ecto_evolver, "~> 0.1.0"},

      # Dev/Test
      {:ex_doc, "~> 0.40.4", only: :dev, runtime: false},
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4.8", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.23.0", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4.4", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.15.0", only: [:dev, :test], runtime: false},
      {:ex_dna, "~> 1.5.4", only: [:dev, :test], runtime: false}
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
        "sobelow --config",
        "ex_dna",
        "doctor",
        "credo --strict",
        "dialyzer"
      ]
    ]
  end

  defp description do
    "Workflows, background jobs and cron in Elixir and Postgres powered by PGMQ."
  end

  defp package do
    [
      maintainers: ["Chase Pursley"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files:
        ~w(lib priv mix.exs README.md LICENSE .formatter.exs) ++
          Enum.reject(Path.wildcard("docs/**/*"), fn path ->
            File.dir?(path) or String.starts_with?(path, "docs/superpowers/") or
              path == "docs/2026-09-14-external-waits-upstream-alignment-brief.md"
          end)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "docs/ARCHITECTURE.md",
        "docs/COMPARISON.md",
        "docs/DASHBOARD.md",
        "docs/ELIXIR_VS_SUPABASE.md",
        "docs/LIVE_CLIENT.md"
      ],
      source_ref: "v#{@version}"
    ]
  end
end
