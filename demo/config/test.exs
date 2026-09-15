import Config

# Configure your database
#
# The MIX_TEST_PARTITION environment variable can be used
# to provide built-in test partitioning in CI environment.
# Run `mix help test` for more information.
config :pgflow_demo, PgflowDemo.Repo,
  username: System.get_env("PGFLOW_DEMO_DB_USER", "postgres"),
  password: System.get_env("PGFLOW_DEMO_DB_PASSWORD", "postgres"),
  hostname: System.get_env("PGFLOW_DEMO_DB_HOST", "localhost"),
  port: String.to_integer(System.get_env("PGFLOW_DEMO_DB_PORT", "54323")),
  # Test setup owns this database; never default to the development database.
  database: System.get_env("PGFLOW_DEMO_DB_NAME", "pgflow_demo_test"),
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 40,
  queue_target: 5000,
  queue_interval: 1000

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :pgflow_demo, PgflowDemoWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "oA1hETGCcU3ErZ4vfdLiM3E09LqOT1yOxKH3jXdao74O7TO4+WsZZ1eqE7PS78Z4",
  server: false

# Print only warnings and errors during test
config :logger, level: :warning

config :pgflow_demo,
  signal_strategy:
    if(System.get_env("PGFLOW_DEMO_SIGNAL") == "notify", do: :notify, else: :polling),
  min_poll_interval: 100,
  max_poll_interval: 500

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime

# Enable helpful, but potentially expensive runtime checks
config :phoenix_live_view,
  enable_expensive_runtime_checks: true

# Sort query params output of verified routes for robust url comparisons
config :phoenix,
  sort_verified_routes_query_params: true
