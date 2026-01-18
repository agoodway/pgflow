import Config

# Configure the test repository
config :pgflow, PgFlow.TestRepo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  port: 54322,
  database: "pgflow_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10

# Configure PgFlow for testing
config :pgflow,
  ecto_repos: [PgFlow.TestRepo],
  attach_default_logger: false

# Print only warnings and errors during test
config :logger, level: :warning
