import Config

port = String.to_integer(System.get_env("PGFLOW_TEST_PORT", "54323"))
database = System.get_env("PGFLOW_TEST_DATABASE", "pgflow_test")

# Configure the test repository
config :pgflow, PgFlow.TestRepo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  port: port,
  database: database,
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10

# Configure PgFlow for testing
config :pgflow,
  ecto_repos: [PgFlow.TestRepo],
  attach_default_logger: false

# Print only warnings and errors during test
config :logger, level: :warning
