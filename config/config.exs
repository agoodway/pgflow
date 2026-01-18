import Config

# Default configuration for PgFlow

config :pgflow,
  attach_default_logger: true

# Import environment specific config
import_config "#{config_env()}.exs"
