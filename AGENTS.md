# AGENTS.md - PgFlow Elixir Project Guidelines

## Build / Lint / Test Commands

```bash
# Install dependencies
mix deps.get

# Run all tests
mix test

# Run a single test file
mix test test/pgflow/flow_test.exs

# Run a specific test by line number
mix test test/pgflow/flow_test.exs:15

# Run only failed tests
mix test --failed

# Format code
mix format

# Check formatting without changes
mix format --check-formatted

# Run Credo (linter)
mix credo

# Run Credo with only warnings
mix credo --only warning

# Run Dialyzer (type checker)
mix dialyzer

# Run quality checks (compile with warnings as errors, format check, credo)
mix quality

# Run Doctor (documentation coverage report)
mix doctor

# Run Doctor with short report
mix doctor --short

# Run Doctor with summary only
mix doctor --summary

# Run Doctor and raise on failures (for CI)
mix doctor --raise

# Database setup for tests
docker compose up -d
mix pgflow.test.setup

# Reset test database
mix pgflow.test.reset
```

## Code Style Guidelines

### Formatting
- Max line length: 120 characters (per .credo.exs)
- Use `mix format` for consistent formatting
- Inputs: `{mix,.formatter}.exs`, `{config,lib,test}/**/*.{ex,exs}`

### Naming Conventions
- Modules: PascalCase (e.g., `PgFlow.FlowCompiler`)
- Functions: snake_case (e.g., `start_flow_sync`)
- Private functions: prefix with `_` when appropriate
- Predicate functions: end with `?` (not `is_` prefix)
- Module attributes: snake_case with `@` prefix
- Variables: snake_case
- Atoms: snake_case (e.g., `:process_order`)

### Imports and Aliases
- Group aliases at the top of the module
- Use multi-alias syntax for multiple modules from same parent:
  ```elixir
  alias PgFlow.{Client, Config, FlowRegistry, WorkerSupervisor}
  ```
- Avoid nested modules in the same file (causes cyclic dependencies)

### Types and Specs
- Use `@spec` for all public functions
- Define types with `@type` in modules that define structs
- Use built-in types: `String.t()`, `atom()`, `pid()`, `keyword()`, `map()`
- Complex return types: `{:ok, term()} | {:error, term()}`

### Error Handling
- Return `{:ok, result}` or `{:error, reason}` tuples
- Use pattern matching for error handling
- Avoid exceptions for control flow
- Use `with` for multiple operations that may fail:
  ```elixir
  with {:ok, config} <- validate(opts),
       {:ok, pid} <- start_process(config) do
    {:ok, pid}
  end
  ```

### Code Organization
- Public API functions first, then private functions
- Group related functions with `##` doc headers
- Use `@moduledoc` for all modules
- Use `@doc` with examples for all public functions
- Keep functions focused and small

### Testing
- Use `ExUnit.Case, async: true` when tests don't share state
- Use `start_supervised!/1` for processes in tests (ensures cleanup)
- Avoid `Process.sleep/1` and `Process.alive?/1`
- Use `Process.monitor/1` and assert on DOWN messages instead of sleeping
- Use `_ = :sys.get_state/1` to synchronize before next call
- Group tests in `describe` blocks by functionality

### Common Patterns
- Use `defdelegate` to expose functions from other modules
- Use `__MODULE__` instead of hardcoding module name
- Prefer pattern matching over `case`/`cond` when possible
- Use pipes `|>` for data transformation pipelines
- Never use `String.to_atom/1` on user input (memory leak risk)
- Never nest multiple modules in the same file

### Documentation
- Use `@moduledoc """` with clear module purpose
- Include compatibility notes where relevant
- Provide usage examples in `@doc`
- Document options with `## Options` headers
- Document return types clearly

## Project Structure

```
lib/pgflow/           # Core implementation
lib/pgflow/flow/      # Flow-related modules
lib/pgflow/schema/    # Ecto schemas
lib/pgflow/queries/   # SQL queries
lib/pgflow/worker/    # Worker implementations
test/pgflow/          # Unit tests
test/support/         # Test helpers and fixtures
docs/                 # Documentation
```
