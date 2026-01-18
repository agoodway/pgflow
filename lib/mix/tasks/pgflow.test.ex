defmodule Mix.Tasks.Pgflow.Test.Setup do
  @shortdoc "Sets up the test database for pgflow"

  @moduledoc """
  Sets up the test database by starting Docker containers and running migrations.

  ## Usage

      mix pgflow.test.setup

  ## What It Does

    1. Starts Docker Compose services defined in test/support/db/compose.yaml
    2. Waits for PostgreSQL to be ready
    3. The pgflow schema is automatically loaded via init scripts

  """

  use Mix.Task

  @compose_file "test/support/db/compose.yaml"

  @impl Mix.Task
  def run(_args) do
    unless File.exists?(@compose_file) do
      Mix.raise("Compose file not found: #{@compose_file}")
    end

    Mix.shell().info("Starting test database...")

    # Start Docker Compose
    case System.cmd("docker", ["compose", "-f", @compose_file, "up", "-d", "--wait"],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        Mix.shell().info(output)
        Mix.shell().info("✓ Test database is ready!")

      {output, code} ->
        Mix.shell().error(output)
        Mix.raise("Failed to start Docker Compose (exit code: #{code})")
    end
  end
end

defmodule Mix.Tasks.Pgflow.Test.Teardown do
  @shortdoc "Tears down the test database for pgflow"

  @moduledoc """
  Stops and removes the test database Docker containers.

  ## Usage

      mix pgflow.test.teardown

  """

  use Mix.Task

  @compose_file "test/support/db/compose.yaml"

  @impl Mix.Task
  def run(_args) do
    unless File.exists?(@compose_file) do
      Mix.raise("Compose file not found: #{@compose_file}")
    end

    Mix.shell().info("Stopping test database...")

    case System.cmd("docker", ["compose", "-f", @compose_file, "down", "-v"],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        Mix.shell().info(output)
        Mix.shell().info("✓ Test database stopped and removed")

      {output, code} ->
        Mix.shell().error(output)
        Mix.raise("Failed to stop Docker Compose (exit code: #{code})")
    end
  end
end

defmodule Mix.Tasks.Pgflow.Test.Reset do
  @shortdoc "Resets the test database (teardown + setup)"

  @moduledoc """
  Resets the test database by stopping and restarting containers.

  ## Usage

      mix pgflow.test.reset

  """

  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("pgflow.test.teardown")
    Mix.Task.run("pgflow.test.setup")
  end
end
