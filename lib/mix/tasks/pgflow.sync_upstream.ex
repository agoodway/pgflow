defmodule Mix.Tasks.Pgflow.SyncUpstream do
  @shortdoc "Builds or checks a pinned pgflow core delta"
  @moduledoc """
  Builds a deterministic core SQL delta from a reviewed upstream checkout.

      mix pgflow.sync_upstream --checkout PATH --sha SHA --version 02
      mix pgflow.sync_upstream --checkout PATH --sha SHA --version 02 --check

  V01 is published and can never be rewritten by this task.
  """

  use Mix.Task

  alias PgFlow.Upstream.Bundle

  @switches [checkout: :string, sha: :string, version: :string, check: :boolean]
  @published_versions ["01"]
  @down_sql """
  DO $pgflow_forward_only$
  BEGIN
    RAISE EXCEPTION 'PgFlow core V02 is forward-only: new statuses and queue identities cannot safely be discarded automatically. Restore from a pre-upgrade backup or roll forward with a corrective migration.';
  END
  $pgflow_forward_only$
  """

  @impl Mix.Task
  def run(args) do
    unless Mix.Project.config()[:app] == :pgflow do
      Mix.raise("pgflow.sync_upstream can only be run from the pgflow project")
    end

    Mix.Task.run("app.start")

    with {:ok, opts} <- parse(args),
         :ok <- validate_version(opts[:version]),
         {:ok, bundle} <- Bundle.build(opts[:checkout], opts[:sha]) do
      files = output_files(opts[:version], bundle)

      if opts[:check] do
        check!(files)
      else
        write!(files)
      end
    else
      {:error, message} -> Mix.raise(message)
    end
  end

  defp parse(args) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, [], []} ->
        missing =
          [:checkout, :sha, :version]
          |> Enum.reject(&Keyword.has_key?(opts, &1))

        if missing == [] do
          {:ok, opts}
        else
          {:error, "missing required options: #{Enum.map_join(missing, ", ", &"--#{&1}")}"}
        end

      {_opts, positional, invalid} ->
        {:error, "invalid arguments: #{inspect(positional ++ invalid)}"}
    end
  end

  defp validate_version(version) when version in @published_versions do
    {:error, "refusing to rewrite published core version V#{version}"}
  end

  defp validate_version("02"), do: :ok

  defp validate_version(version),
    do: {:error, "unsupported core delta version #{inspect(version)}; expected 02"}

  defp output_files(version, bundle) do
    directory = Path.join(File.cwd!(), "priv/pgflow_core/sql/versions/v#{version}")

    %{
      Path.join(directory, "v#{version}_up.sql") => bundle.sql,
      Path.join(directory, "v#{version}_down.sql") => @down_sql,
      Path.join(directory, "v#{version}_manifest.json") =>
        Jason.encode!(bundle.manifest, pretty: true) <> "\n"
    }
  end

  defp check!(files) do
    mismatches =
      Enum.reject(files, fn {path, expected} ->
        File.read(path) == {:ok, expected}
      end)

    case mismatches do
      [] ->
        Mix.shell().info("pgflow core V02 output is current")

      entries ->
        Mix.raise("generated output differs: #{Enum.map_join(entries, ", ", &elem(&1, 0))}")
    end
  end

  defp write!(files) do
    Enum.each(files, fn {path, content} ->
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, content)
    end)

    Mix.shell().info("generated pgflow core V02")
  end
end
