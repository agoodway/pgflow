defmodule PgflowDemo.Checks.StepModeGuard do
  @moduledoc """
  Shows that Elixir workers refuse a flow compiled in upstream step mode.

  1. Provisions `ts_step_mode_shadow` the way a TypeScript worker using
     per-step queues would: `pgflow.ensure_flow_compiled(slug, shape, 'step',
     route_map)`. The route map lists `lower(flow_slug || '__' || step_slug)`
     for each step in shape order and must match the SQL derivation exactly.
     The call returns `compiled` the first time and `verified` afterwards, so
     repeated runs leave one definition in place.
  2. Snapshots the flow, step, and dependency rows.
  3. Runs worker bootstrap (`PgFlow.Worker.Bootstrap.prepare/2`) for
     `PgflowDemo.Flows.TsStepModeShadowFlow`, an Elixir module with the same
     slug that is not part of the `PgFlow.Supervisor` flow list.
  4. Snapshots again and confirms bootstrap returned
     `{:error, {:unsupported_queue_mode, slug, "step"}}`, left the rows
     untouched, and did not track an Elixir worker function.

  The check only reads or writes rows for `ts_step_mode_shadow`.
  """

  @behaviour PgflowDemo.ScenarioChecks

  alias Ecto.Adapters.SQL
  alias PgFlow.Flow.{Definition, Shape}
  alias PgFlow.Worker.Bootstrap
  alias PgflowDemo.Flows.TsStepModeShadowFlow
  alias PgflowDemo.Repo

  @worker_function "elixir:PgflowDemo.Flows.TsStepModeShadowFlow"

  @snapshot_sql """
  SELECT jsonb_build_object(
    'flow', (SELECT to_jsonb(f) FROM pgflow.flows AS f WHERE f.flow_slug = $1::text),
    'steps', (
      SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY s.step_index), '[]'::jsonb)
      FROM pgflow.steps AS s
      WHERE s.flow_slug = $1::text
    ),
    'deps', (
      SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY d.step_slug, d.dep_slug), '[]'::jsonb)
      FROM pgflow.deps AS d
      WHERE d.flow_slug = $1::text
    )
  )
  """

  @impl true
  @spec run(map()) :: map()
  def run(_input) do
    definition = TsStepModeShadowFlow.__pgflow_definition__()
    slug = Definition.slug_to_string(definition)
    shape = Shape.from_definition(definition)

    provision_status = provision_step_mode(slug, shape)
    before = snapshot(slug)
    bootstrap = Bootstrap.prepare(Repo, definition)
    after_snapshot = snapshot(slug)

    Map.merge(
      %{
        "flow_slug" => slug,
        "provision_status" => provision_status,
        "queue_mode" => get_in(after_snapshot, ["flow", "queue_mode"]),
        "step_queues" => Enum.map(after_snapshot["steps"], & &1["queue_name"]),
        "definition_unchanged" => before == after_snapshot,
        "worker_function_tracked" => worker_function_tracked?(),
        "registered_with_flow_starter" =>
          PgFlow.FlowStarter.module_status(TsStepModeShadowFlow) != nil
      },
      bootstrap_outcome(bootstrap)
    )
  end

  defp provision_step_mode(slug, shape) do
    route_map =
      Enum.map(shape["steps"], fn step ->
        %{"stepSlug" => step["slug"], "queueName" => String.downcase("#{slug}__#{step["slug"]}")}
      end)

    %{rows: [[result]]} =
      SQL.query!(
        Repo,
        "SELECT pgflow.ensure_flow_compiled($1::text, $2::jsonb, 'step', $3::jsonb)",
        [slug, shape, route_map]
      )

    result["status"]
  end

  defp snapshot(slug) do
    %{rows: [[snapshot]]} = SQL.query!(Repo, @snapshot_sql, [slug])
    snapshot
  end

  defp worker_function_tracked? do
    %{rows: [[tracked?]]} =
      SQL.query!(
        Repo,
        "SELECT EXISTS (SELECT 1 FROM pgflow.worker_functions WHERE function_name = $1::text)",
        [@worker_function]
      )

    tracked?
  end

  defp bootstrap_outcome({:error, {:unsupported_queue_mode, slug, mode}}) do
    %{
      "bootstrap_result" => "refused",
      "refusal" => %{
        "reason" => "unsupported_queue_mode",
        "flow_slug" => slug,
        "queue_mode" => mode
      }
    }
  end

  defp bootstrap_outcome({:ok, %{compilation_status: status}}) do
    %{"bootstrap_result" => "accepted", "refusal" => nil, "compilation_status" => status}
  end

  defp bootstrap_outcome({:error, reason}) do
    %{"bootstrap_result" => "error", "refusal" => nil, "error" => inspect(reason)}
  end
end
