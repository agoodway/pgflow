# Job Demo Tab

Date: 2026-08-22
Branch: `feat/await-signals`

## Goal

Give the Phoenix demo a **Job** tab that teaches one-off jobs the way Article teaches DAGs and Cron teaches schedules: enqueue a fake send-email job, watch it complete, read the `use PgFlow.Job` source.

A visitor clicks **Start Job**, the handler returns a sent map with no mailer, Output shows that map, and the highlighted Elixir is the canonical README-style example.

## Non-goals

- DAG, event log, or step-output panel on this tab.
- A real mailer, SMTP, or LLM call.
- To / subject / body form (payload is canned).
- Fail / retry UI (`max_attempts: 3` stays in source only).
- Merging this tab with Cron, or making Cron runnable.
- Dashboard changes.
- A standalone `/job` route.
- Refactoring FlowDemoLive tab plumbing beyond the new tab.
- Browser/end-to-end tests that boot a worker inside `demo/`.

## Context

Homepage tabs today: **Article · Onboarding · Approval · Cron**. The first three are runnable flows with a DAG. Cron is a read-only scheduled job (`ArticleFlowCleanup`). There is no on-demand Job: nothing calls `PgFlow.enqueue/2` from the UI, and nothing shows a `perform` block without a cron schedule.

Jobs are single-step flows. `Client.enqueue/2` already delegates to `start_flow/2`, so the existing subscribe + `reconcile_run_state/2` path works without a library change.

## Architecture

**Approach: Job tab on the existing homepage LiveView (layout B).**

```
Visitor                FlowDemoLive                    SendEmail / worker              Postgres
   |                        |                              |                            |
   | Start Job              |                              |                            |
   |----------------------->| Client.enqueue(SendEmail,    |                            |
   |                        |   canned_input)              |                            |
   |                        |----------------------------->| deliver completes          |
   |                        | subscribe pgflow:run:<id>    |                            |
   |                        | reconcile snapshot           |                            |
   |                        | run_completed + output       |                            |
```

### Job

New module `PgflowDemo.Jobs.SendEmail`:

```elixir
defmodule PgflowDemo.Jobs.SendEmail do
  @moduledoc """
  One-off demo job. No mailer — returns a sent map so the homepage can
  teach `PgFlow.enqueue/2` and `use PgFlow.Job`.
  """
  use PgFlow.Job

  @job queue: :send_email, max_attempts: 3, timeout: 30

  perform :deliver do
    fn input, _ctx ->
      %{
        "sent" => true,
        "to" => input["to"],
        "subject" => input["subject"]
      }
    end
  end
end
```

No IO, no sleep. Canned enqueue input:

```elixir
%{
  "to" => "demo@pgflow.dev",
  "subject" => "Welcome to PgFlow",
  "body" => "This email was enqueued as a Job."
}
```

Register on `PgFlow.Supervisor` `jobs:` next to `ArticleFlowCleanup`. Compile with `mix pgflow.gen.job_migration PgflowDemo.Jobs.SendEmail`.

### Homepage

Tab order: **Article · Onboarding · Approval · Job · Cron**.

Selecting **Job** (and **Cron**) only assigns `selected_flow` — do **not** call `switch_flow/2` (that expects a flow DAG). Selecting a flow tab still uses `switch_flow/2` and resets run state.

Layout B, rendered only when `selected_flow == :job`:

1. Tab bar + **Start Job** + status pill (Ready / Running / Completed / Failed) and elapsed duration.
2. **Job DSL** — compile-time Makeup highlight of `send_email.ex`.
3. **Output** — empty until the run is completed; then `run.output`.
4. Footer line: link to `/pgflow/jobs/send_email` (same idea as Cron's dashboard link).

Hide on this tab: interactive tip, workflow SVG, event log, Flow DSL, step-output panel, article/onboarding/approval forms.

Start: `Client.enqueue(PgflowDemo.Jobs.SendEmail, canned_input)`, then the existing PubSub subscribe on `"pgflow:run:#{run_id}"` plus `reconcile_run_state/2`. Hide Start while `run_status == :running`. Show **Reset** when completed or failed (same as flow tabs).

Output is driven by `run_completed` / reconcile snapshot (`run.output`), not by clicking a DAG node.

**DSL:** new `PgflowDemoWeb.Components.JobDSL` for `send_email.ex`. Do not add this job to `FlowDSL`. Do not reuse `CronDSL` (that component also shows next-run / retention).

Reset / tab-switch reuse the current unsubscribe path. A finished or in-flight job row is not deleted by the UI.

## Data flow

1. Start Job → `Client.enqueue(SendEmail, canned_input)` → `{:ok, run_id}` → subscribe `"pgflow:run:<run_id>"` → `reconcile_run_state/2`.
2. Worker runs `deliver` (no IO) → `task_started` / `task_completed` / `run_completed` with output `%{"sent" => true, "to" => ..., "subject" => ...}`.
3. UI: status Completed, Output shows that map, Start replaced by Reset.
4. Instant complete before subscribe: `reconcile_run_state/2` already covers this (same as flows).

## Error handling

| Case | Behavior |
|---|---|
| Enqueue fails | Status stays Ready; red banner via existing `format_user_error/1`. |
| Job raises | `run_failed` → Failed pill + banner; Output stays empty. |
| Double-click Start while running | Start hidden while `run_status == :running`. |
| Switch to Cron mid-run | Assign `:cron` only; job assigns stay (hidden). Switching back to Job can still show them. |
| Switch to a flow tab mid-run | `switch_flow/2` resets and unsubscribes. Row remains in Postgres. |
| Instant complete | Reconcile snapshot; UI must not stick on Running. |

No retry UI.

## Testing

Demo LiveView (`demo/test/pgflow_demo_web/live/flow_demo_live_test.exs`, synthetic `{:pgflow, run_id, event}`):

- Default Article tab has `#tab-job` and does not render SendEmail DSL / Job Output.
- Job tab shows Start Job + Job DSL, hides workflow / flow DSL / article form.
- After `{:run_completed, %{output: %{"sent" => true, ...}}}`, Output includes `sent`.
- Cron tab still has no Start Job.
- Switching Job → Article restores the flow UI and hides Job DSL.

Do not boot `Worker.Server` inside demo tests. Do not add dashboard tests. Do not add library enqueue tests (already covered).

## File map

| File | Change |
|---|---|
| `demo/lib/pgflow_demo/jobs/send_email.ex` | New job |
| `demo/lib/pgflow_demo/application.ex` | Register job |
| `demo/priv/repo/migrations/*_compile_send_email.exs` | Generated compile migration |
| `demo/lib/pgflow_demo_web/components/job_dsl.ex` | Highlighted source |
| `demo/lib/pgflow_demo_web/live/flow_demo_live.ex` | Tab, enqueue, layout B |
| `demo/test/pgflow_demo_web/live/flow_demo_live_test.exs` | Tab + output tests |
| `demo/README.md` | Job tab one-liner |

## Open questions resolved

| Topic | Decision |
|---|---|
| Teaching beat | Runnable one-off job (`enqueue` + `perform`), Cron stays read-only schedule |
| Story | Fake send-email, no mailer |
| Visitor input | Canned payload, Start Job only |
| Layout | B — no DAG; Start, status, Job DSL, Output |
| Tab | **Job**, before Cron |
| Build | Extend `FlowDemoLive`; no new route; no tab-mode refactor |
| Start API | `Client.enqueue/2` (not `start_flow/2` at the call site) |
| Library changes | None |
