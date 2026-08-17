# Dashboard Legibility Implementation Plan

> Do not commit or push this work without explicit user permission.

## Task 1: Lock the visual and workflow behavior in tests

**Files:** component tests under `test/pgflow_dashboard/components/`

1. Add failing assertions for the stronger skipped palette across timeline, progress, badge, and graph.
2. Add failing graph assertions for full labels, an intrinsic scrollable canvas, and hover/focus status text.
3. Add failing JSON viewer tests for semantic tokens, safe escaping, empty values, and copy wiring.
4. Run the focused tests and confirm the failures describe missing behavior.

## Task 2: Implement the reusable components

**Files:** dashboard components and packaged hooks under `lib/pgflow_dashboard/components/` and `priv/static/pgflow_dashboard/hooks/`

1. Apply the shared skipped palette without changing Gantt bar structure.
2. Make dependency graph spacing content-aware and add a non-scaling scroll region and status tooltip.
3. Add the server-rendered JSON viewer and copy hook.
4. Run focused component tests until green.

## Task 3: Integrate Run Detail and demo assets

**Files:** `lib/pgflow_dashboard/live/runs_live/show.ex`, hook index, and `demo/assets/js/app.ts`

1. Replace all Run Detail JSON `<pre>` blocks with the viewer.
2. Register the copy hook in the packaged export and demo LiveSocket.
3. Build demo assets and run Run Detail tests.

## Task 4: Correct LiveFilter documentation

**File:** `docs/DASHBOARD.md`

1. Document `{:livefilter, "~> 0.2.0"}`.
2. Use the packaged JavaScript import path and correct Tailwind source paths.
3. Confirm no stale `live_filter` dependency paths remain.

## Task 5: Verify the finished change

1. Format the root project and run focused dashboard tests.
2. Restart the demo only because packaged dependency assets changed.
3. Verify Run Detail node selection, copy behavior, full labels, and accessibility in the live browser.
4. Run root dashboard tests, migration tests, the full root suite, demo asset build, and `mix quality` sequentially.
5. Inspect the final diff and report results without committing.
