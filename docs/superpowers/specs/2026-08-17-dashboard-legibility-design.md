# Dashboard Legibility Design

## Scope

Improve the existing Run Detail experience without changing its overall information architecture:

- Make skipped state markers clearly distinct from neutral surfaces and pending state.
- Show complete workflow step labels at the existing text size.
- Reveal a node's state on hover and keyboard focus.
- Replace raw JSON `<pre>` blocks with a reusable, syntax-styled viewer and copy action.
- Correct the dashboard installation documentation for the current LiveFilter package.

## Design decisions

Skipped uses a saturated orange in light mode and bright amber in dark mode. Small marks also receive an outline so they remain visible against nearby timeline fills. The same semantic treatment is used by the status badge, progress bar, timeline, and dependency graph.

The dependency graph keeps its circular-node model, but uses an intrinsic canvas inside a horizontal scroll region. Labels are humanized without truncation, and horizontal level spacing is derived from the longest adjacent labels. This prevents the SVG from shrinking its text to fit the card. Interactive nodes expose a compact status tooltip on hover and focus, while retaining their accessible name and click-to-inspect behavior.

JSON is rendered server-side as semantic tokens. This adds useful color and indentation without a client-side highlighter or new dependency. The viewer uses a consistent dark code surface, safely escaped HEEx output, a clear empty value, and a copy button backed by a small packaged LiveView hook.

LiveFilter remains at its current latest release, 0.2.0. The demo's runtime configuration is already correct; only the public installation documentation needs correction to use the package's real dependency, import, and Tailwind scan paths.

## Constraints

- Keep `GanttTimeline.step_bar/1` as the shared bar component.
- No data-layer or SQL changes.
- No mockups, new UI dependency, or JavaScript syntax-highlighting library.
- Do not commit or push any changes without explicit permission.
