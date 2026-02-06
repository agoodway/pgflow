/**
 * PgFlow Dashboard Hooks
 *
 * LiveView hooks for the PgFlow Dashboard. Import and register these hooks
 * with your LiveSocket to enable dashboard functionality.
 *
 * ## Installation
 *
 * In your app.js:
 *
 *   import { DarkMode, KeyboardShortcuts, ShortcutsModal, MobileMenu } from "pgflow_dashboard/hooks"
 *
 *   let liveSocket = new LiveSocket("/live", Socket, {
 *     hooks: { DarkMode, KeyboardShortcuts, ShortcutsModal, MobileMenu, ...YourOtherHooks }
 *   })
 *
 * Or copy the individual hook files from deps/pgflow/priv/static/pgflow_dashboard/hooks/
 * to your assets directory.
 */

export { DarkMode } from "./dark_mode.js";
export { KeyboardShortcuts } from "./keyboard_shortcuts.js";
export { ShortcutsModal } from "./shortcuts_modal.js";
export { MobileMenu } from "./mobile_menu.js";
