// If you want to use Phoenix channels, run `mix help phx.gen.channel`
// to get started and then uncomment the line below.
// import "./user_socket.ts"

// You can include dependencies in two ways.
//
// The simplest option is to put them in assets/vendor and
// import them using relative paths:
//
//     import "../vendor/some-package.js"
//
// Alternatively, you can `npm install some-package --prefix assets` and import
// them using a path starting with the package name:
//
//     import "some-package"
//
// If you have dependencies that try to import CSS, esbuild will generate a separate `app.css` file.
// To load it, simply add a second `<link>` to your `root.html.heex` file.

// Include phoenix_html to handle method=PUT/DELETE in forms and buttons.
import "phoenix_html"
// Establish Phoenix Socket and LiveView configuration.
import { Socket } from "phoenix"
import { LiveSocket } from "phoenix_live_view"
import { hooks as colocatedHooks } from "phoenix-colocated/pgflow_demo"
import topbar from "../vendor/topbar"

// Type declarations for Phoenix LiveView events
interface PhxScrollEvent extends CustomEvent {
  detail: { step: string }
}

interface LiveReloader {
  enableServerLogs(): void
  disableServerLogs(): void
  openEditorAtCaller(target: EventTarget | null): void
  openEditorAtDef(target: EventTarget | null): void
}

interface PhxLiveReloadEvent extends CustomEvent {
  detail: LiveReloader
}

// Extend Window interface for global objects
declare global {
  interface Window {
    liveSocket: LiveSocket
    liveReloader?: LiveReloader
  }
}

const csrfToken = document
  .querySelector("meta[name='csrf-token']")
  ?.getAttribute("content")

const liveSocket = new LiveSocket("/live", Socket, {
  longPollFallbackMs: 2500,
  params: { _csrf_token: csrfToken },
  hooks: { ...colocatedHooks },
})

// Show progress bar on live navigation and form submits
topbar.config({ barColors: { 0: "#29d" }, shadowColor: "rgba(0, 0, 0, .3)" })
window.addEventListener("phx:page-loading-start", (_info) => topbar.show(300))
window.addEventListener("phx:page-loading-stop", (_info) => topbar.hide())

// Scroll to DSL step when highlighted via DSL click or event log click
// This scrolls the entire page to bring the DSL section into view
window.addEventListener("phx:scroll_to_dsl_step", (e) => {
  const event = e as PhxScrollEvent
  const step = event.detail.step
  const element = document.getElementById(`dsl-segment-${step}`)
  if (element) {
    element.scrollIntoView({ behavior: "smooth", block: "center" })
  }
})

// Scroll within the DSL pane only (not the page) when clicking graph nodes
// This centers the step within the Flow DSL container without scrolling the page
window.addEventListener("phx:scroll_dsl_pane", (e) => {
  const event = e as PhxScrollEvent
  const step = event.detail.step
  const element = document.getElementById(`dsl-segment-${step}`)
  const container = document.getElementById("flow-dsl-container")
  if (element && container) {
    const elementRect = element.getBoundingClientRect()
    const containerRect = container.getBoundingClientRect()
    const scrollTop =
      element.offsetTop -
      container.offsetTop -
      container.clientHeight / 2 +
      elementRect.height / 2
    container.scrollTo({ top: scrollTop, behavior: "smooth" })
  }
})

// connect if there are any LiveViews on the page
liveSocket.connect()

// expose liveSocket on window for web console debug logs and latency simulation:
// >> liveSocket.enableDebug()
// >> liveSocket.enableLatencySim(1000)  // enabled for duration of browser session
// >> liveSocket.disableLatencySim()
window.liveSocket = liveSocket

// The lines below enable quality of life phoenix_live_reload
// development features:
//
//     1. stream server logs to the browser console
//     2. click on elements to jump to their definitions in your code editor
//
if (process.env.NODE_ENV === "development") {
  window.addEventListener("phx:live_reload:attached", (e) => {
    const { detail: reloader } = e as PhxLiveReloadEvent
    // Enable server log streaming to client.
    // Disable with reloader.disableServerLogs()
    reloader.enableServerLogs()

    // Open configured PLUG_EDITOR at file:line of the clicked element's HEEx component
    //
    //   * click with "c" key pressed to open at caller location
    //   * click with "d" key pressed to open at function component definition location
    let keyDown: string | null = null
    window.addEventListener("keydown", (e) => (keyDown = e.key))
    window.addEventListener("keyup", (_e) => (keyDown = null))
    window.addEventListener(
      "click",
      (e) => {
        if (keyDown === "c") {
          e.preventDefault()
          e.stopImmediatePropagation()
          reloader.openEditorAtCaller(e.target)
        } else if (keyDown === "d") {
          e.preventDefault()
          e.stopImmediatePropagation()
          reloader.openEditorAtDef(e.target)
        }
      },
      true
    )

    window.liveReloader = reloader
  })
}
