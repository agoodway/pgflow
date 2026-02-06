/**
 * KeyboardShortcuts Hook
 *
 * Handles global keyboard shortcuts for dashboard navigation.
 * Attach to a container element with data-base-path attribute.
 *
 * Usage in LiveView template:
 *   <div id="keyboard-shortcuts" phx-hook="KeyboardShortcuts" data-base-path={@base_path}>
 *     ...
 *   </div>
 *
 * Shortcuts:
 *   - g o: Go to Overview
 *   - g w: Go to Workers
 *   - g f: Go to Flows
 *   - g j: Go to Jobs
 *   - g c: Go to Crons
 *   - g r: Go to Runs
 *   - d: Toggle dark mode
 *   - ? or K: Open shortcuts modal
 *   - Esc: Close modal
 */
export const KeyboardShortcuts = {
  mounted() {
    this.basePath = this.el.dataset.basePath || "/pgflow";
    this.keyBuffer = "";
    this.keyTimeout = null;

    this.handleKeydown = this.handleKeydown.bind(this);
    document.addEventListener("keydown", this.handleKeydown);
  },

  destroyed() {
    document.removeEventListener("keydown", this.handleKeydown);
  },

  handleKeydown(e) {
    // Skip if in input/textarea/select
    if (["INPUT", "TEXTAREA", "SELECT"].includes(e.target.tagName)) return;
    if (e.target.isContentEditable) return;

    const modal = document.getElementById("shortcuts-modal");
    const isModalOpen = modal && !modal.classList.contains("hidden");

    // Escape closes modal
    if (e.key === "Escape") {
      if (isModalOpen) {
        this.closeModal();
        e.preventDefault();
      }
      return;
    }

    // Don't process shortcuts if modal is open
    if (isModalOpen) return;

    // K (shift+k) or ? shows shortcuts modal
    if ((e.key === "K" && e.shiftKey) || e.key === "?") {
      this.openModal();
      e.preventDefault();
      return;
    }

    // d toggles dark mode
    if (e.key === "d" && !e.ctrlKey && !e.metaKey && !e.altKey) {
      const darkModeBtn = document.getElementById("dark-mode-toggle");
      if (darkModeBtn) darkModeBtn.click();
      e.preventDefault();
      return;
    }

    // Handle g + key navigation (vim-style)
    clearTimeout(this.keyTimeout);
    this.keyBuffer += e.key;

    this.keyTimeout = setTimeout(() => {
      this.keyBuffer = "";
    }, 500);

    const routes = {
      "go": "",
      "gw": "/workers",
      "gf": "/flows",
      "gj": "/jobs",
      "gc": "/crons",
      "gr": "/runs"
    };

    if (routes[this.keyBuffer] !== undefined) {
      window.location.href = this.basePath + routes[this.keyBuffer];
      this.keyBuffer = "";
      e.preventDefault();
    }

    // Clear buffer if it gets too long
    if (this.keyBuffer.length > 2) {
      this.keyBuffer = "";
    }
  },

  openModal() {
    const modal = document.getElementById("shortcuts-modal");
    if (modal) {
      modal.classList.remove("hidden");
      document.body.style.overflow = "hidden";
    }
  },

  closeModal() {
    const modal = document.getElementById("shortcuts-modal");
    if (modal) {
      modal.classList.add("hidden");
      document.body.style.overflow = "";
    }
  }
};
