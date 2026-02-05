/**
 * ShortcutsModal Hook
 *
 * Manages the keyboard shortcuts help modal.
 * Listens for clicks on the overlay to close the modal.
 *
 * Usage in LiveView template:
 *   <div id="shortcuts-modal" phx-hook="ShortcutsModal" class="hidden">
 *     ...
 *   </div>
 */
export const ShortcutsModal = {
  mounted() {
    // Handle clicks on the backdrop (overlay)
    this.handleOverlayClick = this.handleOverlayClick.bind(this);
    this.el.addEventListener("click", this.handleOverlayClick);
  },

  destroyed() {
    this.el.removeEventListener("click", this.handleOverlayClick);
  },

  handleOverlayClick(e) {
    // Only close if clicking directly on the modal container (the overlay area)
    // not on the modal content itself
    if (e.target === this.el || e.target.classList.contains("fixed")) {
      this.close();
    }
  },

  open() {
    this.el.classList.remove("hidden");
    document.body.style.overflow = "hidden";
  },

  close() {
    this.el.classList.add("hidden");
    document.body.style.overflow = "";
  }
};
