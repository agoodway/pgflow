/**
 * DarkMode Hook
 *
 * Manages dark mode toggle with localStorage persistence and system preference detection.
 * Attach to a button element that will toggle between light and dark modes.
 *
 * Usage in LiveView template:
 *   <button id="dark-mode-toggle" phx-hook="DarkMode">...</button>
 */
export const DarkMode = {
  mounted() {
    this.loadTheme();
    this.el.addEventListener("click", () => this.toggle());
  },

  loadTheme() {
    const saved = localStorage.getItem("pgflow-theme");
    const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
    const theme = saved || (prefersDark ? "dark" : "light");
    this.setTheme(theme);
  },

  toggle() {
    const current = document.documentElement.getAttribute("data-theme");
    const next = current === "dark" ? "light" : "dark";
    this.setTheme(next);
    localStorage.setItem("pgflow-theme", next);
  },

  setTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    this.el.title = theme === "dark" ? "Switch to light mode" : "Switch to dark mode";
  }
};
