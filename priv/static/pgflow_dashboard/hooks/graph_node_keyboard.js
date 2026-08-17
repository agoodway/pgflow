export const GraphNodeKeyboard = {
  mounted() {
    this.handleKeydown = (event) => {
      const node = event.target.closest('[role="button"]')

      if (!node || !["Enter", " "].includes(event.key)) return

      event.preventDefault()

      if (event.repeat) {
        event.stopPropagation()
      }
    }

    this.el.addEventListener("keydown", this.handleKeydown)
  },

  destroyed() {
    this.el.removeEventListener("keydown", this.handleKeydown)
  },
}
