export const CopyToClipboard = {
  mounted() {
    this.handleClick = async () => {
      if (this.el.hasAttribute("data-copying")) return

      const target = document.getElementById(this.el.dataset.copyTarget)
      const text = target?.innerText || ""

      if (!text) return

      this.el.setAttribute("data-copying", "")
      let timeoutId

      try {
        const timeout = new Promise((_resolve, reject) => {
          timeoutId = setTimeout(() => reject(new Error("Clipboard write timed out")), 1000)
        })

        await Promise.race([navigator.clipboard.writeText(text), timeout])
      } catch (_error) {
        const textarea = document.createElement("textarea")
        textarea.value = text
        textarea.style.position = "fixed"
        textarea.style.opacity = "0"
        document.body.appendChild(textarea)
        textarea.select()
        let copied = true

        try {
          copied = document.execCommand("copy")
        } catch (_fallbackError) {
          copied = false
        }

        textarea.remove()
        if (!copied) return
      } finally {
        clearTimeout(timeoutId)
        this.el.removeAttribute("data-copying")
      }

      this.el.querySelectorAll("[data-copy-default]").forEach((element) => {
        element.hidden = true
      })
      this.el.querySelectorAll("[data-copy-success]").forEach((element) => {
        element.hidden = false
      })
      this.el.querySelector("[data-copy-announcement]").textContent = "JSON copied to clipboard"
      this.el.setAttribute("aria-label", "JSON copied to clipboard")
      this.el.setAttribute("data-copied", "")
      clearTimeout(this.resetTimer)
      this.resetTimer = setTimeout(() => {
        this.el.querySelectorAll("[data-copy-default]").forEach((element) => {
          element.hidden = false
        })
        this.el.querySelectorAll("[data-copy-success]").forEach((element) => {
          element.hidden = true
        })
        this.el.querySelector("[data-copy-announcement]").textContent = ""
        this.el.setAttribute("aria-label", "Copy JSON to clipboard")
        this.el.removeAttribute("data-copied")
      }, 1500)
    }

    this.el.addEventListener("click", this.handleClick)
  },

  destroyed() {
    this.el.removeEventListener("click", this.handleClick)
    clearTimeout(this.resetTimer)
  },
}
