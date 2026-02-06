/**
 * MobileMenu Hook
 *
 * Manages mobile navigation menu with body scroll locking and accessibility features.
 * Uses CSS classes controlled by Phoenix.LiveView.JS for animations.
 *
 * Usage in LiveView template:
 *   <div id="mobile-menu" phx-hook="MobileMenu" class="...">...</div>
 *
 * Expected DOM structure:
 *   - Element with id="mobile-menu-backdrop" for the overlay
 *   - Element with id="mobile-menu-panel" for the slide-out panel
 *   - Close button(s) with data-close-menu attribute
 */

interface HookContext {
  el: HTMLElement;
  pushEvent: (event: string, payload: object) => void;
  handleEvent: (event: string, callback: (payload: object) => void) => void;
}

interface MobileMenuHook extends HookContext {
  panel: HTMLElement | null;
  backdrop: HTMLElement | null;
  focusableElements: HTMLElement[];
  previousActiveElement: Element | null;
  isOpen: boolean;
  boundHandleKeydown: (e: KeyboardEvent) => void;
  boundHandleResize: () => void;

  mounted(): void;
  destroyed(): void;
  open(): void;
  close(): void;
  handleKeydown(e: KeyboardEvent): void;
  handleResize(): void;
  trapFocus(e: KeyboardEvent): void;
  getFocusableElements(): HTMLElement[];
  lockBodyScroll(): void;
  unlockBodyScroll(): void;
}

export const MobileMenu = {
  mounted(this: MobileMenuHook) {
    this.panel = document.getElementById("mobile-menu-panel");
    this.backdrop = document.getElementById("mobile-menu-backdrop");
    this.focusableElements = [];
    this.previousActiveElement = null;
    this.isOpen = false;

    // Bind event handlers
    this.boundHandleKeydown = this.handleKeydown.bind(this);
    this.boundHandleResize = this.handleResize.bind(this);

    // Listen for custom events from LiveView.JS
    this.el.addEventListener("menu:opened", () => this.open());
    this.el.addEventListener("menu:closed", () => this.close());

    // Close on resize to larger viewport
    window.addEventListener("resize", this.boundHandleResize);
  },

  destroyed(this: MobileMenuHook) {
    window.removeEventListener("resize", this.boundHandleResize);
    document.removeEventListener("keydown", this.boundHandleKeydown);
    this.unlockBodyScroll();
  },

  open(this: MobileMenuHook) {
    if (this.isOpen) return;
    this.isOpen = true;

    // Store current focus
    this.previousActiveElement = document.activeElement;

    // Lock body scroll
    this.lockBodyScroll();

    // Get focusable elements for focus trap
    this.focusableElements = this.getFocusableElements();

    // Focus first element
    if (this.focusableElements.length > 0) {
      setTimeout(() => {
        this.focusableElements[0]?.focus();
      }, 100);
    }

    // Add keyboard listener for escape and focus trap
    document.addEventListener("keydown", this.boundHandleKeydown);
  },

  close(this: MobileMenuHook) {
    if (!this.isOpen) return;
    this.isOpen = false;

    // Unlock body scroll
    this.unlockBodyScroll();

    // Remove keyboard listener
    document.removeEventListener("keydown", this.boundHandleKeydown);

    // Restore focus
    if (this.previousActiveElement instanceof HTMLElement) {
      this.previousActiveElement.focus();
    }
  },

  handleKeydown(this: MobileMenuHook, e: KeyboardEvent) {
    if (e.key === "Escape") {
      // Trigger close via JS command on backdrop click
      this.backdrop?.click();
      return;
    }

    if (e.key === "Tab") {
      this.trapFocus(e);
    }
  },

  handleResize(this: MobileMenuHook) {
    // Close menu if viewport becomes larger than sm breakpoint (640px)
    if (window.innerWidth >= 640 && this.isOpen) {
      this.backdrop?.click();
    }
  },

  trapFocus(this: MobileMenuHook, e: KeyboardEvent) {
    const firstElement = this.focusableElements[0];
    const lastElement = this.focusableElements[this.focusableElements.length - 1];

    if (e.shiftKey) {
      // Shift + Tab
      if (document.activeElement === firstElement) {
        e.preventDefault();
        lastElement?.focus();
      }
    } else {
      // Tab
      if (document.activeElement === lastElement) {
        e.preventDefault();
        firstElement?.focus();
      }
    }
  },

  getFocusableElements(this: MobileMenuHook): HTMLElement[] {
    if (!this.panel) return [];

    const selector = [
      'a[href]',
      'button:not([disabled])',
      'input:not([disabled])',
      'select:not([disabled])',
      'textarea:not([disabled])',
      '[tabindex]:not([tabindex="-1"])',
    ].join(', ');

    return Array.from(this.panel.querySelectorAll<HTMLElement>(selector));
  },

  lockBodyScroll(this: MobileMenuHook) {
    // Save current scroll position
    const scrollY = window.scrollY;
    document.body.style.position = 'fixed';
    document.body.style.top = `-${scrollY}px`;
    document.body.style.left = '0';
    document.body.style.right = '0';
    document.body.style.overflow = 'hidden';
  },

  unlockBodyScroll(this: MobileMenuHook) {
    const scrollY = document.body.style.top;
    document.body.style.position = '';
    document.body.style.top = '';
    document.body.style.left = '';
    document.body.style.right = '';
    document.body.style.overflow = '';
    window.scrollTo(0, parseInt(scrollY || '0') * -1);
  },
} as unknown as MobileMenuHook;
