import { afterEach, beforeAll, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import { MemoryRouter } from "react-router-dom"
import { IconShieldLock } from "@tabler/icons-react"
import { NavMain } from "@/components/nav-main"
import { SidebarProvider } from "@/components/ui/sidebar"

/**
 * The badge on a nav item has to say what needs attention, to somebody reading
 * the screen and to somebody hearing it.
 *
 * The visible text is a count capped at "9+", which on its own says nothing
 * about what is wrong, so the reason has to travel separately. It used to
 * travel as an `aria-label` on a plain `span` — where ARIA prohibits it, the
 * implicit role being `generic` — so it was dropped, and the link named itself
 * from its text content instead: the reason first and the title of the thing
 * the reason was about second.
 *
 * These assert the structure the accessible name is computed from, since the
 * name is name-from-content and document order is therefore read order.
 */
beforeAll(() => {
  // The sidebar reads the viewport on mount and jsdom has no matchMedia.
  vi.stubGlobal(
    "matchMedia",
    (query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: () => {},
      removeEventListener: () => {},
      addListener: () => {},
      removeListener: () => {},
      dispatchEvent: () => false,
    }),
  )
})

function renderNav(items: Parameters<typeof NavMain>[0]["items"]) {
  return render(
    <MemoryRouter>
      <SidebarProvider>
        <NavMain items={items} />
      </SidebarProvider>
    </MemoryRouter>,
  )
}

/** The dot: excluded from the name, so it carries `aria-hidden`. */
function dot() {
  return screen.getByRole("link").querySelector("[aria-hidden='true']")
}

function readsInOrder(first: Element, second: Element) {
  return !!(
    first.compareDocumentPosition(second) & Node.DOCUMENT_POSITION_FOLLOWING
  )
}

const admin = { title: "Admin", url: "/admin", icon: IconShieldLock }

afterEach(cleanup)

describe("NavMain", () => {
  it("names the item before the reason for its badge", () => {
    renderNav([{ ...admin, badge: 2, badgeLabel: "Catalogs: 2 missing" }])

    const title = screen.getByText("Admin")
    const reason = screen.getByText("Catalogs: 2 missing")
    expect(readsInOrder(title, reason)).toBe(true)
    // The count is not part of what gets read: it is capped and contextless.
    expect(dot()?.textContent).toBe("2")
  })

  it("caps the visible count but not the reason", () => {
    renderNav([{ ...admin, badge: 14, badgeLabel: "Catalogs: 14 missing" }])

    expect(dot()?.textContent).toBe("9+")
    expect(screen.getByText("Catalogs: 14 missing")).not.toBeNull()
  })

  it("falls back to the title and count, including for an empty label", () => {
    // `??` let an empty string through as the reason, leaving the badge with
    // nothing at all to say.
    renderNav([{ ...admin, badge: 3, badgeLabel: "" }])

    expect(screen.getByText("Admin: 3")).not.toBeNull()
  })

  it("renders a badge on an item with no icon", () => {
    // The dot was nested inside the icon's own condition, so an item without
    // one silently had no badge.
    renderNav([{ title: "Admin", url: "/admin", badge: 2, badgeLabel: "two" }])

    expect(dot()?.textContent).toBe("2")
    expect(readsInOrder(screen.getByText("Admin"), screen.getByText("two"))).toBe(
      true,
    )
  })

  it("renders nothing extra without a count", () => {
    renderNav([{ ...admin, badge: 0, badgeLabel: "Every catalog is present" }])

    expect(dot()).toBeNull()
    expect(screen.getByRole("link").textContent).toBe("Admin")
  })
})
