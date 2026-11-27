import { afterEach, beforeAll, describe, expect, it, vi } from "vitest"
import { cleanup, render } from "@testing-library/react"
import Lightcurve from "@/components/Lightcurve"

vi.mock("@/lib/api", () => ({ fetchVillarFit: () => new Promise(() => {}) }))

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

afterEach(cleanup)

describe("Lightcurve", () => {
  it("clips each plot to its own plot area", () => {
    const data = {
      objectId: "ZTF26aaaaaaa",
      prv_candidates: [{ jd: 2460000.5, magpsf: 18, sigmapsf: 0.05, band: "r" }],
    }
    const { container } = render(
      <>
        <Lightcurve data={data} />
        <Lightcurve data={data} />
      </>,
    )

    const ids = [...container.querySelectorAll("clipPath")].map((c) => c.id)
    expect(ids).toHaveLength(2)
    expect(new Set(ids).size).toBe(2)
    for (const g of container.querySelectorAll("g[clip-path]")) {
      const id = g.getAttribute("clip-path")?.match(/^url\(#(.+)\)$/)?.[1]
      expect(g.closest("svg")?.querySelector(`clipPath[id="${id}"]`)).not.toBeNull()
    }
  })
})
