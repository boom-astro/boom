import { describe, expect, it } from "vitest"
import { formatLabel, formatPercent, rankedClasses } from "@/lib/applecider"

describe("rankedClasses", () => {
  it("orders classes most likely first", () => {
    const ranked = rankedClasses({ TDE: 0.1, "AGN-like": 0.7, "Ia-like SN": 0.2 })
    expect(ranked.map(([name]) => name)).toEqual(["AGN-like", "Ia-like SN", "TDE"])
  })

  it("drops non-numeric probabilities", () => {
    expect(rankedClasses({ TDE: Number.NaN, "AGN-like": 0.5 })).toEqual([["AGN-like", 0.5]])
  })
})

describe("formatLabel", () => {
  it("spaces out PascalCase family and domain names", () => {
    expect(formatLabel("StellarVariable")).toBe("Stellar Variable")
    expect(formatLabel("NuclearTransient")).toBe("Nuclear Transient")
    expect(formatLabel("Supernova")).toBe("Supernova")
  })

  it("leaves leaf class names alone", () => {
    for (const name of ["AGN-like", "Accreting WD Var", "Ia-like SN", "H-rich CCSN", "TDE"]) {
      expect(formatLabel(name)).toBe(name)
    }
  })
})

describe("formatPercent", () => {
  it("rounds to a whole percent", () => {
    expect(formatPercent(0.836)).toBe("84%")
    expect(formatPercent(0)).toBe("0%")
  })

  it("does not show a small nonzero probability as 0%", () => {
    expect(formatPercent(0.001)).toBe("<1%")
  })
})
