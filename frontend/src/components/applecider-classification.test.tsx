import { afterEach, describe, expect, it } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import { ClassProbabilities, Decision } from "@/components/applecider-classification"
import type { AppleCiderFusion } from "@/lib/api"

afterEach(cleanup)

function fusion(overrides: Partial<AppleCiderFusion> = {}): AppleCiderFusion {
  return {
    decision_label: "Ia-like SN",
    decision_level: "class",
    decision_probability: 0.84,
    abstain_completely: false,
    class_probabilities: { "Ia-like SN": 0.84, TDE: 0.16 },
    ...overrides,
  }
}

describe("Decision", () => {
  it("marks a family-level label as coarser and spaces its name", () => {
    render(<Decision fusion={fusion({ decision_label: "StellarVariable", decision_level: "family" })} />)
    expect(screen.getByText("Stellar Variable")).toBeTruthy()
    expect(screen.getByText("(family)")).toBeTruthy()
  })

  it("says Abstained when every level abstained", () => {
    render(<Decision fusion={fusion({ abstain_completely: true, decision_label: undefined })} />)
    expect(screen.getByText("Abstained")).toBeTruthy()
  })

  it("says Not classified for an alert with no classification", () => {
    render(<Decision fusion={null} />)
    expect(screen.getByText("Not classified")).toBeTruthy()
  })
})

describe("ClassProbabilities", () => {
  it("shows the three most likely classes and folds the rest away", () => {
    const probs = { TDE: 0.02, "AGN-like": 0.5, "Ia-like SN": 0.3, "H-rich CCSN": 0.1, SLSN: 0.08 }
    const { container } = render(<ClassProbabilities fusion={fusion({ class_probabilities: probs })} />)
    const names = (root: Element) =>
      Array.from(root.querySelectorAll(":scope > div span.truncate")).map((el) => el.textContent)

    expect(names(container.firstElementChild!)).toEqual(["AGN-like", "Ia-like SN", "H-rich CCSN"])
    expect(screen.getByText("2 more").tagName).toBe("SUMMARY")
    expect(names(container.querySelector("details > div")!)).toEqual(["SLSN", "TDE"])
  })
})
