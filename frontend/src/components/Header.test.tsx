import { afterEach, describe, expect, it } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import { ClassificationBadges } from "@/components/Header"

afterEach(cleanup)

describe("ClassificationBadges", () => {
  it("flags a likely supernova on BTSbot alone", () => {
    render(<ClassificationBadges data={{ classifications: { btsbot: 0.9 } }} />)
    expect(screen.queryByText("SN?")).not.toBeNull()
  })

  it("flags a likely supernova on a hosted, non-stellar, young ACAI source", () => {
    render(<ClassificationBadges data={{ classifications: { acai_h: 0.9 }, candidate: { ndethist: 3 } }} />)
    expect(screen.queryByText("SN?")).not.toBeNull()
  })

  it("does not flag a source neither classifier is confident about", () => {
    render(<ClassificationBadges data={{ classifications: { btsbot: 0.2, acai_h: 0.2 } }} />)
    expect(screen.queryByText("SN?")).toBeNull()
  })
})
