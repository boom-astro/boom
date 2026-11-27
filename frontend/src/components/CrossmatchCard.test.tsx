import { afterEach, describe, expect, it } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import CrossmatchCard from "@/components/CrossmatchCard"
import useAppStore from "@/lib/store"

afterEach(() => {
  cleanup()
  useAppStore.setState({ currentSource: null })
})

describe("CrossmatchCard", () => {
  it("shows a 64-bit catalog ID exactly as received", () => {
    useAppStore.setState({
      currentSource: {
        data: {
          cross_matches: {
            Gaia_DR3: [{ _id: "5853498713190525696", ra: 10, dec: 20, separation_arcsec: 0.1 }],
          },
        },
      },
    })
    render(<CrossmatchCard />)
    expect(screen.getByText("5853498713190525696")).toBeTruthy()
  })
})
