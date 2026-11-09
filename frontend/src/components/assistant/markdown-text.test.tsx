import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, fireEvent, render, screen } from "@testing-library/react"
import { MemoryRouter } from "react-router-dom"

const loadMarkdown = vi.hoisted(() => vi.fn())
vi.mock("@/components/assistant/load-markdown", () => ({ loadMarkdown }))

const actual = () =>
  vi.importActual<typeof import("@/components/assistant/assistant-markdown")>("@/components/assistant/assistant-markdown")
const chunkError = () => Promise.reject(new TypeError("Failed to fetch dynamically imported module"))
const NOTICE = "Formatting did not load, so answers show as plain text."

/** The module keeps its lazy component, so each test starts from a fresh copy. */
async function freshModule() {
  vi.resetModules()
  return import("@/components/assistant/markdown-text")
}

beforeEach(() => {
  loadMarkdown.mockReset()
  // React reports the errors its boundaries catch.
  vi.spyOn(console, "error").mockImplementation(() => {})
})

afterEach(() => {
  cleanup()
  vi.restoreAllMocks()
})

describe("AnswerText", () => {
  it("shows plain text and one retry when the markdown chunk does not load, then formats after a retry", async () => {
    loadMarkdown.mockImplementationOnce(chunkError).mockImplementation(actual)
    const { AnswerText, MarkdownRetry } = await freshModule()
    render(
      <MemoryRouter>
        <MarkdownRetry />
        <AnswerText text="It is **bright** now." />
        <AnswerText text="See ZTF21abcdefg." />
      </MemoryRouter>,
    )

    // The app is still there: both answers read as plain text, with one notice for both.
    expect(await screen.findByText(NOTICE)).toBeTruthy()
    expect(screen.getByText("It is **bright** now.")).toBeTruthy()
    expect(screen.getByText("See ZTF21abcdefg.")).toBeTruthy()
    expect(screen.getAllByRole("button", { name: "Retry" })).toHaveLength(1)

    fireEvent.click(screen.getByRole("button", { name: "Retry" }))

    expect((await screen.findByText("bright")).tagName).toBe("STRONG")
    expect(screen.getByRole("link", { name: "ZTF21abcdefg" })).toBeTruthy()
    expect(screen.queryByText(NOTICE)).toBeNull()
    expect(loadMarkdown).toHaveBeenCalledTimes(2)
  })

  it("offers a reload when trying again fails too", async () => {
    loadMarkdown.mockImplementation(chunkError)
    const { AnswerText, MarkdownRetry } = await freshModule()
    render(
      <MemoryRouter>
        <MarkdownRetry />
        <AnswerText text="Plain **text**" />
      </MemoryRouter>,
    )

    fireEvent.click(await screen.findByRole("button", { name: "Retry" }))

    expect(await screen.findByRole("button", { name: "Reload page" })).toBeTruthy()
    expect(screen.getByText("Plain **text**")).toBeTruthy()
    expect(loadMarkdown).toHaveBeenCalledTimes(2)
  })
})
