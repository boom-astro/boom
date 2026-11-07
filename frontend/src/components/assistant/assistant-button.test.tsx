import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react"
import { MemoryRouter } from "react-router-dom"
import { AssistantButton } from "@/components/assistant/assistant-button"
import { useAssistantStore } from "@/components/assistant/store"
import { TOKEN_KEY } from "@/lib/api"
import useAppStore from "@/lib/store"

const PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="

function sse(...events: object[]) {
  return new TextEncoder().encode(events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join(""))
}

/** The assistant is up, and answers each question with `chat()`. */
function stubAssistant(chat: () => Response) {
  const fetchMock = vi.fn(async (url: string) => {
    if (url.endsWith("/health")) {
      return new Response(JSON.stringify({ status: "ok", model: "test", vision: false }), { status: 200 })
    }
    return chat()
  })
  vi.stubGlobal("fetch", fetchMock)
  return fetchMock
}

function streamOf(...chunks: Uint8Array[]) {
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(chunk)
        controller.close()
      },
    }),
    { status: 200, headers: { "Content-Type": "text/event-stream" } },
  )
}

async function openPanel(path = "/query") {
  render(
    <MemoryRouter initialEntries={[path]}>
      <AssistantButton />
    </MemoryRouter>,
  )
  fireEvent.click(await screen.findByRole("button", { name: "Ask Babamul" }))
  return screen.findByRole("dialog", { name: "Ask Babamul" })
}

beforeEach(() => {
  localStorage.setItem(
    TOKEN_KEY,
    JSON.stringify({ access_token: "test-token", token_type: "Bearer", expires_at: Date.now() + 3_600_000 }),
  )
  useAppStore.setState({ profile: { id: "u1", username: "ada", email: "ada@example.org", created_at: 0 } })
  useAssistantStore.setState({ user: null, turns: [], running: false, status: null })
})

afterEach(() => {
  cleanup()
  localStorage.clear()
  sessionStorage.clear()
  vi.unstubAllGlobals()
})

describe("AssistantButton", () => {
  it("sends the object in view and shows the answer, tool call and image", async () => {
    const fetchMock = stubAssistant(() =>
      streamOf(
        sse(
          { type: "tool_call", id: "c1", name: "get_cutouts", arguments: { object_id: "ZTF21abcdefg" } },
          {
            type: "tool_result",
            id: "c1",
            name: "get_cutouts",
            ok: true,
            summary: "Science, template and difference",
            images: [{ mime_type: "image/png", data: PNG }],
          },
        ),
        sse({ type: "text", delta: "It is " }, { type: "text", delta: "ZTF21abcdefg, a supernova." }),
        sse({ type: "done", content: "It is ZTF21abcdefg, a supernova." }),
      ),
    )

    const panel = await openPanel("/objects/ZTF/ZTF21abcdefg")
    expect(within(panel).getByText("Viewing ZTF21abcdefg")).toBeTruthy()
    fireEvent.click(within(panel).getByRole("button", { name: "Summarize this object" }))

    const link = await within(panel).findByRole("link", { name: "ZTF21abcdefg" })
    expect(link.getAttribute("href")).toBe("/objects/ZTF/ZTF21abcdefg")

    const chip = within(panel).getByRole("button", { name: /^get_cutouts/ })
    expect(chip.getAttribute("aria-expanded")).toBe("false")
    fireEvent.click(chip)
    expect(within(panel).getByText("Science, template and difference")).toBeTruthy()
    expect(within(panel).getByRole("img", { name: "get_cutouts image 1" }).getAttribute("src")).toBe(
      `data:image/png;base64,${PNG}`,
    )

    const [url, init] = fetchMock.mock.calls.find(([u]) => u.endsWith("/chat")) as unknown as [string, RequestInit]
    expect(url).toBe("/api/assistant/chat")
    expect(JSON.parse(String(init.body))).toEqual({
      messages: [{ role: "user", content: "Summarize this object" }],
      context: { path: "/objects/ZTF/ZTF21abcdefg", survey: "ZTF", object_id: "ZTF21abcdefg" },
    })
    expect(new Headers(init.headers).get("Authorization")).toBe("Bearer test-token")
  })

  it("sends no context once the object chip is dismissed", async () => {
    const fetchMock = stubAssistant(() => streamOf(sse({ type: "done", content: "Hello." })))

    const panel = await openPanel("/objects/ZTF/ZTF21abcdefg")
    fireEvent.click(within(panel).getByRole("button", { name: "Don't send ZTF21abcdefg with questions" }))
    expect(within(panel).queryByText("Viewing ZTF21abcdefg")).toBeNull()
    fireEvent.change(within(panel).getByRole("textbox", { name: "Question" }), { target: { value: "hello" } })
    fireEvent.click(within(panel).getByRole("button", { name: "Send" }))

    await within(panel).findByText("Hello.")
    const [, init] = fetchMock.mock.calls.find(([u]) => u.endsWith("/chat")) as unknown as [string, RequestInit]
    expect(JSON.parse(String(init.body))).toEqual({ messages: [{ role: "user", content: "hello" }] })
  })

  it("asks the user to log in again after a 401", async () => {
    stubAssistant(() => new Response(JSON.stringify({ error: "Invalid token" }), { status: 401 }))

    const panel = await openPanel()
    fireEvent.change(within(panel).getByRole("textbox", { name: "Question" }), { target: { value: "hello" } })
    fireEvent.click(within(panel).getByRole("button", { name: "Send" }))

    expect((await within(panel).findByRole("alert")).textContent).toContain("Log in again")
    expect(within(panel).getByRole("link", { name: "Log in" }).getAttribute("href")).toBe("/login")
  })

  it("offers a retry when the assistant is busy", async () => {
    stubAssistant(() => new Response(JSON.stringify({ error: "You already have a question running." }), { status: 429 }))

    const panel = await openPanel()
    const box = within(panel).getByRole("textbox", { name: "Question" })
    fireEvent.change(box, { target: { value: "hello" } })
    fireEvent.keyDown(box, { key: "Enter" })

    expect((await within(panel).findByRole("alert")).textContent).toContain("You already have a question running.")
    expect(within(panel).getByRole("button", { name: "Try again" })).toBeTruthy()
  })
})
