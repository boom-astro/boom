import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react"
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
function stubAssistant(chat: (init: RequestInit) => Response) {
  const fetchMock = vi.fn(async (url: string, init: RequestInit) => {
    if (url.endsWith("/health")) {
      return new Response(JSON.stringify({ status: "ok", model: "test", vision: false }), { status: 200 })
    }
    return chat(init)
  })
  vi.stubGlobal("fetch", fetchMock)
  return fetchMock
}

type LiveStream = {
  signal: AbortSignal
  send: (...events: object[]) => Promise<void>
  fail: (err: unknown) => Promise<void>
}

/** Answers with streams the test feeds by hand; they stay open until it says otherwise. */
function stubLiveAssistant() {
  const streams: LiveStream[] = []
  const fetchMock = stubAssistant((init) => {
    let controller!: ReadableStreamDefaultController<Uint8Array>
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        controller = c
        // Like fetch, error the body when the request is aborted.
        init.signal?.addEventListener("abort", () => c.error(new DOMException("Aborted", "AbortError")))
      },
    })
    streams.push({
      signal: init.signal as AbortSignal,
      send: (...events) => act(async () => controller.enqueue(sse(...events))),
      fail: (err) => act(async () => controller.error(err)),
    })
    return new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } })
  })
  return { fetchMock, streams }
}

async function ask(panel: HTMLElement, question: string) {
  fireEvent.change(within(panel).getByRole("textbox", { name: "Question" }), { target: { value: question } })
  fireEvent.click(within(panel).getByRole("button", { name: "Send" }))
}

const SAVED_KEY = "babamul-assistant:u1"

function saved() {
  return JSON.parse(sessionStorage.getItem(SAVED_KEY) ?? "null")
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
  // Ends a stream a test left open, so it cannot reach the next test.
  useAssistantStore.getState().reset()
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

  it("shows the server's place in line until the answer starts", async () => {
    const { streams } = stubLiveAssistant()
    const panel = await openPanel()
    await ask(panel, "hello")
    await waitFor(() => expect(streams).toHaveLength(1))

    await streams[0].send({ type: "status", message: "Waiting for the model, 3 questions ahead of you" })
    expect(within(panel).getByRole("status").textContent).toBe("Waiting for the model, 3 questions ahead of you")
    await streams[0].send({ type: "status", message: "Waiting for the model, 1 question ahead of you" })
    expect(within(panel).getByRole("status").textContent).toBe("Waiting for the model, 1 question ahead of you")

    await streams[0].send({ type: "text", delta: "Hello there" })
    await within(panel).findByText("Hello there")
    expect(within(panel).queryByText(/questions? ahead of you/)).toBeNull()
    expect(within(panel).getByRole("status").textContent).toBe("Writing")
  })

  it("stops a running answer, keeps what came so far and lets the user ask again", async () => {
    const { streams } = stubLiveAssistant()
    const panel = await openPanel()
    await ask(panel, "hello")
    await waitFor(() => expect(streams).toHaveLength(1))
    await streams[0].send({ type: "text", delta: "Partial answer" })
    await within(panel).findByText("Partial answer")

    fireEvent.click(within(panel).getByRole("button", { name: "Stop" }))

    expect(streams[0].signal.aborted).toBe(true)
    expect(await within(panel).findByText("Stopped.")).toBeTruthy()
    expect(within(panel).getByText("Partial answer")).toBeTruthy()
    await waitFor(() => expect(useAssistantStore.getState().running).toBe(false))
    expect(within(panel).getByRole("button", { name: "Send" })).toBeTruthy()
    expect(saved()[1]).toMatchObject({ state: "stopped", parts: [{ kind: "text", text: "Partial answer" }] })
  })

  it("starts a new chat while an answer runs, and ignores what that answer still sends", async () => {
    const { streams } = stubLiveAssistant()
    const panel = await openPanel()
    await ask(panel, "first question")
    await waitFor(() => expect(streams).toHaveLength(1))
    await streams[0].send({ type: "text", delta: "Old answer" })
    await within(panel).findByText("Old answer")
    expect(saved()).toHaveLength(2)

    fireEvent.click(within(panel).getByRole("button", { name: "New chat" }))

    expect(streams[0].signal.aborted).toBe(true)
    expect(within(panel).queryByText("first question")).toBeNull()
    expect(within(panel).getByRole("button", { name: "What can you look up for me?" })).toBeTruthy()
    expect(sessionStorage.getItem(SAVED_KEY)).toBeNull()
    expect(useAssistantStore.getState().running).toBe(false)

    await ask(panel, "second question")
    await waitFor(() => expect(streams).toHaveLength(2))
    await streams[1].send({ type: "done", content: "New answer" })
    await within(panel).findByText("New answer")
    expect(within(panel).queryByText("Old answer")).toBeNull()
    expect(saved().map((t: { id: string; role: string }) => t.role)).toEqual(["user", "assistant"])
  })

  it("says the answer was cut off when the network drops mid-stream", async () => {
    const { streams } = stubLiveAssistant()
    const panel = await openPanel()
    await ask(panel, "hello")
    await waitFor(() => expect(streams).toHaveLength(1))
    await streams[0].send({ type: "text", delta: "Half an answer" })
    await within(panel).findByText("Half an answer")

    await streams[0].fail(new TypeError("network error"))

    expect((await within(panel).findByRole("alert")).textContent).toContain("The answer was cut off. Try again.")
    expect(within(panel).getByText("Half an answer")).toBeTruthy()
    expect(within(panel).getByRole("button", { name: "Try again" })).toBeTruthy()
  })

  it("shows the service's own message when it fails before answering", async () => {
    stubAssistant(
      () =>
        new Response(JSON.stringify({ error: "The assistant is not set up correctly. Tell the Babamul team." }), {
          status: 503,
        }),
    )
    const panel = await openPanel()
    await ask(panel, "hello")
    expect((await within(panel).findByRole("alert")).textContent).toContain("Tell the Babamul team.")
  })

  it("restores the chat after a reload, with an answer cut off by it read as stopped", async () => {
    sessionStorage.setItem(
      SAVED_KEY,
      JSON.stringify([
        { id: "1", role: "user", content: "Earlier question" },
        { id: "2", role: "assistant", content: "", state: "running", parts: [{ kind: "text", text: "Earlier text" }] },
      ]),
    )
    stubAssistant(() => streamOf())
    const panel = await openPanel()
    expect(within(panel).getByText("Earlier question")).toBeTruthy()
    expect(within(panel).getByText("Earlier text")).toBeTruthy()
    expect(within(panel).getByText("Stopped.")).toBeTruthy()
    expect(within(panel).getByRole("button", { name: "Send" })).toBeTruthy()
  })

  it("drops the oldest images first when storage is full, and keeps the text", async () => {
    const older = "A".repeat(300_000)
    const newer = "B".repeat(300_000)
    const plot = (id: string, data: string) => [
      { type: "tool_call", id, name: "plot_light_curve", arguments: {} },
      { type: "tool_result", id, name: "plot_light_curve", ok: true, summary: "", images: [{ mime_type: "image/png", data }] },
    ]
    stubAssistant(() => streamOf(sse(...plot("c1", older), ...plot("c2", newer), { type: "done", content: "Two plots." })))
    const setItem = Storage.prototype.setItem
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (this: Storage, key: string, value: string) {
      // Room for one of the two plots.
      if (value.length > 400_000) throw new DOMException("The quota has been exceeded.", "QuotaExceededError")
      setItem.call(this, key, value)
    })

    const panel = await openPanel()
    await ask(panel, "Show me two plots")
    await within(panel).findByText("Two plots.")
    await waitFor(() => expect(saved()?.[1]?.state).toBe("done"))
    const parts = saved()[1].parts
    expect(parts.map((p: { step?: { images: { data: string }[] } }) => p.step?.images.map((i) => i.data[0]))).toEqual([
      [],
      ["B"],
      undefined,
    ])
    expect(parts[2]).toEqual({ kind: "text", text: "Two plots." })
  })

  it("says why an answer stopped when the user logs out while it runs", async () => {
    const { streams } = stubLiveAssistant()
    const profile = useAppStore.getState().profile
    const panel = await openPanel()
    await ask(panel, "hello")
    await waitFor(() => expect(streams).toHaveLength(1))
    await streams[0].send({ type: "text", delta: "Partial answer" })
    await within(panel).findByText("Partial answer")

    act(() => useAppStore.setState({ profile: null }))
    expect(streams[0].signal.aborted).toBe(true)
    expect(screen.queryByRole("button", { name: "Ask Babamul" })).toBeNull()
    expect(useAssistantStore.getState().turns).toEqual([])

    act(() => useAppStore.setState({ profile }))
    fireEvent.click(await screen.findByRole("button", { name: "Ask Babamul" }))
    const again = await screen.findByRole("dialog", { name: "Ask Babamul" })
    expect(within(again).getByText("Partial answer")).toBeTruthy()
    expect(within(again).getByRole("alert").textContent).toContain("Stopped because you logged out.")
  })

  it("lets an answer finish when the token expires during it; the next question asks to log in", async () => {
    const { streams } = stubLiveAssistant()
    const panel = await openPanel()
    await ask(panel, "first")
    await waitFor(() => expect(streams).toHaveLength(1))

    localStorage.setItem(
      TOKEN_KEY,
      JSON.stringify({ access_token: "test-token", token_type: "Bearer", expires_at: Date.now() - 1 }),
    )
    // The end of the answer re-renders the header button.
    await streams[0].send({ type: "done", content: "First answer." })
    await within(panel).findByText("First answer.")
    expect(streams[0].signal.aborted).toBe(false)
    await waitFor(() => expect(useAssistantStore.getState().running).toBe(false))
    expect(within(panel).getByText("First answer.")).toBeTruthy()

    await ask(panel, "second")
    expect((await within(panel).findByRole("alert")).textContent).toContain("Your session has expired.")
    expect(within(panel).getByText("First answer.")).toBeTruthy()
    expect(within(panel).getByRole("link", { name: "Log in" })).toBeTruthy()
  })

  it("tells assistive tech an answer is running while the panel is closed", async () => {
    const { streams } = stubLiveAssistant()
    const panel = await openPanel()
    await ask(panel, "hello")
    await waitFor(() => expect(streams).toHaveLength(1))
    fireEvent.keyDown(panel, { key: "Escape" })
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull())

    const button = screen.getByRole("button", { name: "Ask Babamul" })
    const describedBy = button.getAttribute("aria-describedby")
    expect(describedBy && document.getElementById(describedBy)?.textContent).toBe("Answer in progress")
  })

  it("only makes the latest answer a live region", async () => {
    sessionStorage.setItem(
      SAVED_KEY,
      JSON.stringify([
        { id: "1", role: "user", content: "q1" },
        { id: "2", role: "assistant", content: "a1", state: "done", parts: [{ kind: "text", text: "a1" }] },
        { id: "3", role: "user", content: "q2" },
        { id: "4", role: "assistant", content: "a2", state: "done", parts: [{ kind: "text", text: "a2" }] },
      ]),
    )
    stubAssistant(() => streamOf())
    const panel = await openPanel()
    await within(panel).findByText("a2")
    const live = panel.querySelectorAll("[aria-live]")
    expect(live).toHaveLength(1)
    expect(live[0].textContent).toContain("a2")
  })

  it("lets Enter start a new line on a touch screen", async () => {
    vi.stubGlobal("matchMedia", (query: string) => ({ matches: query === "(pointer: coarse)" }))
    const fetchMock = stubAssistant(() => streamOf(sse({ type: "done", content: "Hi." })))
    const panel = await openPanel()
    const box = within(panel).getByRole("textbox", { name: "Question" })
    fireEvent.change(box, { target: { value: "hello" } })
    fireEvent.keyDown(box, { key: "Enter" })

    expect(fetchMock.mock.calls.some(([u]) => u.endsWith("/chat"))).toBe(false)
    expect((box as HTMLTextAreaElement).value).toBe("hello")
    fireEvent.click(within(panel).getByRole("button", { name: "Send" }))
    await within(panel).findByText("Hi.")
  })
})
