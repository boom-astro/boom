import { afterEach, describe, expect, it, vi } from "vitest"
import {
  AssistantError,
  applyEvent,
  buildHistory,
  checkAssistant,
  createSSEParser,
  inAppObjectPath,
  parseSavedTurns,
  remarkObjectLinks,
  splitObjectIds,
  streamChat,
  trimImages,
  type AssistantEvent,
  type AssistantTurn,
  type Turn,
} from "@/lib/assistant"

const encoder = new TextEncoder()

function sse(...events: object[]) {
  return events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("")
}

/** A body that delivers exactly these chunks, then closes. */
function chunkedBody(chunks: (string | Uint8Array)[]) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(typeof chunk === "string" ? encoder.encode(chunk) : chunk)
      controller.close()
    },
  })
}

function stubFetch(response: Response | (() => Promise<Response>)) {
  const fetchMock = vi.fn(async () => (typeof response === "function" ? response() : response))
  vi.stubGlobal("fetch", fetchMock)
  return fetchMock
}

function streamResponse(chunks: (string | Uint8Array)[]) {
  return new Response(chunkedBody(chunks), { status: 200, headers: { "Content-Type": "text/event-stream" } })
}

async function collect(chunks: (string | Uint8Array)[]) {
  stubFetch(streamResponse(chunks))
  const events: AssistantEvent[] = []
  await streamChat({ messages: [{ role: "user", content: "hi" }] }, "tok", (e) => events.push(e))
  return events
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe("createSSEParser", () => {
  function parseAll(pieces: string[]) {
    const out: string[] = []
    const parser = createSSEParser((d) => out.push(d))
    for (const piece of pieces) parser.push(piece)
    parser.end()
    return out
  }

  it("joins an event split across chunks", () => {
    expect(parseAll(["da", "ta: {\"a\"", ":1}\n", "\n"])).toEqual(['{"a":1}'])
  })

  it("reads several events from one chunk", () => {
    expect(parseAll(["data: 1\n\ndata: 2\n\ndata: 3\n\n"])).toEqual(["1", "2", "3"])
  })

  it("dispatches a last event that has no blank line after it", () => {
    expect(parseAll(["data: 1\n\ndata: 2"])).toEqual(["1", "2"])
    expect(parseAll(["data: 1\n\ndata: 2\n"])).toEqual(["1", "2"])
  })

  it("handles CRLF, including a CR and LF in different chunks", () => {
    expect(parseAll(["data: 1\r", "\n\r\n", "data: 2\r\n\r\n"])).toEqual(["1", "2"])
  })

  it("ignores comments and other fields, and joins multi-line data", () => {
    expect(parseAll([": keepalive\n\nevent: x\nid: 7\ndata: a\ndata: b\n\n"])).toEqual(["a\nb"])
  })

  it("dispatches nothing for an empty stream", () => {
    expect(parseAll([])).toEqual([])
    expect(parseAll(["\n\n"])).toEqual([])
  })
})

describe("streamChat", () => {
  it("sends the question with the token and reads every event", async () => {
    const fetchMock = stubFetch(
      streamResponse([
        sse({ type: "status", message: "Waiting for the model" }, { type: "text", delta: "Hel" }),
        sse({ type: "text", delta: "lo" }),
        sse({ type: "done", content: "Hello" }),
      ]),
    )
    const events: AssistantEvent[] = []
    await streamChat(
      { messages: [{ role: "user", content: "hi" }], context: { survey: "ZTF", object_id: "ZTF21abcdefg" } },
      "tok",
      (e) => events.push(e),
    )

    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit]
    expect(url).toBe("/api/assistant/chat")
    expect(init.method).toBe("POST")
    const headers = new Headers(init.headers)
    expect(headers.get("Authorization")).toBe("Bearer tok")
    expect(headers.get("Content-Type")).toBe("application/json")
    expect(JSON.parse(String(init.body))).toEqual({
      messages: [{ role: "user", content: "hi" }],
      context: { survey: "ZTF", object_id: "ZTF21abcdefg" },
    })
    expect(events.map((e) => e.type)).toEqual(["status", "text", "text", "done"])
  })

  it("reassembles an event split across chunks, mid-character too", async () => {
    const bytes = encoder.encode(sse({ type: "done", content: "Mag 18.2 ± 0.1" }))
    const cut = bytes.indexOf(0xb1) // second byte of "±"
    const events = await collect([bytes.slice(0, 5), bytes.slice(5, cut), bytes.slice(cut)])
    expect(events).toEqual([{ type: "done", content: "Mag 18.2 ± 0.1" }])
  })

  it("reads several events delivered in one chunk", async () => {
    const events = await collect([
      sse(
        { type: "tool_call", id: "c1", name: "get_object", arguments: { object_id: "ZTF21abcdefg" } },
        { type: "tool_result", id: "c1", name: "get_object", ok: true, summary: "found", images: [] },
        { type: "done", content: "ok" },
      ),
    ])
    expect(events.map((e) => e.type)).toEqual(["tool_call", "tool_result", "done"])
  })

  it("reads a final event that has no blank line after it", async () => {
    const events = await collect(["data: " + JSON.stringify({ type: "done", content: "end" })])
    expect(events).toEqual([{ type: "done", content: "end" }])
  })

  it("passes an error event through and resolves", async () => {
    const events = await collect([sse({ type: "text", delta: "x" }, { type: "error", message: "The model stopped." })])
    expect(events[events.length - 1]).toEqual({ type: "error", message: "The model stopped." })
  })

  it("skips events it does not know and images that are not images", async () => {
    const events = await collect([
      sse(
        { type: "thinking", delta: "hmm" },
        { type: "tool_result", id: "c", name: "n", ok: true, summary: "", images: [{ mime_type: "text/html", data: "PGI+" }, { mime_type: "image/png", data: "iVBO" }] },
        { type: "done", content: "" },
      ),
      "data: not json\n\n",
    ])
    expect(events.map((e) => e.type)).toEqual(["tool_result", "done"])
    expect(events[0]).toMatchObject({ images: [{ mime_type: "image/png", data: "iVBO" }] })
  })

  it("stops reading after done, even if the connection stays open", async () => {
    let cancelled = false
    const body = new ReadableStream<Uint8Array>({
      start(stream) {
        stream.enqueue(encoder.encode(sse({ type: "done", content: "ok" })))
      },
      cancel() {
        cancelled = true
      },
    })
    stubFetch(new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } }))
    const events: AssistantEvent[] = []
    await streamChat({ messages: [] }, "tok", (e) => events.push(e))
    expect(events).toEqual([{ type: "done", content: "ok" }])
    expect(cancelled).toBe(true)
  })

  it("rejects as interrupted when the stream ends without done or error", async () => {
    stubFetch(streamResponse([sse({ type: "text", delta: "partial" })]))
    await expect(streamChat({ messages: [] }, "tok", () => {})).rejects.toMatchObject({ kind: "interrupted" })
  })

  it.each([
    [401, { error: "Invalid token" }, "auth", "Your session has expired. Log in again."],
    [429, { error: "You already have a question running." }, "busy", "You already have a question running."],
    [429, null, "busy", "One question at a time. Try again in a moment."],
    [503, { error: "model down" }, "unavailable", "model down"],
    [
      503,
      { error: "The model could not take this conversation, maybe because it is too long. Start a new chat and try again." },
      "unavailable",
      "The model could not take this conversation, maybe because it is too long. Start a new chat and try again.",
    ],
    [502, null, "unavailable", "The assistant is unavailable right now. Try again later."],
    [500, { error: "  " }, "unavailable", "The assistant is unavailable right now. Try again later."],
    [400, { error: "messages must end with a user message" }, "bad_request", "messages must end with a user message"],
  ])("maps HTTP %i to %s", async (status, body, kind, message) => {
    stubFetch(
      new Response(body ? JSON.stringify(body) : "<html>Bad gateway</html>", {
        status,
        headers: { "Content-Type": body ? "application/json" : "text/html" },
      }),
    )
    const onEvent = vi.fn()
    const err = await streamChat({ messages: [] }, "tok", onEvent).catch((e) => e)
    expect(err).toBeInstanceOf(AssistantError)
    expect(err).toMatchObject({ kind, message, status })
    expect(onEvent).not.toHaveBeenCalled()
  })

  it("maps a network failure to unavailable", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Promise.reject(new TypeError("Failed to fetch"))))
    await expect(streamChat({ messages: [] }, "tok", () => {})).rejects.toMatchObject({ kind: "unavailable" })
  })

  it("rejects as interrupted when the network drops mid-stream, after the events so far", async () => {
    const body = new ReadableStream<Uint8Array>({
      start(stream) {
        stream.enqueue(encoder.encode(sse({ type: "text", delta: "Hal" })))
      },
      pull(stream) {
        // What fetch does when the connection is lost while reading.
        stream.error(new TypeError("network error"))
      },
    })
    stubFetch(new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } }))
    const events: AssistantEvent[] = []
    await expect(streamChat({ messages: [] }, "tok", (e) => events.push(e))).rejects.toMatchObject({
      kind: "interrupted",
      message: "The answer was cut off. Try again.",
    })
    expect(events).toEqual([{ type: "text", delta: "Hal" }])
  })

  it("rejects as aborted when stopped before the response arrives", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        (_url: string, init: RequestInit) =>
          new Promise<Response>((_resolve, reject) => {
            init.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")))
          }),
      ),
    )
    const controller = new AbortController()
    const pending = streamChat({ messages: [] }, "tok", () => {}, controller.signal)
    controller.abort()
    await expect(pending).rejects.toMatchObject({ kind: "aborted" })
  })

  it("rejects as aborted when stopped mid-stream, after the events so far", async () => {
    const controller = new AbortController()
    const body = new ReadableStream<Uint8Array>({
      start(stream) {
        stream.enqueue(encoder.encode(sse({ type: "text", delta: "Hal" })))
        // Like fetch, error the body when the request is aborted.
        controller.signal.addEventListener("abort", () => stream.error(new DOMException("Aborted", "AbortError")))
      },
    })
    stubFetch(new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } }))
    const events: AssistantEvent[] = []
    const pending = streamChat({ messages: [] }, "tok", (e) => {
      events.push(e)
      controller.abort()
    }, controller.signal)
    await expect(pending).rejects.toMatchObject({ kind: "aborted" })
    expect(events).toEqual([{ type: "text", delta: "Hal" }])
  })
})

describe("checkAssistant", () => {
  it("returns the model when the service is up", async () => {
    stubFetch(new Response(JSON.stringify({ status: "ok", model: "qwen", vision: false }), { status: 200 }))
    expect(await checkAssistant()).toEqual({ model: "qwen", vision: false })
  })

  it.each([
    ["a 404", new Response("{}", { status: 404 })],
    ["the SPA's index.html", new Response("<!doctype html><html></html>", { status: 200 })],
    ["another JSON body", new Response(JSON.stringify({ status: "down" }), { status: 200 })],
  ])("returns null for %s", async (_name, response) => {
    stubFetch(response)
    expect(await checkAssistant()).toBeNull()
  })
})

function answer(...events: AssistantEvent[]): AssistantTurn {
  const start: AssistantTurn = { id: "a", role: "assistant", parts: [], content: "", state: "running" }
  return events.reduce(applyEvent, start)
}

describe("applyEvent", () => {
  it("keeps text and tool calls in the order they streamed", () => {
    const turn = answer(
      { type: "text", delta: "Let me " },
      { type: "text", delta: "check." },
      { type: "tool_call", id: "c1", name: "get_object", arguments: { object_id: "ZTF21abcdefg" } },
      { type: "tool_result", id: "c1", name: "get_object", ok: true, summary: "An SN Ia", images: [] },
      { type: "text", delta: "It is " },
      { type: "done", content: "It is an SN Ia." },
    )
    expect(turn.state).toBe("done")
    expect(turn.content).toBe("It is an SN Ia.")
    expect(turn.parts).toEqual([
      { kind: "text", text: "Let me check." },
      {
        kind: "tool",
        step: {
          id: "c1",
          name: "get_object",
          arguments: { object_id: "ZTF21abcdefg" },
          state: "ok",
          summary: "An SN Ia",
          images: [],
        },
      },
      { kind: "text", text: "It is an SN Ia." },
    ])
  })

  it("marks a failed tool, and tools left running when the stream errors", () => {
    const turn = answer(
      { type: "tool_call", id: "c1", name: "a", arguments: {} },
      { type: "tool_result", id: "c1", name: "a", ok: false, summary: "404", images: [] },
      { type: "tool_call", id: "c2", name: "b", arguments: {} },
      { type: "error", message: "Model timed out." },
    )
    expect(turn.state).toBe("error")
    expect(turn.error?.message).toBe("Model timed out.")
    expect(turn.parts.map((p) => p.kind === "tool" && p.step.state)).toEqual(["failed", "failed"])
  })
})

describe("buildHistory", () => {
  const user = (id: string, content: string): Turn => ({ id, role: "user", content })
  const done = (id: string, content: string): Turn => ({ id, role: "assistant", parts: [], content, state: "done" })

  it("sends earlier text turns and the new question, oldest first", () => {
    expect(buildHistory([user("1", "q1"), done("2", "a1")], "q2")).toEqual([
      { role: "user", content: "q1" },
      { role: "assistant", content: "a1" },
      { role: "user", content: "q2" },
    ])
  })

  it("leaves out questions that got no answer", () => {
    const failed: Turn = { id: "2", role: "assistant", parts: [], content: "", state: "error" }
    expect(buildHistory([user("1", "q1"), failed], "q2")).toEqual([{ role: "user", content: "q2" }])
  })

  it("keeps the last 30 messages, starting with a question", () => {
    const turns: Turn[] = []
    for (let i = 0; i < 20; i++) turns.push(user(`u${i}`, `q${i}`), done(`a${i}`, `a${i}`))
    const history = buildHistory(turns, "last")
    expect(history.length).toBeLessThanOrEqual(30)
    expect(history[0].role).toBe("user")
    expect(history[history.length - 1]).toEqual({ role: "user", content: "last" })
  })
})

describe("parseSavedTurns", () => {
  it("reads an answer cut off by a reload as stopped", () => {
    const saved = JSON.stringify([
      { id: "1", role: "user", content: "q" },
      {
        id: "2",
        role: "assistant",
        content: "",
        state: "running",
        parts: [{ kind: "tool", step: { id: "c", name: "n", arguments: {}, state: "running", images: [] } }],
      },
    ])
    const [, turn] = parseSavedTurns(saved) as [Turn, AssistantTurn]
    expect(turn.state).toBe("stopped")
    expect(turn.parts[0]).toMatchObject({ step: { state: "failed" } })
  })

  it("returns nothing for missing or broken storage", () => {
    expect(parseSavedTurns(null)).toEqual([])
    expect(parseSavedTurns("{")).toEqual([])
    expect(parseSavedTurns(JSON.stringify([{ role: "system" }]))).toEqual([])
  })
})

describe("trimImages", () => {
  const image = (data: string) => ({ mime_type: "image/png", data })
  const withImages = (id: string, ...images: string[]): AssistantTurn => ({
    id,
    role: "assistant",
    content: id,
    state: "done",
    parts: [
      { kind: "text", text: id },
      ...images.map((data, i) => ({
        kind: "tool" as const,
        step: { id: `${id}-${i}`, name: "plot", arguments: {}, state: "ok" as const, images: [image(data)] },
      })),
    ],
  })
  const kept = (turns: Turn[]) =>
    turns.flatMap((t) =>
      t.role === "assistant" ? t.parts.flatMap((p) => (p.kind === "tool" ? p.step.images.map((i) => i.data) : [])) : [],
    )

  it("keeps the newest images that fit and drops older ones first", () => {
    const turns: Turn[] = [
      { id: "q1", role: "user", content: "q1" },
      withImages("a1", "1111", "2222"),
      { id: "q2", role: "user", content: "q2" },
      withImages("a2", "3333", "4444"),
    ]
    expect(kept(trimImages(turns, 100))).toEqual(["1111", "2222", "3333", "4444"])
    expect(kept(trimImages(turns, 12))).toEqual(["2222", "3333", "4444"])
    expect(kept(trimImages(turns, 9))).toEqual(["3333", "4444"])
    expect(kept(trimImages(turns, 3))).toEqual([])
    // The text and the tool calls stay; only the pictures go.
    const none = trimImages(turns, 0)
    expect(none.map((t) => t.role === "assistant" && t.parts.length)).toEqual([false, 3, false, 3])
    expect(kept(turns)).toHaveLength(4)
  })

  it("does not keep an older image that fits after a newer one did not", () => {
    const turns: Turn[] = [withImages("a1", "11"), withImages("a2", "22222222")]
    expect(kept(trimImages(turns, 4))).toEqual([])
  })
})

describe("object links", () => {
  it("finds ZTF object ids in text", () => {
    expect(splitObjectIds("See ZTF21abcdefg and ZTF22xyzabcd.")).toEqual([
      { text: "See " },
      { text: "ZTF21abcdefg", objectId: "ZTF21abcdefg" },
      { text: " and " },
      { text: "ZTF22xyzabcd", objectId: "ZTF22xyzabcd" },
      { text: "." },
    ])
    expect(splitObjectIds("ZTF21abcdefgh ZTF21ABCDEFG")).toEqual([{ text: "ZTF21abcdefgh ZTF21ABCDEFG" }])
  })

  it("links bare ids but leaves links and code alone", () => {
    const tree = {
      type: "root",
      children: [
        {
          type: "paragraph",
          children: [
            { type: "text", value: "Try ZTF21abcdefg" },
            { type: "link", url: "/x", children: [{ type: "text", value: "ZTF21abcdefg" }] },
            { type: "inlineCode", value: "ZTF21abcdefg" },
          ],
        },
      ],
    }
    remarkObjectLinks()(tree)
    expect(tree.children[0].children).toEqual([
      { type: "text", value: "Try " },
      { type: "link", url: "/objects/ZTF/ZTF21abcdefg", children: [{ type: "text", value: "ZTF21abcdefg" }] },
      { type: "link", url: "/x", children: [{ type: "text", value: "ZTF21abcdefg" }] },
      { type: "inlineCode", value: "ZTF21abcdefg" },
    ])
  })

  it("routes same-origin object links in the app", () => {
    const origin = "https://babamul.example.org"
    expect(inAppObjectPath("/objects/ZTF/ZTF21abcdefg", origin)).toBe("/objects/ZTF/ZTF21abcdefg")
    expect(inAppObjectPath(`${origin}/objects/LSST/123?x=1`, origin)).toBe("/objects/LSST/123?x=1")
    expect(inAppObjectPath("https://other.example.org/objects/ZTF/a", origin)).toBeNull()
    expect(inAppObjectPath("/query", origin)).toBeNull()
    expect(inAppObjectPath(undefined, origin)).toBeNull()
  })
})
