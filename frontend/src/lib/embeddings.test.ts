import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { fetchEmbeddingsCount, fetchSimilarObjects } from "@/lib/api"

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  })
}

/** `fetchWithAuth` refuses to send anything without a stored token. */
function signIn() {
  localStorage.setItem(
    "api_token",
    JSON.stringify({ access_token: "test-token", token_type: "Bearer", expires_at: Date.now() + 3_600_000 })
  )
}

function lastRequest(fetchMock: ReturnType<typeof vi.fn>) {
  const calls = fetchMock.mock.calls
  const [url, init] = calls[calls.length - 1] as [string, RequestInit]
  return { url, init }
}

beforeEach(() => signIn())

afterEach(() => {
  localStorage.clear()
  vi.unstubAllGlobals()
})

describe("fetchSimilarObjects", () => {
  it("posts the object and top_k, and returns the neighbors", async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({
        message: "success",
        data: [
          { object_id: "ZTF21aaa", score: 0.98, candid: 123, jd: 2460000.5 },
          { object_id: "ZTF21bbb", score: 0.91, candid: 456, jd: 2460001.5 },
        ],
      })
    )
    vi.stubGlobal("fetch", fetchMock)

    const { results } = await fetchSimilarObjects("ZTF18abcdefg", 2)

    const { url, init } = lastRequest(fetchMock)
    expect(url).toBe("/api/babamul/similarity/objects")
    expect(init.method).toBe("POST")
    expect(JSON.parse(String(init.body))).toEqual({ object_id: "ZTF18abcdefg", top_k: 2 })
    expect(results.map((r) => r.object_id)).toEqual(["ZTF21aaa", "ZTF21bbb"])
    expect(results[0].score).toBeCloseTo(0.98)
  })

  it("treats 404 as an empty result with the server's explanation, not an error", async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({ message: "No embedding stored for object ZTF18abcdefg" }, 404)
    )
    vi.stubGlobal("fetch", fetchMock)

    const { results, message } = await fetchSimilarObjects("ZTF18abcdefg")

    expect(results).toEqual([])
    expect(message).toBe("No embedding stored for object ZTF18abcdefg")
  })

  it("throws when Milvus is unavailable, so the page can say so", async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({ message: "Milvus is not enabled or was unavailable at startup" }, 500)
    )
    vi.stubGlobal("fetch", fetchMock)

    await expect(fetchSimilarObjects("ZTF18abcdefg")).rejects.toThrow(
      "Milvus is not enabled or was unavailable at startup"
    )
  })

  it("works signed out, and sends no Authorization header", async () => {
    localStorage.clear()
    const fetchMock = vi.fn(async () =>
      jsonResponse({ message: "success", data: [{ object_id: "ZTF21aaa", score: 0.9 }] })
    )
    vi.stubGlobal("fetch", fetchMock)

    const { results } = await fetchSimilarObjects("ZTF18abcdefg")

    expect(results).toHaveLength(1)
    const { init } = lastRequest(fetchMock)
    expect(new Headers(init.headers).has("Authorization")).toBe(false)
  })

  it("defaults to 10 neighbors when no count is given", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ message: "success", data: [] }))
    vi.stubGlobal("fetch", fetchMock)

    await fetchSimilarObjects("ZTF18abcdefg")

    const { init } = lastRequest(fetchMock)
    expect(JSON.parse(String(init.body)).top_k).toBe(10)
  })
})

describe("fetchEmbeddingsCount", () => {
  it("unwraps the count", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ message: "success", data: { count: 4211 } }))
    vi.stubGlobal("fetch", fetchMock)

    await expect(fetchEmbeddingsCount()).resolves.toBe(4211)
    expect(lastRequest(fetchMock).url).toBe("/api/babamul/embeddings/count")
  })
})
