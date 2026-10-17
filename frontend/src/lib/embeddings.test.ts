import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { deleteObjectEmbedding, fetchEmbeddingsCount, fetchSimilarObjects } from "@/lib/api"

/**
 * Similarity search is the one place the UI asks Milvus a question directly, so
 * what matters is that a "nothing stored" answer stays distinguishable from a
 * failure: an object with no embedding is a normal outcome (only ZTF alerts
 * that pass the AppleCiDEr gate get one), not an error the user should chase.
 */

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
    // Only ZTF alerts that clear the AppleCiDEr gate get an embedding, so
    // "no embedding stored" is an ordinary answer about an object that exists.
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
    // The page is public like /dashboard, so a signed-out visitor must get
    // results rather than the "Not authenticated" throw fetchWithAuth raises.
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

describe("deleteObjectEmbedding", () => {
  it("DELETEs the object and reports how many rows went", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ message: "success", data: { deleted: 1 } }))
    vi.stubGlobal("fetch", fetchMock)

    const deleted = await deleteObjectEmbedding("ZTF18abcdefg")

    const { url, init } = lastRequest(fetchMock)
    expect(url).toBe("/api/babamul/embeddings/ZTF18abcdefg")
    expect(init.method).toBe("DELETE")
    expect(deleted).toBe(1)
  })

  it("reports 0 when the object had nothing stored", async () => {
    // Milvus deletes by key without checking existence, so a no-op comes back
    // as a success with deleted: 0 — the caller has to tell the two apart.
    const fetchMock = vi.fn(async () => jsonResponse({ message: "success", data: { deleted: 0 } }))
    vi.stubGlobal("fetch", fetchMock)

    await expect(deleteObjectEmbedding("ZTF18abcdefg")).resolves.toBe(0)
  })

  it("surfaces the server's refusal for a non-admin", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ message: "Access denied: Admins only" }, 403))
    vi.stubGlobal("fetch", fetchMock)

    await expect(deleteObjectEmbedding("ZTF18abcdefg")).rejects.toThrow("Access denied: Admins only")
  })
})
