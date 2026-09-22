import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { updateProfileName } from "@/lib/api"

/**
 * The display name is the one profile field a user can change, so what matters
 * is that the request says what they meant: a name goes up as typed, and an
 * empty one is a deliberate "clear it" rather than a no-op the API ignores.
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
  // Indexed rather than `.at(-1)`: this project's lib floor is ES2020, which
  // is the browser API surface the app is allowed to use.
  const calls = fetchMock.mock.calls
  const [url, init] = calls[calls.length - 1] as [string, RequestInit]
  return { url, init, body: JSON.parse(String(init.body)) }
}

beforeEach(() => signIn())

afterEach(() => {
  localStorage.clear()
  vi.unstubAllGlobals()
})

describe("updateProfileName", () => {
  it("PATCHes the profile and returns the updated one", async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({ message: "success", data: { id: "1", username: "ada", email: "ada@example.org", created_at: 0, name: "Ada Lovelace" } })
    )
    vi.stubGlobal("fetch", fetchMock)

    const profile = await updateProfileName("Ada Lovelace")

    const { url, init, body } = lastRequest(fetchMock)
    expect(url).toBe("/api/babamul/profile")
    expect(init.method).toBe("PATCH")
    expect(body).toEqual({ name: "Ada Lovelace" })
    expect(profile?.name).toBe("Ada Lovelace")
  })

  it("sends an empty name to clear it, rather than omitting the field", async () => {
    // An omitted `name` means "leave it alone" to the API, so clearing has to
    // be an explicit empty string or the request would silently do nothing.
    const fetchMock = vi.fn(async () =>
      jsonResponse({ message: "success", data: { id: "1", username: "ada", email: "ada@example.org", created_at: 0, name: null } })
    )
    vi.stubGlobal("fetch", fetchMock)

    const profile = await updateProfileName("")

    expect(lastRequest(fetchMock).body).toEqual({ name: "" })
    expect(profile?.name ?? null).toBeNull()
  })

  it("surfaces the API's message when the name is rejected", async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({ message: "Name must be at most 100 characters" }, 400)
    )
    vi.stubGlobal("fetch", fetchMock)

    await expect(updateProfileName("a".repeat(101))).rejects.toThrow(
      "Name must be at most 100 characters"
    )
  })
})
