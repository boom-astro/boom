import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { fetchProfile, logout, setLogoutHandler, updateProfileName, UnauthorizedError, TOKEN_KEY } from "@/lib/api"

type ProfileData = Record<string, unknown>

function stubFetch(body: unknown, status = 200) {
  const fetchMock = vi.fn(
    async () => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } })
  )
  vi.stubGlobal("fetch", fetchMock)
  return fetchMock
}

function stubProfile(data: ProfileData) {
  return stubFetch({ message: "success", data })
}

function lastRequest(fetchMock: ReturnType<typeof vi.fn>) {
  const calls = fetchMock.mock.calls
  const [url, init] = calls[calls.length - 1] as [string, RequestInit]
  return { url, init, body: JSON.parse(String(init.body)) }
}

function signIn() {
  localStorage.setItem(
    TOKEN_KEY,
    JSON.stringify({ access_token: "test-token", token_type: "Bearer", expires_at: Date.now() + 3_600_000 })
  )
}

beforeEach(signIn)

afterEach(() => {
  localStorage.clear()
  vi.unstubAllGlobals()
})

describe("updateProfileName", () => {
  it("PATCHes the profile and returns the updated one", async () => {
    const fetchMock = stubProfile({ id: "1", username: "ada", email: "ada@example.org", created_at: 0, name: "Ada Lovelace" })

    const profile = await updateProfileName("Ada Lovelace")

    const { url, init, body } = lastRequest(fetchMock)
    expect(url).toBe("/api/babamul/profile")
    expect(init.method).toBe("PATCH")
    expect(body).toEqual({ name: "Ada Lovelace" })
    expect(profile?.name).toBe("Ada Lovelace")
  })

  it("sends an empty name to clear it, rather than omitting the field", async () => {
    const fetchMock = stubProfile({ id: "1", username: "ada", email: "ada@example.org", created_at: 0, name: null })

    const profile = await updateProfileName("")

    expect(lastRequest(fetchMock).body).toEqual({ name: "" })
    expect(profile?.name ?? null).toBeNull()
  })

  it("surfaces the API's message when the name is rejected", async () => {
    stubFetch({ message: "Name must be at most 100 characters" }, 400)

    await expect(updateProfileName("a".repeat(101))).rejects.toThrow(
      "Name must be at most 100 characters"
    )
  })
})

describe("fetchProfile", () => {
  it("reads the id the API sends", async () => {
    stubProfile({ id: "68f0c1a2b3c4d5e6f7a8b9c0", username: "ada", email: "ada@example.org", created_at: 0 })

    const profile = await fetchProfile()

    expect(profile?.id).toBe("68f0c1a2b3c4d5e6f7a8b9c0")
    expect(profile?.email).toBe("ada@example.org")
  })

  it("leaves the id undefined when the API sends neither spelling", async () => {
    stubProfile({ username: "ada", email: "ada@example.org", created_at: 0 })

    const profile = await fetchProfile()

    expect(profile?.id).toBeUndefined()
    expect(profile?.username).toBe("ada")
  })

  it("still accepts the legacy `_id` spelling", async () => {
    stubProfile({ _id: "abc123", username: "ada", email: "ada@example.org", created_at: 0 })

    expect((await fetchProfile())?.id).toBe("abc123")
  })

  it("treats an empty id as no id", async () => {
    stubProfile({ id: "", username: "ada", email: "ada@example.org", created_at: 0 })

    expect((await fetchProfile())?.id).toBeUndefined()
  })

  it("returns null when the body isn't JSON", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("<html>502</html>", { status: 200, headers: { "Content-Type": "text/html" } }))
    )

    expect(await fetchProfile()).toBeNull()
  })

  it("returns null when the payload has no username", async () => {
    stubProfile({ email: "ada@example.org", created_at: 0 })

    expect(await fetchProfile()).toBeNull()
  })

  it("returns null for a body that parsed but carries no account", async () => {
    stubFetch({ message: "upstream unavailable" })

    expect(await fetchProfile()).toBeNull()
  })
})

describe("an expired session", () => {
  afterEach(() => setLogoutHandler(null))

  it("is distinguishable from a request that merely failed", async () => {
    // Whether the user gets signed out turns on this difference, so it cannot
    // be a message a caller string-matches on.
    stubFetch({ message: "no" }, 401)
    await expect(fetchProfile()).rejects.toBeInstanceOf(UnauthorizedError)

    // The 401 above took the token with it, so sign back in: otherwise the
    // next call fails for want of a credential and proves nothing.
    signIn()
    stubFetch({ message: "database is down" }, 500)
    const failure = await fetchProfile().catch((e) => e)
    expect(failure).toBeInstanceOf(Error)
    expect(failure).not.toBeInstanceOf(UnauthorizedError)
  })

  it("has no token left to retry with", async () => {
    stubFetch({ message: "no" }, 401)
    await fetchProfile().catch(() => {})
    expect(localStorage.getItem(TOKEN_KEY)).toBeNull()
  })

  it("tears down whatever else was holding the session", async () => {
    // The token is only half of a session. The rest is the persisted profile,
    // which outlives it and leaves a page that was gated on it still rendered.
    const torn = vi.fn()
    setLogoutHandler(torn)
    stubFetch({ message: "no" }, 401)

    await fetchProfile().catch(() => {})

    expect(torn).toHaveBeenCalledTimes(1)
  })

  it("tears down on a deliberate sign-out too", () => {
    const torn = vi.fn()
    setLogoutHandler(torn)
    logout()
    expect(torn).toHaveBeenCalledTimes(1)
  })

  it("refuses a request with no token without reaching the network", async () => {
    localStorage.clear()
    const fetchMock = stubFetch({ message: "unreachable" })
    await expect(fetchProfile()).rejects.toBeInstanceOf(UnauthorizedError)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it("tears down the session when the token has expired on its own", async () => {
    // The common way a session ends: nothing returns 401 because the request is
    // never made -- `getTokenRecord` finds an expired record and drops it. That
    // still has to clear the rest of the session, or a page polling directly
    // stays rendered on a profile whose credential is gone.
    const torn = vi.fn()
    setLogoutHandler(torn)
    localStorage.setItem(
      TOKEN_KEY,
      JSON.stringify({ access_token: "stale", token_type: "Bearer", expires_at: Date.now() - 1000 })
    )
    const fetchMock = stubFetch({ message: "unreachable" })

    await expect(fetchProfile()).rejects.toBeInstanceOf(UnauthorizedError)

    expect(fetchMock).not.toHaveBeenCalled()
    expect(torn).toHaveBeenCalledTimes(1)
  })
})
