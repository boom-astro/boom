import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { ensureProfileLoaded, useAppStore } from "@/lib/store"
import { fetchProfile, logout, TOKEN_KEY } from "@/lib/api"

const PROFILE = {
  id: "1",
  username: "ada",
  email: "ada@example.org",
  created_at: 0,
  is_admin: true,
}

function stubFetch(body: unknown, status = 200) {
  const fetchMock = vi.fn(
    async () =>
      new Response(JSON.stringify(body), {
        status,
        headers: { "Content-Type": "application/json" },
      })
  )
  vi.stubGlobal("fetch", fetchMock)
  return fetchMock
}

function signIn() {
  localStorage.setItem(
    TOKEN_KEY,
    JSON.stringify({
      access_token: "test-token",
      token_type: "Bearer",
      expires_at: Date.now() + 3_600_000,
    })
  )
}

beforeEach(() => {
  signIn()
  useAppStore.setState({ profile: PROFILE, lastUpdated: Date.now() })
  vi.spyOn(console, "error").mockImplementation(() => {})
})

afterEach(() => {
  localStorage.clear()
  useAppStore.setState({ profile: null, lastUpdated: null })
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe("ensureProfileLoaded", () => {
  it("keeps the session through a transient failure", async () => {
    // The case this exists for: a 500 on this one request used to sign the user
    // out of the whole app and drop them on the landing page, discarding
    // anything they were part way through on an admin or query page.
    stubFetch({ message: "database is down" }, 500)

    const profile = await ensureProfileLoaded({ force: true })

    expect(profile).toEqual(PROFILE)
    expect(useAppStore.getState().profile).toEqual(PROFILE)
    expect(localStorage.getItem(TOKEN_KEY)).not.toBeNull()
  })

  it("signs the user out when the session really is gone", async () => {
    stubFetch({ message: "no" }, 401)

    const profile = await ensureProfileLoaded({ force: true })

    expect(profile).toBeNull()
    expect(useAppStore.getState().profile).toBeNull()
    expect(localStorage.getItem(TOKEN_KEY)).toBeNull()
  })

  it("clears the persisted profile on any sign-out", () => {
    // The profile is persisted, so without this a signed-out admin's
    // `is_admin` survives in localStorage and the admin shell still renders.
    logout()
    expect(useAppStore.getState().profile).toBeNull()
  })

  it("drops a profile left behind by an expired token on a direct call", async () => {
    // Not through ensureProfileLoaded: a page polling the API directly is the
    // case that used to keep the profile and stay rendered.
    localStorage.setItem(
      TOKEN_KEY,
      JSON.stringify({ access_token: "stale", token_type: "Bearer", expires_at: Date.now() - 1000 })
    )
    stubFetch({ message: "unreachable" })

    await expect(fetchProfile()).rejects.toThrow()

    expect(useAppStore.getState().profile).toBeNull()
  })

  it("drops a profile left behind by a vanished token", async () => {
    localStorage.clear()
    const fetchMock = stubFetch({ message: "unreachable" })

    expect(await ensureProfileLoaded()).toBeNull()
    expect(useAppStore.getState().profile).toBeNull()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it("serves a fresh profile from the store without a request", async () => {
    const fetchMock = stubFetch({ message: "unreachable" })

    expect(await ensureProfileLoaded()).toEqual(PROFILE)
    expect(fetchMock).not.toHaveBeenCalled()
  })
})
