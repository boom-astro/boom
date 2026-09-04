import { beforeEach, describe, expect, it, vi } from "vitest"
import posthog from "posthog-js"
import { identifyUser } from "@/lib/analytics"

vi.mock("posthog-js", () => ({
  default: {
    get_distinct_id: vi.fn(),
    alias: vi.fn(),
    identify: vi.fn(),
  },
}))

const mocked = vi.mocked(posthog)

beforeEach(() => vi.clearAllMocks())

describe("identifyUser", () => {
  /**
   * `posthog.identify` merges only while `$user_state` is still anonymous, so a
   * browser that signed in under an older build — identified on the username,
   * because `/profile` sent `_id` and `profile.id` was `undefined` — needs an
   * explicit alias or its history is stranded on a second person.
   */
  it("aliases the username the account was previously identified under", () => {
    mocked.get_distinct_id.mockReturnValue("ada")

    identifyUser("68f0c1a2b3c4d5e6f7a8b9c0", "ada@example.org", "ada")

    expect(mocked.alias).toHaveBeenCalledWith("68f0c1a2b3c4d5e6f7a8b9c0", "ada")
    expect(mocked.identify).toHaveBeenCalledWith("68f0c1a2b3c4d5e6f7a8b9c0", {
      email: "ada@example.org",
    })
  })

  it("leaves a stranger's distinct id alone", () => {
    // A shared browser: the id on record belongs to whoever signed in last, not
    // to this account, and an alias would irreversibly merge two real people.
    mocked.get_distinct_id.mockReturnValue("bob")

    identifyUser("68f0c1a2b3c4d5e6f7a8b9c0", "ada@example.org", "ada")

    expect(mocked.alias).not.toHaveBeenCalled()
    expect(mocked.identify).toHaveBeenCalledOnce()
  })

  it("does not alias an id to itself", () => {
    // No `profile.id` to move to, so `identifyUser` is handed the username it
    // is already identified under.
    mocked.get_distinct_id.mockReturnValue("ada")

    identifyUser("ada", "ada@example.org", "ada")

    expect(mocked.alias).not.toHaveBeenCalled()
  })

  it("skips the alias when no username vouches for the previous id", () => {
    mocked.get_distinct_id.mockReturnValue("ada")

    identifyUser("68f0c1a2b3c4d5e6f7a8b9c0", "ada@example.org")

    expect(mocked.alias).not.toHaveBeenCalled()
    expect(mocked.identify).toHaveBeenCalledOnce()
  })
})
