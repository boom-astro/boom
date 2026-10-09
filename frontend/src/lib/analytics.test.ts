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
const USER_ID = "68f0c1a2b3c4d5e6f7a8b9c0"
const EMAIL = "ada@example.org"

beforeEach(() => vi.clearAllMocks())

describe("identifyUser", () => {
  it("aliases the username the account was previously identified under", () => {
    mocked.get_distinct_id.mockReturnValue("ada")

    identifyUser(USER_ID, "ada")

    expect(mocked.alias).toHaveBeenCalledWith(USER_ID, "ada")
    expect(mocked.identify).toHaveBeenCalledOnce()
  })

  it("identifies without sending the address as a person property", () => {
    mocked.get_distinct_id.mockReturnValue("ada")

    identifyUser(USER_ID, "ada")

    expect(mocked.identify).toHaveBeenCalledWith(USER_ID)
  })

  it("identifies before aliasing", () => {
    mocked.get_distinct_id.mockReturnValue("ada")

    identifyUser(USER_ID, "ada")

    expect(mocked.identify.mock.invocationCallOrder[0]).toBeLessThan(
      mocked.alias.mock.invocationCallOrder[0]
    )
  })

  it("leaves a session identified on the address alone, rather than aliasing it", () => {
    mocked.get_distinct_id.mockReturnValue(EMAIL)

    identifyUser(USER_ID, "ada")

    expect(mocked.alias).not.toHaveBeenCalled()
  })

  it("leaves a stranger's distinct id alone", () => {
    mocked.get_distinct_id.mockReturnValue("bob")

    identifyUser(USER_ID, "ada")

    expect(mocked.alias).not.toHaveBeenCalled()
    expect(mocked.identify).toHaveBeenCalledOnce()
  })

  it("does not alias an id to itself", () => {
    mocked.get_distinct_id.mockReturnValue("ada")

    identifyUser("ada", "ada")

    expect(mocked.alias).not.toHaveBeenCalled()
  })

  it("skips the alias when nothing vouches for the previous id", () => {
    mocked.get_distinct_id.mockReturnValue("ada")

    identifyUser(USER_ID)

    expect(mocked.alias).not.toHaveBeenCalled()
    expect(mocked.identify).toHaveBeenCalledOnce()
  })
})
