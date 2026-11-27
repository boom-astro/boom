import { afterEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen, waitFor } from "@testing-library/react"
import { MemoryRouter } from "react-router-dom"
import { OAuthButtons } from "@/components/oauth-buttons"

/**
 * A deployment must never advertise a sign-in provider it has not configured.
 *
 * The backend decides which providers exist — a provider is enabled exactly
 * when both halves of its credential are set — and these tests pin the other
 * half of that contract: the web app renders buttons for what
 * `/oauth/providers` returns and nothing else, and it renders nothing at all
 * when the answer is empty or unusable.
 */

/** Stand in for the API, returning `body` as a JSON response with `status`. */
function mockApi(body: unknown, status = 200) {
  const fetchMock = vi.fn(async () =>
    new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    })
  )
  vi.stubGlobal("fetch", fetchMock)
  return fetchMock
}

function renderButtons() {
  return render(
    <MemoryRouter>
      <OAuthButtons />
    </MemoryRouter>
  )
}

/** Every provider the app knows how to draw a button for. */
const ALL_PROVIDERS = ["Google", "GitHub", "ORCID"]

function buttonFor(providerName: string) {
  return screen.queryByRole("button", { name: new RegExp(providerName, "i") })
}

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

describe("OAuthButtons", () => {
  it("shows a button only for the providers the API reports", async () => {
    mockApi({
      message: "success",
      data: [{ id: "github", name: "GitHub", start_url: "/babamul/oauth/github/start" }],
    })

    renderButtons()

    await waitFor(() => expect(buttonFor("GitHub")).not.toBeNull())
    // Google and ORCID are compiled into the app — icons and all — and must
    // still stay hidden purely because the deployment has no credentials for
    // them. This is the case a half-configured provider would break.
    expect(buttonFor("Google")).toBeNull()
    expect(buttonFor("ORCID")).toBeNull()
  })

  it("renders nothing when no provider is configured", async () => {
    const fetchMock = mockApi({ message: "success", data: [] })

    const { container } = renderButtons()

    await waitFor(() => expect(fetchMock).toHaveBeenCalled())
    // Not even the "or" divider: a password-only deployment should look like
    // one that never had social sign-in at all.
    expect(container.firstChild).toBeNull()
    for (const provider of ALL_PROVIDERS) {
      expect(buttonFor(provider)).toBeNull()
    }
  })

  it("comes back on its own once the API is reachable again", async () => {
    // The API restarts on every source edit in development, and a login page
    // that gave up on the first failure showed a password-only form until
    // someone reloaded it by hand.
    vi.useFakeTimers()
    let attempts = 0
    const fetchMock = vi.fn(async () => {
      attempts += 1
      if (attempts === 1) throw new TypeError("API restarting")
      return new Response(
        JSON.stringify({
          message: "success",
          data: [{ id: "github", name: "GitHub", start_url: "/babamul/oauth/github/start" }],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      )
    })
    vi.stubGlobal("fetch", fetchMock)

    renderButtons()

    // Nothing is shown while it cannot ask -- failing closed still holds.
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    expect(buttonFor("GitHub")).toBeNull()

    await vi.advanceTimersByTimeAsync(1000)
    await vi.waitFor(() => expect(buttonFor("GitHub")).not.toBeNull())
    vi.useRealTimers()
  })

  it("retries when the body never finishes arriving", async () => {
    // The headers arrived and the connection then dropped. That is a transport
    // failure, not the deployment answering "no providers" -- and it used to
    // read as the latter, because the body read was swallowed into an empty
    // list that the component treated as final.
    vi.useFakeTimers()
    let attempts = 0
    const fetchMock = vi.fn(async () => {
      attempts += 1
      if (attempts === 1) {
        return {
          ok: true,
          status: 200,
          text: () => Promise.reject(new TypeError("connection reset")),
        } as unknown as Response
      }
      return new Response(
        JSON.stringify({
          message: "success",
          data: [{ id: "github", name: "GitHub", start_url: "/babamul/oauth/github/start" }],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      )
    })
    vi.stubGlobal("fetch", fetchMock)

    renderButtons()

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    expect(buttonFor("GitHub")).toBeNull()
    await vi.advanceTimersByTimeAsync(1000)
    await vi.waitFor(() => expect(buttonFor("GitHub")).not.toBeNull())
    vi.useRealTimers()
  })

  it("does not retry a body it read in full but could not parse", async () => {
    // Read to the end and not JSON: the API disagreeing with us, which no
    // amount of retrying fixes. The distinction from the case above is the
    // whole point -- one is the transport, the other is the answer.
    vi.useFakeTimers()
    const fetchMock = vi.fn(
      async () =>
        new Response("not json at all", {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
    )
    vi.stubGlobal("fetch", fetchMock)

    const { container } = renderButtons()

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    await vi.advanceTimersByTimeAsync(30_000)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(container.firstChild).toBeNull()
    vi.useRealTimers()
  })

  it("does not retry an answer it could read", async () => {
    // An empty list is this deployment saying it has no provider, which is
    // final. Retrying it would poll a healthy API forever.
    vi.useFakeTimers()
    const fetchMock = mockApi({ message: "success", data: [] })

    renderButtons()

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    await vi.advanceTimersByTimeAsync(30_000)
    expect(fetchMock).toHaveBeenCalledTimes(1)
    vi.useRealTimers()
  })

  it.each([
    ["the API returns an error", () => mockApi({ message: "boom" }, 500)],
    ["the response is not a provider list", () => mockApi({ message: "success", data: { google: true } })],
    [
      "the request fails outright",
      () => {
        const fetchMock = vi.fn(async () => {
          throw new TypeError("network down")
        })
        vi.stubGlobal("fetch", fetchMock)
        return fetchMock
      },
    ],
  ])("renders nothing when %s", async (_case, setup) => {
    const fetchMock = setup()

    const { container } = renderButtons()

    // Failing closed is the point: an unreachable or confused API must not be
    // able to put a button on the page, and must not break password sign-in.
    await waitFor(() => expect(fetchMock).toHaveBeenCalled())
    expect(container.firstChild).toBeNull()
    for (const provider of ALL_PROVIDERS) {
      expect(buttonFor(provider)).toBeNull()
    }
  })
})
