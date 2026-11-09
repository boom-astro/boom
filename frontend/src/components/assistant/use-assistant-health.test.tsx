import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, renderHook } from "@testing-library/react"

const checkAssistant = vi.hoisted(() => vi.fn())
vi.mock("@/lib/assistant", () => ({ checkAssistant }))

const UP = { model: "qwen", vision: false }

/** The module remembers a healthy answer, so each test starts from a fresh copy. */
async function freshHook() {
  vi.resetModules()
  return (await import("@/components/assistant/use-assistant-health")).useAssistantAvailable
}

const wait = (ms: number) =>
  act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })

beforeEach(() => {
  vi.useFakeTimers()
  checkAssistant.mockReset()
  checkAssistant.mockResolvedValue(null)
})

afterEach(() => {
  cleanup()
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe("useAssistantAvailable", () => {
  it("asks again with a growing wait while the service is down, and stops once it is up", async () => {
    const useAssistantAvailable = await freshHook()
    const { result } = renderHook(() => useAssistantAvailable(true))
    await wait(0)
    expect(result.current).toBe(false)
    expect(checkAssistant).toHaveBeenCalledTimes(1)

    await wait(4_999)
    expect(checkAssistant).toHaveBeenCalledTimes(1)
    await wait(1)
    expect(checkAssistant).toHaveBeenCalledTimes(2)
    await wait(10_000)
    expect(checkAssistant).toHaveBeenCalledTimes(3)

    checkAssistant.mockResolvedValue(UP)
    await wait(20_000)
    expect(checkAssistant).toHaveBeenCalledTimes(4)
    expect(result.current).toBe(true)

    await wait(10 * 60_000)
    expect(checkAssistant).toHaveBeenCalledTimes(4)
  })

  it("waits at most a minute between checks", async () => {
    const useAssistantAvailable = await freshHook()
    renderHook(() => useAssistantAvailable(true))
    await wait(5_000 + 10_000 + 20_000 + 40_000)
    expect(checkAssistant).toHaveBeenCalledTimes(5)
    await wait(60_000)
    expect(checkAssistant).toHaveBeenCalledTimes(6)
    await wait(60_000)
    expect(checkAssistant).toHaveBeenCalledTimes(7)
  })

  it("asks again at once when the window gets focus", async () => {
    const useAssistantAvailable = await freshHook()
    const { result } = renderHook(() => useAssistantAvailable(true))
    await wait(0)
    expect(result.current).toBe(false)

    checkAssistant.mockResolvedValue(UP)
    await act(async () => {
      window.dispatchEvent(new Event("focus"))
    })
    expect(checkAssistant).toHaveBeenCalledTimes(2)
    expect(result.current).toBe(true)
  })

  it("does not poll from a background tab, and asks when the tab is shown again", async () => {
    const visibility = vi.spyOn(document, "visibilityState", "get").mockReturnValue("hidden")
    const useAssistantAvailable = await freshHook()
    const { result } = renderHook(() => useAssistantAvailable(true))
    await wait(10 * 60_000)
    expect(checkAssistant).not.toHaveBeenCalled()

    visibility.mockReturnValue("visible")
    checkAssistant.mockResolvedValue(UP)
    await act(async () => {
      document.dispatchEvent(new Event("visibilitychange"))
    })
    expect(checkAssistant).toHaveBeenCalledTimes(1)
    expect(result.current).toBe(true)
  })

  it("asks as soon as the user logs in, even right after a failed check", async () => {
    const useAssistantAvailable = await freshHook()
    const { result, rerender } = renderHook(({ enabled }) => useAssistantAvailable(enabled), {
      initialProps: { enabled: true },
    })
    await wait(0)
    expect(checkAssistant).toHaveBeenCalledTimes(1)

    rerender({ enabled: false })
    await wait(60_000)
    expect(checkAssistant).toHaveBeenCalledTimes(1)
    expect(result.current).toBe(false)

    checkAssistant.mockResolvedValue(UP)
    rerender({ enabled: true })
    await wait(0)
    expect(checkAssistant).toHaveBeenCalledTimes(2)
    expect(result.current).toBe(true)
  })
})
