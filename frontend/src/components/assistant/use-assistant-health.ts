import { useEffect, useState } from "react"
import { checkAssistant } from "@/lib/assistant"

// While the assistant does not answer, ask again after 5 s, then 10, 20, 40 and
// once a minute after that, so a service that comes up later shows the button
// without a reload. Coming back to the tab or window asks again at once.
const FIRST_RETRY_MS = 5_000
const MAX_RETRY_MS = 60_000

// A healthy answer holds for the page's life. If the service dies later, each
// question says so, which beats the panel vanishing in the middle of a chat.
let healthy = false
let pending: Promise<boolean> | null = null

function check(): Promise<boolean> {
  if (healthy) return Promise.resolve(true)
  // One request at a time, however many callers ask.
  pending ??= checkAssistant().then((h) => {
    pending = null
    healthy = !!h
    return healthy
  })
  return pending
}

/** Whether to offer the assistant; asks the service only while `enabled`, so again on login. */
export function useAssistantAvailable(enabled: boolean): boolean {
  const [available, setAvailable] = useState(healthy)

  useEffect(() => {
    if (!enabled) return
    let active = true
    let checking = false
    let timer: ReturnType<typeof setTimeout> | undefined
    let delay = FIRST_RETRY_MS

    const run = () => {
      clearTimeout(timer)
      timer = undefined
      // A background tab waits for the user to come back instead of polling.
      if (checking || document.visibilityState === "hidden") return
      checking = true
      check().then((ok) => {
        checking = false
        if (!active) return
        setAvailable(ok)
        if (ok) return
        timer = setTimeout(run, delay)
        delay = Math.min(delay * 2, MAX_RETRY_MS)
      })
    }

    const onReturn = () => {
      if (healthy || document.visibilityState === "hidden") return
      delay = FIRST_RETRY_MS
      run()
    }

    run()
    window.addEventListener("focus", onReturn)
    document.addEventListener("visibilitychange", onReturn)
    return () => {
      active = false
      clearTimeout(timer)
      window.removeEventListener("focus", onReturn)
      document.removeEventListener("visibilitychange", onReturn)
    }
  }, [enabled])

  return enabled && available
}
