import { useEffect, useState } from "react"
import { useLocation } from "react-router-dom"
import { checkAssistant, type AssistantHealth } from "@/lib/assistant"

// A deployment without the assistant is asked again after this long, on the
// next navigation, so one that comes up later shows the button without a reload.
const RECHECK_MS = 60_000

let cached: { at: number; result: Promise<AssistantHealth | null> } | null = null

function health(): Promise<AssistantHealth | null> {
  const now = Date.now()
  if (!cached || now - cached.at > RECHECK_MS) {
    const result = checkAssistant()
    cached = { at: now, result }
    // A healthy answer holds for the session.
    result.then((h) => {
      if (h && cached?.result === result) cached.at = Infinity
    })
  }
  return cached.result
}

/** Whether to offer the assistant; asks the service only while `enabled`. */
export function useAssistantAvailable(enabled: boolean): boolean {
  const [available, setAvailable] = useState(false)
  const { pathname } = useLocation()

  useEffect(() => {
    if (!enabled) return
    let active = true
    health().then((h) => {
      if (active) setAvailable(!!h)
    })
    return () => {
      active = false
    }
  }, [enabled, pathname])

  return enabled && available
}
