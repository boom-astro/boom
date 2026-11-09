import { create } from "zustand"
import { getTokenRecord } from "@/lib/api"
import {
  AssistantError,
  applyEvent,
  buildHistory,
  finishTools,
  parseSavedTurns,
  streamChat,
  trimImages,
  type AssistantTurn,
  type ChatContext,
  type Turn,
  type UserTurn,
} from "@/lib/assistant"

const STORAGE_PREFIX = "babamul-assistant:"
// Text arrives a few tokens at a time; re-rendering markdown for each is wasted work.
const TEXT_FLUSH_MS = 50
// A plot is about 60 KB of base64 and a site gets about 5 MB of sessionStorage.
// Keep the newest plots within this budget, and less if even that does not fit.
const IMAGE_BUDGETS = [2_000_000, 500_000, 0]

type AssistantState = {
  /** Whose conversation this is; each account keeps its own. */
  user: string | null
  turns: Turn[]
  running: boolean
  /** The server's latest `status` message, cleared once the answer moves on. */
  status: string | null
  setUser: (user: string | null) => void
  ask: (question: string, context?: ChatContext) => void
  retry: () => void
  stop: () => void
  reset: () => void
}

// Lives outside the store so a reset can tell a stale stream from the current one.
let current: AbortController | null = null
let nextId = 0

const newId = () => `${Date.now().toString(36)}-${nextId++}`

function storageKey(user: string) {
  return STORAGE_PREFIX + user
}

function load(user: string | null): Turn[] {
  if (!user) return []
  try {
    return parseSavedTurns(sessionStorage.getItem(storageKey(user)))
  } catch {
    return []
  }
}

function save(user: string | null, turns: Turn[]) {
  if (!user) return
  if (turns.length === 0) {
    try {
      sessionStorage.removeItem(storageKey(user))
    } catch {
      // Blocked; there is nothing to keep anyway.
    }
    return
  }
  // Images are most of the size, so older ones go first when storage is full.
  for (const budget of IMAGE_BUDGETS) {
    try {
      sessionStorage.setItem(storageKey(user), JSON.stringify(trimImages(turns, budget)))
      return
    } catch {
      // Full or blocked; try with fewer images. The conversation still lives in memory.
    }
  }
}

/** Ends a running answer and says why, so the account that asked sees it next time. */
function endRunning(turns: Turn[], message: string): Turn[] {
  return turns.map((t): Turn =>
    t.role === "assistant" && t.state === "running"
      ? { ...finishTools(t), state: "error", error: { kind: "aborted", message } }
      : t,
  )
}

function cancelCurrent() {
  current?.abort()
  current = null
}

export const useAssistantStore = create<AssistantState>()((set, get) => ({
  user: null,
  turns: [],
  running: false,
  status: null,

  setUser: (user) => {
    const { user: previous, turns, running } = get()
    if (previous === user) return
    // The answer cannot carry on for someone else, but it should not vanish without a word.
    if (running) {
      const why = user ? "Stopped because you switched accounts." : "Stopped because you logged out."
      save(previous, endRunning(turns, why))
    }
    cancelCurrent()
    set({ user, turns: load(user), running: false, status: null })
  },

  ask: (question, context) => {
    const content = question.trim()
    const { turns, running, user } = get()
    if (!content || running) return

    const userTurn: UserTurn = { id: newId(), role: "user", content, context }
    const answer: AssistantTurn = { id: newId(), role: "assistant", parts: [], content: "", state: "running" }
    const token = getTokenRecord()
    if (!token) {
      const expired = new AssistantError("auth")
      const failed: AssistantTurn = { ...answer, state: "error", error: { kind: expired.kind, message: expired.message } }
      set({ turns: [...turns, userTurn, failed] })
      return
    }

    const messages = buildHistory(turns, content)
    const controller = new AbortController()
    current = controller
    set({ turns: [...turns, userTurn, answer], running: true, status: null })

    const update = (change: (turn: AssistantTurn) => AssistantTurn, status: string | null = null) => {
      if (current !== controller) return
      set((s) => ({
        status,
        turns: s.turns.map((t) => (t.id === answer.id && t.role === "assistant" ? change(t) : t)),
      }))
    }

    let pendingText = ""
    let timer: ReturnType<typeof setTimeout> | null = null
    const flushText = () => {
      if (timer) clearTimeout(timer)
      timer = null
      if (!pendingText) return
      const delta = pendingText
      pendingText = ""
      update((t) => applyEvent(t, { type: "text", delta }))
    }

    streamChat(
      { messages, context },
      token.access_token,
      (event) => {
        if (event.type === "text") {
          pendingText += event.delta
          timer ??= setTimeout(flushText, TEXT_FLUSH_MS)
          return
        }
        flushText()
        if (event.type === "status") {
          update((t) => t, event.message || null)
          return
        }
        update((t) => applyEvent(t, event))
      },
      controller.signal,
    )
      .catch((err: unknown) => {
        flushText()
        const error = err instanceof AssistantError ? err : new AssistantError("unavailable")
        update((t) =>
          error.kind === "aborted"
            ? { ...finishTools(t), state: "stopped" }
            : { ...finishTools(t), state: "error", error: { kind: error.kind, message: error.message } },
        )
      })
      .finally(() => {
        if (timer) clearTimeout(timer)
        if (current !== controller) return
        current = null
        set({ running: false, status: null })
        save(get().user, get().turns)
      })

    save(user, get().turns)
  },

  retry: () => {
    const { turns, running, ask } = get()
    const answer = turns[turns.length - 1]
    const question = turns[turns.length - 2]
    if (running || answer?.role !== "assistant" || answer.state !== "error" || question?.role !== "user") return
    set({ turns: turns.slice(0, -2) })
    ask(question.content, question.context)
  },

  stop: () => {
    current?.abort()
  },

  reset: () => {
    cancelCurrent()
    set({ turns: [], running: false, status: null })
    save(get().user, [])
  },
}))
