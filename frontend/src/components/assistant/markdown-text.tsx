import { Component, Suspense, lazy, useSyncExternalStore, type ReactNode } from "react"
import { RotateCw } from "lucide-react"
import { Button } from "@/components/ui/button"
import { loadMarkdown } from "./load-markdown"

// React.lazy keeps a failed import for good, so trying again needs a new one.
// The chunk fails to load after a redeploy renames it, or on a flaky network.
let Markdown = lazy(loadMarkdown)
let attempt = 0
let failed = false
const listeners = new Set<() => void>()

function subscribe(listener: () => void) {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

function notify() {
  for (const listener of listeners) listener()
}

function markFailed() {
  if (failed) return
  failed = true
  notify()
}

function retry() {
  Markdown = lazy(loadMarkdown)
  attempt++
  failed = false
  notify()
}

/** Shows `fallback` instead of letting a markdown failure unmount the whole app. */
class MarkdownBoundary extends Component<{ fallback: ReactNode; children: ReactNode }, { error: boolean }> {
  state = { error: false }

  static getDerivedStateFromError() {
    return { error: true }
  }

  componentDidCatch() {
    markFailed()
  }

  render() {
    return this.state.error ? this.props.fallback : this.props.children
  }
}

/** One stretch of an answer: markdown once it has loaded, plain text until then or if it cannot. */
export function AnswerText({ text, onNavigate }: { text: string; onNavigate?: () => void }) {
  const current = useSyncExternalStore(subscribe, () => attempt)
  const plain = <p className="text-sm break-words whitespace-pre-wrap">{text}</p>
  return (
    <MarkdownBoundary key={current} fallback={plain}>
      <Suspense fallback={plain}>
        <Markdown text={text} onNavigate={onNavigate} />
      </Suspense>
    </MarkdownBoundary>
  )
}

/** Says once, for the whole panel, that answers are plain text, and offers to load formatting again. */
export function MarkdownRetry() {
  const show = useSyncExternalStore(subscribe, () => failed)
  const retried = useSyncExternalStore(subscribe, () => attempt > 0)
  if (!show) return null
  // A chunk renamed by a redeploy only comes back with a reload; the chat is kept in sessionStorage.
  return (
    <div role="status" className="flex items-center gap-2 text-xs text-muted-foreground">
      <span className="min-w-0">Formatting did not load, so answers show as plain text.</span>
      <Button
        variant="ghost"
        size="sm"
        className="ml-auto h-7 shrink-0"
        onClick={retried ? () => window.location.reload() : retry}
      >
        <RotateCw />
        {retried ? "Reload page" : "Retry"}
      </Button>
    </div>
  )
}
