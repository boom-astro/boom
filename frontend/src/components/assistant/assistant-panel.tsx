import {
  useCallback,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type FormEvent,
  type KeyboardEvent,
} from "react"
import { Link, matchPath, useLocation } from "react-router-dom"
import { ArrowUp, CircleAlert, Crosshair, Loader2, Plus, Square, X } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet"
import { Textarea } from "@/components/ui/textarea"
import type { AssistantTurn, ChatContext } from "@/lib/assistant"
import { cn } from "@/lib/utils"
import { AnswerText, MarkdownRetry } from "./markdown-text"
import { useAssistantStore } from "./store"
import { ToolCall } from "./tool-call"

const GENERAL_EXAMPLES = [
  "Find likely supernovae in last night's ZTF alerts",
  "What is near RA 210.91, Dec 54.31?",
  "What can you look up for me?",
]

const OBJECT_EXAMPLES = [
  "Summarize this object",
  "Show me its light curve and cutouts",
  "Find objects that look like this one",
]

// Within this many pixels of the end, new text keeps the view scrolled down.
const STICK_PX = 48

// On phones, focusing the box opens the keyboard over half the panel, and the
// keyboard has no Shift, so Enter starts a new line and the button sends.
const isTouch = () => window.matchMedia?.("(pointer: coarse)").matches ?? false

/** The assistant's side panel: the conversation and the question box. */
export function AssistantPanel({ onNavigate }: { onNavigate: () => void }) {
  const location = useLocation()
  const { pathname } = location
  const match = matchPath("/objects/:survey/:objectId", pathname)
  const [dismissedFor, setDismissedFor] = useState<string | null>(null)
  const object =
    match?.params.survey && match.params.objectId && dismissedFor !== pathname
      ? { survey: match.params.survey, objectId: match.params.objectId }
      : null

  const turns = useAssistantStore((s) => s.turns)
  const running = useAssistantStore((s) => s.running)
  const status = useAssistantStore((s) => s.status)
  const ask = useAssistantStore((s) => s.ask)
  const stop = useAssistantStore((s) => s.stop)
  const reset = useAssistantStore((s) => s.reset)
  const retry = useAssistantStore((s) => s.retry)

  const [draft, setDraft] = useState("")
  const contentRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLTextAreaElement>(null)
  const listRef = useRef<HTMLDivElement | null>(null)
  const stickToEnd = useRef(true)
  const hintId = useId()

  const scrollToEnd = useCallback(() => {
    const el = listRef.current
    if (el && stickToEnd.current) el.scrollTop = el.scrollHeight
  }, [])

  // The list mounts each time the sheet opens; start at the latest message.
  const setList = useCallback((el: HTMLDivElement | null) => {
    listRef.current = el
    stickToEnd.current = true
    if (el) el.scrollTop = el.scrollHeight
  }, [])

  // Images and lazily rendered markdown change the height after the turns do.
  const observeContent = useCallback(
    (el: HTMLDivElement | null) => {
      if (!el || typeof ResizeObserver === "undefined") return
      const observer = new ResizeObserver(scrollToEnd)
      observer.observe(el)
      return () => observer.disconnect()
    },
    [scrollToEnd],
  )

  useLayoutEffect(() => {
    scrollToEnd()
  }, [turns, status, scrollToEnd])

  const refocus = () => {
    if (isTouch()) contentRef.current?.focus()
    else inputRef.current?.focus()
  }

  const onScroll = () => {
    const el = listRef.current
    if (el) stickToEnd.current = el.scrollHeight - el.scrollTop - el.clientHeight < STICK_PX
  }

  const send = (text: string) => {
    if (!text.trim() || running) return
    // A dismissed chip sends nothing: the object page's path names the object too.
    const context: ChatContext | undefined = object
      ? { path: pathname, survey: object.survey, object_id: object.objectId }
      : match
        ? undefined
        : { path: pathname }
    stickToEnd.current = true
    ask(text, context)
    setDraft("")
  }

  const onSubmit = (e: FormEvent) => {
    e.preventDefault()
    send(draft)
    refocus()
  }

  const onKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing && !isTouch()) {
      e.preventDefault()
      send(draft)
    }
  }

  const newChat = () => {
    reset()
    setDraft("")
    refocus()
  }

  return (
    <SheetContent
      ref={contentRef}
      className="w-full gap-0 sm:max-w-md"
      onOpenAutoFocus={(e) => {
        e.preventDefault()
        refocus()
      }}
    >
      <SheetHeader className="gap-1 border-b">
        <SheetTitle className="pr-8">Ask Babamul</SheetTitle>
        <SheetDescription className="pr-8">
          It looks things up in Babamul for you. It can be wrong, so check what matters.
        </SheetDescription>
        <div className="mt-2 flex min-h-8 items-center gap-2">
          {object && (
            <Badge variant="secondary" className="min-w-0 gap-1 py-0.5 pr-0.5 pl-2 font-normal">
              <Crosshair aria-hidden />
              <span className="truncate" title="Sent with your questions">
                Viewing {object.objectId}
              </span>
              <button
                type="button"
                onClick={() => {
                  setDismissedFor(pathname)
                  refocus()
                }}
                aria-label={`Don't send ${object.objectId} with questions`}
                className="rounded-sm p-0.5 opacity-70 outline-none hover:opacity-100 focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset"
              >
                <X className="size-3" />
              </button>
            </Badge>
          )}
          <Button variant="ghost" size="sm" className="ml-auto" onClick={newChat} disabled={turns.length === 0}>
            <Plus />
            New chat
          </Button>
        </div>
        <MarkdownRetry />
      </SheetHeader>

      <div
        ref={setList}
        onScroll={onScroll}
        role="region"
        aria-label="Conversation"
        tabIndex={0}
        className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-4 py-4 outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:ring-inset"
      >
        <div ref={observeContent} className="flex min-h-full flex-col">
          {turns.length === 0 ? (
            <div className="mt-auto flex flex-col gap-3">
              <p className="text-sm text-muted-foreground">
                Ask about alerts, objects, light curves or cutouts. Each step it takes is shown.
              </p>
              <ul className="flex flex-col gap-2">
                {(object ? OBJECT_EXAMPLES : GENERAL_EXAMPLES).map((q) => (
                  <li key={q}>
                    <Button
                      variant="outline"
                      className="h-auto w-full justify-start py-2 text-left font-normal whitespace-normal"
                      onClick={() => {
                        send(q)
                        refocus()
                      }}
                    >
                      {q}
                    </Button>
                  </li>
                ))}
              </ul>
            </div>
          ) : (
            <div className="flex flex-col gap-4">
              {turns.map((turn, i) =>
                turn.role === "user" ? (
                  <div
                    key={turn.id}
                    className="max-w-[85%] self-end rounded-lg bg-muted px-3 py-2 text-sm break-words whitespace-pre-wrap"
                  >
                    <span className="sr-only">You asked: </span>
                    {turn.content}
                  </div>
                ) : (
                  <Answer
                    key={turn.id}
                    turn={turn}
                    status={status}
                    isLast={i === turns.length - 1}
                    canRetry={i === turns.length - 1 && !running}
                    onRetry={retry}
                    onNavigate={onNavigate}
                    from={location}
                  />
                ),
              )}
            </div>
          )}
        </div>
      </div>

      <form onSubmit={onSubmit} className="border-t p-3 pb-[max(0.75rem,env(safe-area-inset-bottom))]">
        <div className="flex items-end gap-2">
          <Textarea
            ref={inputRef}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={onKeyDown}
            rows={1}
            maxLength={4000}
            placeholder="Ask about alerts or objects"
            aria-label="Question"
            aria-describedby={hintId}
            className="max-h-40 min-h-10 resize-none"
          />
          <Button
            type={running ? "button" : "submit"}
            size="icon"
            variant={running ? "outline" : "default"}
            onClick={
              running
                ? () => {
                    stop()
                    refocus()
                  }
                : undefined
            }
            disabled={!running && !draft.trim()}
            aria-label={running ? "Stop" : "Send"}
            className="size-10"
          >
            {running ? <Square className="fill-current" /> : <ArrowUp />}
          </Button>
        </div>
        <p id={hintId} className="sr-only">
          {isTouch() ? "Enter starts a new line. The Send button sends." : "Enter sends. Shift and Enter starts a new line."}
        </p>
      </form>
    </SheetContent>
  )
}

function activity(turn: AssistantTurn, status: string | null): string {
  if (status) return status
  const last = turn.parts[turn.parts.length - 1]
  if (last?.kind === "tool" && last.step.state === "running") return `Running ${last.step.name}`
  if (last?.kind === "text") return "Writing"
  return "Thinking"
}

function Answer({
  turn,
  status,
  isLast,
  canRetry,
  onRetry,
  onNavigate,
  from,
}: {
  turn: AssistantTurn
  status: string | null
  isLast: boolean
  canRetry: boolean
  onRetry: () => void
  onNavigate: () => void
  from: ReturnType<typeof useLocation>
}) {
  const running = turn.state === "running"
  const stopped = turn.state === "stopped"
  return (
    <div className="flex min-w-0 flex-col gap-3">
      {/* Only the latest answer is live, so older ones re-rendering are not read out again. */}
      <div className="flex min-w-0 flex-col gap-3" aria-live={isLast ? "polite" : undefined} aria-busy={running}>
        <span className="sr-only">Babamul answered:</span>
        {turn.parts.map((part, i) =>
          part.kind === "text" ? (
            <AnswerText key={i} text={part.text} onNavigate={onNavigate} />
          ) : (
            <ToolCall key={i} step={part.step} />
          ),
        )}
      </div>
      {/* Kept under the latest answer once it is done, so the end of the list does not jump.
          Outside the busy region so a queue position is read out while the answer waits. */}
      {(running || stopped || isLast) && (
        <p
          role="status"
          aria-hidden={!running && !stopped}
          className={cn(
            "flex min-h-5 items-center gap-2 text-xs text-muted-foreground",
            !running && !stopped && "invisible",
          )}
        >
          {running && <Loader2 aria-hidden className="size-3.5 animate-spin" />}
          {running ? activity(turn, status) : stopped ? "Stopped." : null}
        </p>
      )}
      {turn.state === "error" && turn.error && (
        <div
          role="alert"
          className="flex flex-wrap items-center gap-x-3 gap-y-2 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive"
        >
          <span className="flex min-w-0 items-start gap-2">
            <CircleAlert aria-hidden className="mt-0.5 size-4 shrink-0" />
            {turn.error.message}
          </span>
          {turn.error.kind === "auth" ? (
            <Button asChild size="sm" variant="outline" className="text-foreground">
              <Link to="/login" state={{ from }} onClick={onNavigate}>
                Log in
              </Link>
            </Button>
          ) : (
            canRetry && (
              <Button size="sm" variant="outline" className="text-foreground" onClick={onRetry}>
                Try again
              </Button>
            )
          )}
        </div>
      )}
    </div>
  )
}
