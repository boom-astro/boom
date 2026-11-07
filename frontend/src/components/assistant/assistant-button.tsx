import { useCallback, useEffect, useState } from "react"
import { Sparkles } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Sheet, SheetTrigger } from "@/components/ui/sheet"
import { getTokenRecord } from "@/lib/api"
import useAppStore from "@/lib/store"
import { AssistantPanel } from "./assistant-panel"
import { loadMarkdown } from "./load-markdown"
import { useAssistantStore } from "./store"
import { useAssistantAvailable } from "./use-assistant-health"

const preload = () => {
  loadMarkdown().catch(() => {})
}

/** The header's "Ask" button and the panel it opens. Shown only to a signed-in user when the service is up. */
export function AssistantButton() {
  const profile = useAppStore((s) => s.profile)
  const user = profile && getTokenRecord() ? profile.id || profile.username || profile.email : null
  const available = useAssistantAvailable(!!user)
  const setUser = useAssistantStore((s) => s.setUser)
  const running = useAssistantStore((s) => s.running)
  const [open, setOpen] = useState(false)
  const close = useCallback(() => setOpen(false), [])

  useEffect(() => {
    setUser(user)
  }, [user, setUser])

  if (!available) return null

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          aria-label="Ask Babamul"
          onPointerEnter={preload}
          onPointerDown={preload}
          onFocus={preload}
          className="relative"
        >
          <Sparkles />
          <span className="hidden sm:inline">Ask</span>
          {running && !open && (
            <span
              aria-hidden
              className="absolute -top-1 -right-1 size-2.5 animate-pulse rounded-full bg-primary ring-2 ring-background"
            />
          )}
        </Button>
      </SheetTrigger>
      <AssistantPanel onNavigate={close} />
    </Sheet>
  )
}
