import { useId, useState } from "react"
import { ChevronDown, CircleAlert, CircleCheck, Loader2 } from "lucide-react"
import { Dialog, DialogContent, DialogDescription, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { imageSrc, type ToolImage, type ToolStep } from "@/lib/assistant"
import { cn } from "@/lib/utils"

const STATE_LABEL: Record<ToolStep["state"], string> = {
  running: "running",
  ok: "done",
  failed: "failed",
}

/** The first couple of plain arguments, enough to tell two calls apart. */
function argSummary(args: Record<string, unknown>): string {
  return Object.entries(args)
    .filter(([, v]) => ["string", "number", "boolean"].includes(typeof v))
    .slice(0, 2)
    .map(([k, v]) => `${k}: ${v}`)
    .join(", ")
}

function StateIcon({ state }: { state: ToolStep["state"] }) {
  if (state === "running") return <Loader2 aria-hidden className="size-3.5 shrink-0 animate-spin" />
  if (state === "ok") return <CircleCheck aria-hidden className="size-3.5 shrink-0 text-emerald-600 dark:text-emerald-400" />
  return <CircleAlert aria-hidden className="size-3.5 shrink-0 text-destructive" />
}

/** One tool call: a chip that expands to its arguments and result, then any images it returned. */
export function ToolCall({ step }: { step: ToolStep }) {
  const [open, setOpen] = useState(false)
  const detailsId = useId()
  const summary = argSummary(step.arguments)
  const args = Object.keys(step.arguments).length > 0 ? JSON.stringify(step.arguments, null, 2) : null

  return (
    <div className="flex flex-col items-start gap-2">
      <button
        type="button"
        aria-expanded={open}
        aria-controls={detailsId}
        onClick={() => setOpen((v) => !v)}
        className="inline-flex h-7 max-w-full items-center gap-1.5 rounded-md border bg-muted/40 px-2 text-xs text-muted-foreground transition-colors outline-none hover:bg-accent hover:text-accent-foreground focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"
      >
        <StateIcon state={step.state} />
        <span className="shrink-0 font-mono font-medium text-foreground">{step.name}</span>
        {summary && <span className="min-w-0 truncate">{summary}</span>}
        <span className="sr-only">, {STATE_LABEL[step.state]}</span>
        <ChevronDown aria-hidden className={cn("size-3 shrink-0 transition-transform", open && "rotate-180")} />
      </button>
      <div
        id={detailsId}
        hidden={!open}
        className="w-full space-y-2 rounded-md border bg-muted/30 p-2 text-xs"
      >
        <p className="break-words whitespace-pre-wrap">
          {step.summary || (step.state === "running" ? "Still running." : "No result.")}
        </p>
        {args && (
          <pre className="max-h-48 overflow-auto font-mono text-[11px] break-all whitespace-pre-wrap text-muted-foreground">
            {args}
          </pre>
        )}
      </div>
      {step.images.map((image, i) => (
        <ToolImageView key={i} image={image} label={`${step.name} image ${i + 1}`} />
      ))}
    </div>
  )
}

function ToolImageView({ image, label }: { image: ToolImage; label: string }) {
  const src = imageSrc(image)
  return (
    <Dialog>
      <DialogTrigger asChild>
        <button
          type="button"
          aria-label={`Enlarge ${label}`}
          className="block max-w-full cursor-zoom-in overflow-hidden rounded-md border bg-muted outline-none focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50"
        >
          <img src={src} alt={label} className="block h-auto max-w-full" />
        </button>
      </DialogTrigger>
      <DialogContent className="max-w-[calc(100%-1rem)] p-2 pt-10 sm:max-w-4xl">
        <DialogTitle className="sr-only">{label}</DialogTitle>
        <DialogDescription className="sr-only">The image at full size.</DialogDescription>
        <img src={src} alt={label} className="block h-auto max-h-[80vh] w-full object-contain" />
      </DialogContent>
    </Dialog>
  )
}
