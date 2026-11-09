import { useId, useMemo, type MouseEvent } from "react"
import ReactMarkdown, { type Components } from "react-markdown"
import remarkGfm from "remark-gfm"
import { Link } from "react-router-dom"
import { inAppObjectPath, remarkObjectLinks } from "@/lib/assistant"
import { cn } from "@/lib/utils"

const remarkPlugins = [remarkGfm, remarkObjectLinks]

const linkClass = "font-medium text-primary underline underline-offset-4 break-all"

// No typography plugin in this app, so markdown elements are styled from here.
const proseClass = [
  "min-w-0 break-words text-sm leading-relaxed",
  "[&>:first-child]:mt-0 [&>:last-child]:mb-0",
  "[&_p]:my-2 [&_ul]:my-2 [&_ul]:list-disc [&_ul]:pl-5 [&_ol]:my-2 [&_ol]:list-decimal [&_ol]:pl-5 [&_li]:my-0.5",
  "[&_blockquote]:my-2 [&_blockquote]:border-l-2 [&_blockquote]:pl-3 [&_blockquote]:text-muted-foreground",
  "[&_code]:rounded [&_code]:bg-muted [&_code]:px-1 [&_code]:py-0.5 [&_code]:font-mono [&_code]:text-[0.85em]",
  "[&_pre_code]:bg-transparent [&_pre_code]:p-0",
  "[&_hr]:my-3",
].join(" ")

type HeadingTag = "h3" | "h4" | "h5" | "h6"

// The panel's title is an h2, so the model's headings start one level below it.
function heading(Tag: HeadingTag, style: string): Components["h1"] {
  // `id` and `className` carry the footnotes' screen reader label.
  return function Heading({ id, className, children }) {
    return (
      <Tag id={id} className={cn(style, className)}>
        {children}
      </Tag>
    )
  }
}

const headings: Components = {
  h1: heading("h3", "mt-3 mb-2 text-base font-semibold"),
  h2: heading("h4", "mt-3 mb-2 text-sm font-semibold"),
  h3: heading("h5", "mt-3 mb-1 text-sm font-semibold"),
  h4: heading("h6", "mt-3 mb-1 text-sm font-semibold"),
  h5: heading("h6", "mt-3 mb-1 text-sm font-semibold"),
  h6: heading("h6", "mt-3 mb-1 text-sm font-semibold"),
}

// Footnotes jump within the panel without touching the page's URL.
function scrollToTarget(e: MouseEvent<HTMLAnchorElement>, id: string) {
  e.preventDefault()
  document.getElementById(id)?.scrollIntoView?.({ block: "nearest" })
}

/** An answer rendered as markdown, with object pages opened in the app. */
export default function AssistantMarkdown({ text, onNavigate }: { text: string; onNavigate?: () => void }) {
  // Footnote ids are unique per answer, so one answer's links do not land in another's notes.
  const idPrefix = `${useId()}-`
  const components = useMemo<Components>(
    () => ({
      ...headings,
      a: ({ href, children, id, "aria-describedby": describedBy, "aria-label": label }) => {
        // react-markdown empties javascript: and data: links; a dead link reads as text.
        if (!href) return <span>{children}</span>
        if (href.startsWith("#")) {
          // `id` and the labels let footnote references and back links find each other.
          return (
            <a
              href={href}
              id={id}
              aria-describedby={describedBy}
              aria-label={label}
              onClick={(e) => scrollToTarget(e, href.slice(1))}
              className={linkClass}
            >
              {children}
            </a>
          )
        }
        const path = inAppObjectPath(href)
        if (path) {
          const onClick = (e: MouseEvent) => {
            // Leave the panel open when the link opens in another tab.
            if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return
            onNavigate?.()
          }
          return (
            <Link to={path} onClick={onClick} className={linkClass}>
              {children}
            </Link>
          )
        }
        return (
          <a href={href} target="_blank" rel="noopener noreferrer" className={linkClass}>
            {children}
          </a>
        )
      },
      pre: ({ children }) => (
        <pre className="my-2 max-w-full overflow-x-auto rounded-md bg-muted p-2 text-xs">{children}</pre>
      ),
      table: ({ children }) => (
        <div className="my-2 max-w-full overflow-x-auto rounded-md border">
          <table className="w-full border-collapse text-xs">{children}</table>
        </div>
      ),
      th: ({ children, style }) => (
        <th style={style} className="border-b bg-muted/50 px-2 py-1 text-left font-medium whitespace-nowrap">
          {children}
        </th>
      ),
      td: ({ children, style }) => (
        <td style={style} className="border-b px-2 py-1 align-top">
          {children}
        </td>
      ),
    }),
    [onNavigate],
  )

  return (
    <div className={proseClass}>
      {/* Tool images are shown with their tool call; images the model links to are not loaded. */}
      <ReactMarkdown
        remarkPlugins={remarkPlugins}
        remarkRehypeOptions={{ clobberPrefix: idPrefix }}
        components={components}
        disallowedElements={["img"]}
        unwrapDisallowed
      >
        {text}
      </ReactMarkdown>
    </div>
  )
}
