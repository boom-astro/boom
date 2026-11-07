import { useMemo, type MouseEvent } from "react"
import ReactMarkdown, { type Components } from "react-markdown"
import remarkGfm from "remark-gfm"
import { Link } from "react-router-dom"
import { inAppObjectPath, remarkObjectLinks } from "@/lib/assistant"

const remarkPlugins = [remarkGfm, remarkObjectLinks]

const linkClass = "font-medium text-primary underline underline-offset-4 break-all"

// No typography plugin in this app, so markdown elements are styled from here.
const proseClass = [
  "min-w-0 break-words text-sm leading-relaxed",
  "[&>:first-child]:mt-0 [&>:last-child]:mb-0",
  "[&_p]:my-2 [&_ul]:my-2 [&_ul]:list-disc [&_ul]:pl-5 [&_ol]:my-2 [&_ol]:list-decimal [&_ol]:pl-5 [&_li]:my-0.5",
  "[&_h1]:mt-3 [&_h1]:mb-2 [&_h1]:text-base [&_h1]:font-semibold",
  "[&_h2]:mt-3 [&_h2]:mb-2 [&_h2]:text-sm [&_h2]:font-semibold",
  "[&_h3]:mt-3 [&_h3]:mb-1 [&_h3]:text-sm [&_h3]:font-semibold",
  "[&_blockquote]:my-2 [&_blockquote]:border-l-2 [&_blockquote]:pl-3 [&_blockquote]:text-muted-foreground",
  "[&_code]:rounded [&_code]:bg-muted [&_code]:px-1 [&_code]:py-0.5 [&_code]:font-mono [&_code]:text-[0.85em]",
  "[&_pre_code]:bg-transparent [&_pre_code]:p-0",
  "[&_hr]:my-3",
].join(" ")

/** An answer rendered as markdown, with object pages opened in the app. */
export default function AssistantMarkdown({ text, onNavigate }: { text: string; onNavigate?: () => void }) {
  const components = useMemo<Components>(
    () => ({
      a: ({ href, children }) => {
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
        components={components}
        disallowedElements={["img"]}
        unwrapDisallowed
      >
        {text}
      </ReactMarkdown>
    </div>
  )
}
