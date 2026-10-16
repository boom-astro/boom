import { type Icon } from "@tabler/icons-react"
import { Link } from "react-router-dom"

import { cn } from "@/lib/utils"
import {
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar"

/**
 * The count of things needing attention on an item.
 *
 * Decorative to assistive technology: it is a number capped at "9+" with no
 * context, and the sr-only text beside the label carries the reason. An
 * `aria-label` here would not have worked anyway -- ARIA prohibits it on a
 * plain `span`, whose implicit role is `generic`, so it was ignored and the
 * link named itself from its text content instead.
 */
function CountDot({
  count,
  reason,
  inline,
}: {
  count: number
  reason: string
  inline?: boolean
}) {
  return (
    <span
      aria-hidden="true"
      title={reason}
      className={cn(
        `flex h-3.5 min-w-3.5 items-center justify-center rounded-full
         bg-destructive px-[3px] text-[9px] font-semibold leading-none
         text-white ring-1 ring-sidebar`,
        inline ? "ml-auto" : "absolute -right-1 -top-1",
      )}
    >
      {count > 9 ? "9+" : count}
    </span>
  )
}

export function NavMain({
  items,
}: {
  items: {
    title: string
    url: string
    icon?: Icon
    /** Count of things needing attention. Omitted or 0 renders nothing. */
    badge?: number
    /** Why the badge is there. Falls back to the title and the count. */
    badgeLabel?: string
  }[]
}) {
  return (
    <SidebarGroup>
      <SidebarGroupContent className="flex flex-col gap-2">
        <SidebarMenu>
          {items.map((item) => {
            const count = item.badge ?? 0
            // `||` rather than `??`, so an empty label falls back too. A bare
            // count says nothing about what needs attention, and the visible
            // text caps at "9+", so the reason has to be carried separately
            // even when the caller supplied none.
            const reason = item.badgeLabel || `${item.title}: ${count}`
            return (
              <SidebarMenuItem key={item.title}>
                <SidebarMenuButton asChild tooltip={item.title}>
                  <Link to={item.url} className="flex items-center gap-2">
                    {/* The icon is wrapped so the dot can sit on it: when the
                        sidebar is collapsed the label is hidden and the icon is
                        all that remains, so the badge has to ride the icon
                        rather than the row.

                        Two constraints from SidebarMenuButton, both easy to
                        trip: it sets `overflow-hidden`, so the badge must stay
                        inside the button's box; and it sizes icons with
                        `[&>svg]:size-4`, a direct-child selector that this
                        wrapper defeats -- hence sizing the icon explicitly. */}
                    {item.icon && (
                      <span className="relative flex size-4 shrink-0 items-center justify-center">
                        <item.icon className="size-4 shrink-0" />
                        {!!count && <CountDot count={count} reason={reason} />}
                      </span>
                    )}
                    <span>{item.title}</span>
                    {/* An item with no icon has nothing for the dot to ride, so
                        it sits after the label rather than not rendering at
                        all -- which is what used to happen, the badge being
                        nested inside the icon's own condition. */}
                    {!item.icon && !!count && (
                      <CountDot count={count} reason={reason} inline />
                    )}
                    {/* Read after the title, so a screen reader gets "Admin, 2
                        catalogs missing" rather than the reason first and the
                        name of the thing it is about second. */}
                    {!!count && <span className="sr-only">{reason}</span>}
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            )
          })}
        </SidebarMenu>
      </SidebarGroupContent>
    </SidebarGroup>
  )
}
