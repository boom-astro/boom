import { type Icon } from "@tabler/icons-react"
import { Link } from "react-router-dom"

import {
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar"

export function NavMain({
  items,
}: {
  items: {
    title: string
    url: string
    icon?: Icon
    /** Count of things needing attention. Omitted or 0 renders nothing. */
    badge?: number
    /** Why the badge is there, for the title attribute. */
    badgeLabel?: string
  }[]
}) {
  return (
    <SidebarGroup>
      <SidebarGroupContent className="flex flex-col gap-2">
        <SidebarMenu>
          {items.map((item) => (
            <SidebarMenuItem key={item.title}>
              <SidebarMenuButton asChild tooltip={item.title}>
                <Link to={item.url} className="flex items-center gap-2">
                  {/* The icon is wrapped so the dot can sit on it: when the
                      sidebar is collapsed the label is hidden and the icon is
                      all that remains, so the badge has to ride the icon rather
                      than the row.

                      Two constraints from SidebarMenuButton, both easy to trip:
                      it sets `overflow-hidden`, so the badge must stay inside
                      the button's box; and it sizes icons with `[&>svg]:size-4`,
                      a direct-child selector that this wrapper defeats -- hence
                      sizing the icon explicitly here. */}
                  {item.icon && (
                    <span className="relative flex size-4 shrink-0 items-center justify-center">
                      <item.icon className="size-4 shrink-0" />
                      {!!item.badge && (
                        <span
                          title={item.badgeLabel}
                          aria-label={item.badgeLabel}
                          className="absolute -right-1 -top-1 flex h-3.5 min-w-3.5 items-center
                                     justify-center rounded-full bg-destructive px-[3px]
                                     text-[9px] font-semibold leading-none text-white
                                     ring-1 ring-sidebar"
                        >
                          {item.badge > 9 ? "9+" : item.badge}
                        </span>
                      )}
                    </span>
                  )}
                  <span>{item.title}</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
      </SidebarGroupContent>
    </SidebarGroup>
  )
}
