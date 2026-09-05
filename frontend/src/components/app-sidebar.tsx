import * as React from "react"
import { Link } from "react-router-dom"
import {
  IconChartBar,
  IconDatabase,
  IconHelp,
  IconSearch,
  IconBinaryTree,
  IconPackage,
  IconNotebook,
  IconBook,
  IconSettings,
} from "@tabler/icons-react"

import useAppStore from "@/lib/store"
import { useCatalogIssues } from "@/hooks/use-catalog-issues"
import { NavDocuments } from "@/components/nav-documents"
import { NavMain } from "@/components/nav-main"
import { NavSecondary } from "@/components/nav-secondary"
import { NavUser } from "@/components/nav-user"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarSeparator,
} from "@/components/ui/sidebar"

const data = {
  navMain: [
    {
      title: "Query",
      url: "/query",
      icon: IconSearch,
    },
    {
      title: "Dashboard",
      url: "/dashboard",
      icon: IconChartBar,
    },
  ],
  navSecondary: [
    {
      title: "Acknowledgments",
      url: "/acknowledgments",
      icon: IconBook,
    },
    {
      title: "Get Help",
      url: "/help",
      icon: IconHelp,
    },
  ],
  documentation: [
    {
      name: "Kafka Documentation",
      url: "/docs/kafka",
      icon: IconBinaryTree,
    },
    {
      name: "API Documentation",
      url: "/docs/api",
      icon: IconDatabase,
    },
  ],
  tools: [
    {
      name: "Python Client (PyPI)",
      url: "https://pypi.org/project/babamul/",
      icon: IconPackage,
      external: true,
    },
    {
      name: "Notebook Examples",
      url: "https://github.com/boom-astro/babamul/tree/main/examples",
      icon: IconNotebook,
      external: true,
    },
  ],
}

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
  const profile = useAppStore((s) => s.profile)
  // Only polled for admins; for anyone else the hook is inert.
  const issues = useCatalogIssues(!!profile?.is_admin)
  // Shown only to admins, but the page and every route behind it are enforced
  // server-side -- this just avoids offering a link that would 403.
  const navMain = profile?.is_admin
    ? [
        ...data.navMain,
        {
          title: "Admin",
          url: "/admin",
          icon: IconSettings,
          badge: issues.count,
          badgeLabel: issues.label,
        },
      ]
    : data.navMain

  return (
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              asChild
              className="data-[slot=sidebar-menu-button]:p-1.5!"
            >
              <Link to="/" state={{ explicit: true }} className="flex items-center gap-2">
                {/* Collapsed version - stacked */}
                <div className="hidden group-data-[collapsible=icon]:flex flex-col items-center justify-center leading-[1.1] text-md -ml-1">
                  <span>𒁀𒁀</span>
                  <span>𒀯</span>
                </div>
                {/* Expanded version - horizontal */}
                <span className="group-data-[collapsible=icon]:hidden">𒁀𒁀𒀯</span>
                <span className="text-base font-semibold">Babamul</span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <NavMain items={navMain} />
        <SidebarSeparator />
        <NavDocuments items={data.documentation} />
        <SidebarSeparator />
        <NavDocuments items={data.tools} label="Tools" />
        <SidebarSeparator />
        <NavSecondary items={data.navSecondary} className="mt-auto" />
      </SidebarContent>
      <SidebarFooter>
        <NavUser />
      </SidebarFooter>
    </Sidebar>
  )
}
