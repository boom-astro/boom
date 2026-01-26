import * as React from "react"
import { Link } from "react-router-dom"
import {
  IconDatabase,
  IconHelp,
  IconSearch,
  IconBinaryTree,
} from "@tabler/icons-react"

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
  ],
  navSecondary: [
    {
      title: "Get Help",
      url: "#",
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
    }
  ],
}

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
  return (
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
              <SidebarMenuButton
              asChild
              className="data-[slot=sidebar-menu-button]:!p-1.5"
            >
              <Link to="/" className="flex items-center gap-2">
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
        <NavMain items={data.navMain} />
        <SidebarSeparator />
        <NavDocuments items={data.documentation} />
        <SidebarSeparator />
        <NavSecondary items={data.navSecondary} className="mt-auto" />
      </SidebarContent>
      <SidebarFooter>
        <NavUser />
      </SidebarFooter>
    </Sidebar>
  )
}
