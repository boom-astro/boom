import { Separator } from "@/components/ui/separator"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { ModeToggle } from "./mode-toggle"
import { Link } from "react-router-dom"
import { SearchDialog, useSearchDialog } from "./search-dialog"
import { Search } from "lucide-react"
import { Button } from "./ui/button"
import { Tooltip, TooltipContent, TooltipTrigger } from "./ui/tooltip"

export function SiteHeader() {
  const { open, setOpen } = useSearchDialog()

  return (
    <header className="flex h-(--header-height) shrink-0 items-center gap-2 border-b transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-(--header-height)">
      <div className="flex w-full items-center gap-1 px-4 lg:gap-2 lg:px-6">
        <SidebarTrigger className="-ml-1" />
        <Separator
          orientation="vertical"
          className="mx-2 data-[orientation=vertical]:h-4"
        />
        <h1 className="text-base font-medium">
          <Link to="/" className="text-base font-medium">Babamul</Link>
        </h1>
        <div className="ml-auto flex items-center gap-2">
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setOpen(true)}
                className="-ml-1"
              >
                <Search className="h-[1.2rem] w-[1.2rem]" />
                <span className="sr-only">Search objects</span>
              </Button>
            </TooltipTrigger>
            <TooltipContent>
              <p>Search objects (Ctrl+K)</p>
            </TooltipContent>
          </Tooltip>
          {/* <Button variant="ghost" asChild size="sm" className="hidden sm:flex">
            <a
              href="https://github.com/shadcn-ui/ui/tree/main/apps/v4/app/(examples)/dashboard"
              rel="noopener noreferrer"
              target="_blank"
              className="dark:text-foreground"
            >
              GitHub
            </a>
          </Button> */}
          <ModeToggle className="-ml-1" />
          {/* Top-bar user avatar removed — NavUser in sidebar provides account actions */}
        </div>
      </div>
      <SearchDialog open={open} onOpenChange={setOpen} />
    </header>
  )
}
