import { IconDotsVertical, IconLogout, IconUserCircle } from "@tabler/icons-react"

import { useEffect } from "react";
import { useNavigate } from "react-router-dom";

import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar"
import { DropdownMenu, DropdownMenuContent, DropdownMenuGroup, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { SidebarMenu, SidebarMenuButton, SidebarMenuItem, useSidebar } from "@/components/ui/sidebar"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import * as analytics from "@/lib/analytics"
import api, { TOKEN_KEY, USERNAME_KEY, type Profile } from "@/lib/api"
import useAppStore, { ensureProfileLoaded } from "@/lib/store"
import { cn } from "@/lib/utils"

function ProfileAvatar({ profile, className }: { profile: Profile; className?: string }) {
  return (
    <Avatar className={cn("h-8 w-8 rounded-lg", className)}>
      {profile?.avatar ? (
        <AvatarImage src={profile.avatar} alt={profile?.username} />
      ) : (
        <AvatarFallback className="rounded-lg">{profile?.username?.[0]?.toUpperCase() ?? 'U'}</AvatarFallback>
      )}
    </Avatar>
  )
}

function ProfileLabel({ profile }: { profile: Profile }) {
  return (
    <div className="grid flex-1 text-left text-sm leading-tight">
      <span className="truncate font-medium">{profile?.username}</span>
      <span className="text-muted-foreground truncate text-xs">{profile?.email}</span>
    </div>
  )
}

export function NavUser() {
  const { isMobile, state } = useSidebar()
  const navigate = useNavigate()

  const profile = useAppStore((s) => s.profile)
  const clearProfile = useAppStore((s) => s.clearProfile)
  const authenticated = !!profile?.username || !!api.getTokenRecord()

  useEffect(() => {
    function onStorage(e: StorageEvent) {
      if (e.key !== null && e.key !== TOKEN_KEY && e.key !== USERNAME_KEY) return
      if (!api.getTokenRecord()) clearProfile()
    }
    window.addEventListener('storage', onStorage)
    return () => window.removeEventListener('storage', onStorage)
  }, [clearProfile])

  useEffect(() => {
    if (!authenticated) return
    ensureProfileLoaded().catch((err) => console.error('nav-user: ensureProfileLoaded failed', err))
  }, [authenticated])

  function handleLogout() {
    api.logout()
    // Not in api.logout(): that also runs on every 401, where a reset mints a new anonymous person.
    analytics.resetUser()
    clearProfile()
    navigate('/')
  }

  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <Tooltip>
            <DropdownMenuTrigger asChild>
              <TooltipTrigger asChild>
                <SidebarMenuButton
                  size="lg"
                  className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
                >
                  <ProfileAvatar profile={profile} className="grayscale" />
                  <ProfileLabel profile={profile} />
                  <IconDotsVertical className="ml-auto size-4" />
                </SidebarMenuButton>
              </TooltipTrigger>
            </DropdownMenuTrigger>
            <TooltipContent
              side="right"
              align="center"
              hidden={state !== "collapsed" || isMobile}
            >
              {profile?.username}
            </TooltipContent>
          </Tooltip>
          <DropdownMenuContent
            className="w-(--radix-dropdown-menu-trigger-width) min-w-56 rounded-lg"
            side={isMobile ? 'bottom' : 'right'}
            align="end"
            sideOffset={4}
          >
            <DropdownMenuLabel className="p-0 font-normal">
              <div className="flex items-center gap-2 px-1 py-1.5 text-left text-sm">
                <ProfileAvatar profile={profile} />
                <ProfileLabel profile={profile} />
              </div>
            </DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuGroup>
              <DropdownMenuItem onSelect={() => navigate('/profile')}>
                <IconUserCircle />
                Profile
              </DropdownMenuItem>
            </DropdownMenuGroup>
            <DropdownMenuSeparator />
            {authenticated ? (
              <DropdownMenuItem onSelect={handleLogout}>
                <IconLogout />
                Log out
              </DropdownMenuItem>
            ) : (
              <DropdownMenuItem onSelect={() => navigate('/login')}>Sign in</DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      </SidebarMenuItem>
    </SidebarMenu>
  )
}
