import { IconDotsVertical, IconLogout, IconUserCircle } from "@tabler/icons-react"

import { useEffect } from "react";
import { useNavigate } from "react-router-dom";

import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar"
import { DropdownMenu, DropdownMenuContent, DropdownMenuGroup, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { SidebarMenu, SidebarMenuButton, SidebarMenuItem, useSidebar } from "@/components/ui/sidebar"
import api from "@/lib/api"
import useAppStore, { ensureProfileLoaded } from "@/lib/store"

export function NavUser() {
  const { isMobile } = useSidebar()
  const navigate = useNavigate()

  const profile = useAppStore((s) => s.profile)
  
  const clearProfile = useAppStore((s) => s.clearProfile)
  const authenticatedLocal = !!api.getTokenRecord()
  const authenticated = !!profile?.username || authenticatedLocal

  useEffect(() => {
    function onStorage(e: StorageEvent) {
      if (e.key === null || e.key === 'api_token' || e.key === 'api_user') {
        const hasToken = !!api.getTokenRecord()
        if (!hasToken) clearProfile()
      }
    }
    window.addEventListener('storage', onStorage)
    return () => window.removeEventListener('storage', onStorage)
  }, [])

  useEffect(() => {
    // load profile into global store when authenticated
    if (!authenticated) return
    let cancelled = false
    async function load() {
      try {
        await ensureProfileLoaded()
      } catch (err) {
        if (!cancelled) console.error('nav-user: ensureProfileLoaded failed', err)
      }
    }
    load()
    return () => { cancelled = true }
  }, [authenticated])

  function handleSignIn() {
    navigate('/login')
  }

  function handleLogout() {
    api.logout()
    clearProfile()
    navigate('/')
  }

  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <SidebarMenuButton
              size="lg"
              className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
            >
              <Avatar className="h-8 w-8 rounded-lg grayscale">
                {profile?.avatar ? (
                  <AvatarImage src={profile.avatar} alt={profile?.username} />
                ) : (
                  <AvatarFallback className="rounded-lg">{profile?.username?.[0]?.toUpperCase() ?? 'U'}</AvatarFallback>
                )}
              </Avatar>
              <div className="grid flex-1 text-left text-sm leading-tight">
                <span className="truncate font-medium">{profile?.username}</span>
                <span className="text-muted-foreground truncate text-xs">{profile?.email}</span>
              </div>
              <IconDotsVertical className="ml-auto size-4" />
            </SidebarMenuButton>
          </DropdownMenuTrigger>
          <DropdownMenuContent
            className="w-(--radix-dropdown-menu-trigger-width) min-w-56 rounded-lg"
            side={isMobile ? 'bottom' : 'right'}
            align="end"
            sideOffset={4}
          >
            <DropdownMenuLabel className="p-0 font-normal">
              <div className="flex items-center gap-2 px-1 py-1.5 text-left text-sm">
                <Avatar className="h-8 w-8 rounded-lg">
                  {profile?.avatar ? (
                    <AvatarImage src={profile.avatar} alt={profile?.username} />
                  ) : (
                    <AvatarFallback className="rounded-lg">{profile?.username?.[0]?.toUpperCase() ?? 'U'}</AvatarFallback>
                  )}
                </Avatar>
                <div className="grid flex-1 text-left text-sm leading-tight">
                  <span className="truncate font-medium">{profile?.username}</span>
                  <span className="text-muted-foreground truncate text-xs">{profile?.email}</span>
                </div>
              </div>
            </DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuGroup>
              <DropdownMenuItem>
                <IconUserCircle />
                Account
              </DropdownMenuItem>
              {/* <DropdownMenuItem>
                <IconCreditCard />
                Billing
              </DropdownMenuItem> */}
              {/* <DropdownMenuItem>
                <IconNotification />
                Notifications
              </DropdownMenuItem> */}
            </DropdownMenuGroup>
            <DropdownMenuSeparator />
            {authenticated ? (
              <DropdownMenuItem onClick={handleLogout}>
                <IconLogout />
                Log out
              </DropdownMenuItem>
            ) : (
              <DropdownMenuItem onClick={handleSignIn}>Sign in</DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      </SidebarMenuItem>
    </SidebarMenu>
  )
}
