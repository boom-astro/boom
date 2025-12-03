import create from 'zustand'
import { persist } from 'zustand/middleware'
import api, { Profile } from './api'

async function generateAvatarUrl(email: string) {
    const msgUint8 = new TextEncoder().encode(email.trim().toLowerCase());
    const hashBuffer = await crypto.subtle.digest("SHA-256", msgUint8);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    return `https://www.gravatar.com/avatar/${hashHex}?d=identicon&s=64`;
}

type AppState = {
  profile: Profile
  setProfile: (p: Profile) => void
  clearProfile: () => void

  // currently-loaded source/object data
  currentSource: Record<string, unknown> | null
  setCurrentSource: (v: Record<string, unknown> | null) => void
  clearCurrentSource: () => void

  // misc helpers
  lastUpdated: number | null
  touch: () => void
}

export const useAppStore = create<AppState>()(
  persist(
    (set) => ({
      profile: null,
      setProfile: (p) => set(() => ({ profile: p, lastUpdated: Date.now() })),
      clearProfile: () => {
        set(() => ({ profile: null, lastUpdated: Date.now() }))
      },

      currentSource: null,
      setCurrentSource: (v) => set(() => ({ currentSource: v, lastUpdated: Date.now() })),
      clearCurrentSource: () => set(() => ({ currentSource: null, lastUpdated: Date.now() })),

      lastUpdated: null,
      touch: () => set(() => ({ lastUpdated: Date.now() })),
    }),
    {
      name: 'boom-app-state', // localStorage key
      // only persist selected parts
      partialize: (state) => ({ profile: state.profile, currentSource: state.currentSource, lastUpdated: state.lastUpdated }),
    }
  )
)

// helper to load profile if authenticated and no profile in store
export async function ensureProfileLoaded() {
  const token = api.getTokenRecord()
  if (!token) return
  const state = useAppStore.getState()
  if (state.profile && state.lastUpdated && state.lastUpdated > Date.now() - 5 * 60 * 1000) {
    // profile exists and was updated less than 5min ago
    return
  }
    try {
    const data = await api.fetchProfile()
    // assume API returns { username, email, avatar }
    // normalize: if API returns `name`, copy it to `username`
    console.log('fetched profile:', data);
    if (data && data.name && !data.username) {
      data.username = data.name as string;
    }
    if (data && data.email) {
        console.log('generate avatar for', data.email);
        data.avatar = await generateAvatarUrl(data.email);
    }
    useAppStore.getState().setProfile(data)
  } catch (err: unknown) {
    // if fetchProfile throws (e.g. unauthorized) clear token
    console.error('ensureProfileLoaded: failed to fetch profile', err)
    api.logout()
    useAppStore.getState().clearProfile()
  }
}

export default useAppStore
