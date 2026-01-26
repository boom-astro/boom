import { useState, useEffect, useCallback } from "react"
import { useNavigate } from "react-router-dom"
import { Search } from "lucide-react"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { searchObjects, SearchResult } from "@/lib/api"
import { toast } from "sonner"
import { Spinner } from "./ui/spinner"

interface SearchContentProps {
  onResultClick: (result: SearchResult) => void
  maxResults?: number
  autoFocus?: boolean
  showFooter?: boolean
}

const DEBOUNCE_MS = 400

export function SearchContent({ 
  onResultClick, 
  maxResults = 10,
  autoFocus = true,
  showFooter = true 
}: SearchContentProps) {
  const [searchValue, setSearchValue] = useState("")
  const [results, setResults] = useState<SearchResult[]>([])
  const [isSearching, setIsSearching] = useState(false)
  const [placeholderMessage, setPlaceholderMessage] = useState<string | undefined>(undefined)
  const [lastSearchValue, setLastSearchValue] = useState("")

  // Search function with debouncing
  const performSearch = useCallback(async (value: string) => {
    const trimmed = value.trim()
    if (trimmed.length < 1) {
      setResults([])
      setIsSearching(false)
      setPlaceholderMessage(undefined)
      setLastSearchValue("")
      return
    }

    setIsSearching(true)
    try {
      const { results: searchResults, message } = await searchObjects(trimmed, maxResults)
      setResults(searchResults)
      setLastSearchValue(trimmed)
      const msg = typeof message === 'string' ? message : undefined
      if (msg && msg.toLowerCase().startsWith("invalid objectid format")) {
        setPlaceholderMessage(msg)
      } else {
        setPlaceholderMessage(undefined)
      }
    } catch (error) {
      console.error("Search error:", error)
      toast.error("Failed to search objects")
      setResults([])
      setLastSearchValue("")
    } finally {
      setIsSearching(false)
    }
  }, [maxResults])

  // Debounce search
  useEffect(() => {
    const trimmed = searchValue.trim()
    if (trimmed.length < 1) {
      setResults([])
      setIsSearching(false)
      setPlaceholderMessage(undefined)
      setLastSearchValue("")
      return
    }

    const timer = setTimeout(() => {
      // Optimization: skip search if user added characters and all results still match
      const isExtension = trimmed.length > lastSearchValue.length && trimmed.startsWith(lastSearchValue)
      const queryLower = trimmed.toLowerCase()
      const startsWithTwoDigitsAndLetter = /^\d{2}[a-z]/i.test(trimmed)
      const isDigitsOnly = /^\d+$/.test(trimmed)
      const isLsstPrefixProgression = ["l", "ls", "lss", "lsst"].includes(queryLower)

      const matchesQuery = (id: string) => {
        const idLower = id.toLowerCase()
        if (idLower.startsWith(queryLower)) return true
        // Exception: if query looks like "20a" (two digits + letter), allow matches like "ZTF20a"
        if (startsWithTwoDigitsAndLetter && idLower.startsWith(`ztf${queryLower}`)) return true
        // Exception: if query is digits-only, allow matches like "LSST123" for query "123"
        if (isDigitsOnly && idLower.startsWith(`lsst${queryLower}`)) return true
        // Exception: when user is typing LSST prefix (l, ls, lss, lsst) and all current results belong to LSST survey
        if (isLsstPrefixProgression && idLower.startsWith("lsst")) return true
        return false
      }

      if (
        isExtension &&
        results.length > 0 &&
        results.every(result => matchesQuery(result.objectId))
      ) {
        // Skip search, results are already filtered correctly
        return
      }
      
      performSearch(trimmed)
    }, DEBOUNCE_MS)

    return () => clearTimeout(timer)
  }, [searchValue, performSearch])

  return (
    <>
      <div className="relative">
        <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          placeholder="Enter object ID..."
          value={searchValue}
          onChange={(e) => setSearchValue(e.target.value)}
          className="pl-10"
          autoFocus={autoFocus}
        />
      </div>

      <div className="mt-4 h-[50vh] max-h-[500px] overflow-y-auto border rounded-md">
        {isSearching && (
          <div className="flex items-center justify-center h-full">
            <Spinner className="h-6 w-6" />
          </div>
        )}
        
        {!isSearching && results.length === 0 && searchValue && (
          <div className="relative flex items-center justify-center h-full text-center text-sm text-muted-foreground">
            <div>No objects found matching "{searchValue}"</div>
            {placeholderMessage && (
              <div className="absolute bottom-2 left-0 right-0 px-4 italic text-justify text-xs text-yellow-500">
                🚨 {placeholderMessage}
              </div>
            )}
          </div>
        )}

        {!isSearching && results.length === 0 && !searchValue && (
          <div className="flex items-center justify-center h-full text-center text-sm text-muted-foreground">
            Start typing to search for objects
          </div>
        )}

        {!isSearching && results.length > 0 && (
          <div className="space-y-1 p-2">
            {results.map((result, index) => (
              <button
                key={`${result.survey}-${result.objectId}-${index}`}
                onClick={() => onResultClick(result)}
                className="flex w-full items-center justify-between rounded-md px-3 py-2 text-sm hover:bg-accent transition-colors text-left"
              >
                <div className="flex flex-col gap-1">
                  <span className="font-medium">{result.objectId}</span>
                  <span className="text-xs text-muted-foreground">
                    RA: {result.ra.toFixed(6)}° | Dec: {result.dec.toFixed(6)}°
                  </span>
                </div>
                <span className="rounded-md bg-primary/10 px-2 py-1 text-xs font-medium text-primary">
                  {result.survey}
                </span>
              </button>
            ))}
          </div>
        )}
      </div>

      {showFooter && (
        <div className="mt-4 flex items-center justify-between border-t pt-4 text-xs text-muted-foreground">
          <span>Press ESC to close</span>
          <span>Showing up to {maxResults} results</span>
        </div>
      )}
    </>
  )
}

interface SearchDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function SearchDialog({ open, onOpenChange }: SearchDialogProps) {
  const navigate = useNavigate()

  const handleResultClick = (result: SearchResult) => {
    navigate(`/objects/${result.survey}/${result.objectId}`)
    onOpenChange(false)
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[600px] top-[10%] translate-y-0">
        <DialogHeader>
          <DialogTitle>Search Objects</DialogTitle>
          <DialogDescription>
            Search for any object ID from any survey
          </DialogDescription>
        </DialogHeader>
        
        <SearchContent onResultClick={handleResultClick} />
      </DialogContent>
    </Dialog>
  )
}

export function useSearchDialog() {
  const [open, setOpen] = useState(false)

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Ctrl+K or Cmd+K to open search
      if ((e.ctrlKey || e.metaKey) && e.key === "k") {
        e.preventDefault()
        setOpen((prev) => !prev)
      }
    }

    document.addEventListener("keydown", handleKeyDown)
    return () => document.removeEventListener("keydown", handleKeyDown)
  }, [])

  return { open, setOpen }
}
