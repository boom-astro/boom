import { useState, useEffect, useCallback, useRef, useMemo, memo } from "react";
import { Card, CardHeader, CardTitle, CardDescription, CardContent, CardFooter } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import api, { Alert, Cutouts } from "@/lib/api";
import { bytes2image } from "@/lib/imageProcessing";

// ─── AlertSearchResults ───────────────────────────────────────────────────────

export function AlertSearchResults({ searchResultsRef, loading, error, alerts, survey, currentPage, pageSize, onPrev, onNext }: {
  searchResultsRef: React.RefObject<HTMLDivElement | null>;
  loading: boolean;
  error: string | null;
  alerts: Alert[];
  survey: 'ZTF' | 'LSST';
  currentPage: number;
  pageSize: number;
  onPrev: () => void;
  onNext: () => void;
}) {
  // Per-page cutout cache keyed by candid. Cleared whenever the result set changes
  // (new search or page navigation) so memory never accumulates across pages.
  const cutoutCache = useRef<Map<string, Cutouts>>(new Map());
  useEffect(() => { cutoutCache.current.clear(); }, [alerts]);

  const scrollContainerRef = useRef<HTMLDivElement>(null);

  const getCache = useCallback((candid: string) => cutoutCache.current.get(candid), []);
  const setCache = useCallback((candid: string, data: Cutouts) => { cutoutCache.current.set(candid, data); }, []);

  return (
    <Card ref={searchResultsRef} className="max-w-6xl mx-auto" style={{ height: '94vh' }}>
      <CardHeader>
        <CardTitle>Search Results</CardTitle>
        <CardDescription>
          {loading ? "Loading..." : alerts.length > 0 ? `Page ${currentPage} — ${alerts.length} alert${alerts.length !== 1 ? 's' : ''}` : error ? "Error" : "No results yet"}
        </CardDescription>
      </CardHeader>
      <CardContent ref={scrollContainerRef} className="overflow-y-auto" style={{ maxHeight: 'calc(100vh - 200px)' }}>
        {error && <div className="text-red-500 text-sm">{error}</div>}
        {loading && (
          <div className="space-y-4">
            {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map((i) => (
              <div key={i} className="flex gap-4 p-4 border rounded-lg">
                <div className="flex gap-2 shrink-0">
                  <Skeleton className="h-24 w-24" />
                  <Skeleton className="h-24 w-24" />
                  <Skeleton className="h-24 w-24" />
                </div>
                <div className="flex-1 grid grid-cols-2 gap-x-4 gap-y-1">
                  <Skeleton className="h-4 w-3/4" />
                  <Skeleton className="h-4 w-1/2" />
                  <Skeleton className="h-4 w-2/3" />
                </div>
              </div>
            ))}
          </div>
        )}
        {!loading && !error && alerts.length > 0 && (
          <div className="space-y-3">
            {alerts.map((alert) => (
              <AlertCard
                key={alert.candid}
                alert={alert}
                survey={survey}
                scrollRoot={scrollContainerRef}
                getCache={getCache}
                setCache={setCache}
              />
            ))}
          </div>
        )}
        {!loading && !error && alerts.length === 0 && (
          <div className="text-center text-muted-foreground py-8">
            Enter search parameters and click "Search Alerts" to find alerts.
          </div>
        )}
      </CardContent>
      <CardFooter className="border-t px-6 py-4">
        <PaginationBar
          currentPage={currentPage}
          hasMore={alerts.length >= pageSize}
          onPrev={onPrev}
          onNext={onNext}
        />
      </CardFooter>
    </Card>
  );
}

// ─── Pagination ───────────────────────────────────────────────────────────────

function PaginationBar({ currentPage, hasMore, onPrev, onNext }: {
  currentPage: number; hasMore: boolean; onPrev: () => void; onNext: () => void;
}) {
  return (
    <div className="flex items-center justify-between text-sm gap-4 w-full">
      <div className="text-muted-foreground">Page {currentPage}</div>
      <div className="flex gap-2">
        <Button variant="outline" size="sm" onClick={onPrev} disabled={currentPage === 1}>Previous</Button>
        <Button variant="outline" size="sm" onClick={onNext} disabled={!hasMore}>Next</Button>
      </div>
    </div>
  );
}

// ─── Alert card ───────────────────────────────────────────────────────────────

const AlertCard = memo(function AlertCard({ alert, survey, scrollRoot, getCache, setCache }: {
  alert: Alert;
  survey: 'ZTF' | 'LSST';
  scrollRoot: React.RefObject<HTMLDivElement | null>;
  getCache: (candid: string) => Cutouts | undefined;
  setCache: (candid: string, data: Cutouts) => void;
}) {
  const [cutouts, setCutouts] = useState<Cutouts | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const cardRef = useRef<HTMLDivElement>(null);
  // Incremented on every exit or unmount to invalidate in-flight fetches.
  const fetchVersion = useRef(0);

  useEffect(() => {
    const el = cardRef.current;
    if (!el) return;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (!entry.isIntersecting) {
          // Leaving the viewport: invalidate any in-flight fetch and drop rendered data.
          // The raw bytes remain in the shared cache for instant restore on re-entry.
          fetchVersion.current++;
          setIsLoading(false);
          setCutouts(null);
          return;
        }

        // Entering the viewport: restore from cache or fetch for the first time.
        const cached = getCache(alert.candid);
        if (cached !== undefined) {
          setCutouts(cached);
          return;
        }

        setIsLoading(true);
        const version = ++fetchVersion.current;

        api.fetchAlertCutouts(survey, alert.candid)
          .then(data => {
            if (fetchVersion.current !== version) return;
            setCache(alert.candid, data);
            setCutouts(data);
          })
          .catch(() => {
            if (fetchVersion.current !== version) return;
            // Cache the empty result so we don't retry on every re-entry.
            const empty: Cutouts = {};
            setCache(alert.candid, empty);
            setCutouts(empty);
          })
          .finally(() => {
            if (fetchVersion.current === version) setIsLoading(false);
          });
      },
      { root: scrollRoot.current, rootMargin: '150px 0px' },
    );

    observer.observe(el);
    return () => {
      fetchVersion.current++;
      observer.disconnect();
    };
  }, [alert.candid, survey, getCache, setCache]);

  // Decode images only when cutouts change, not on every render.
  // cutouts === null means off-screen; {} means fetched but empty — both are handled.
  const images = useMemo(() => {
    if (cutouts === null) return null;
    return {
      science:    bytes2image(cutouts.cutoutScience,    survey, "science",    "bone"),
      template:   bytes2image(cutouts.cutoutTemplate,   survey, "template",   "bone"),
      difference: bytes2image(cutouts.cutoutDifference, survey, "difference", "bone"),
    };
  }, [cutouts, survey]);

  const drb = alert.candidate.drb ?? alert.candidate.reliability ?? null;

  const handleClick = useCallback(() => {
    window.open(`/objects/${encodeURIComponent(survey)}/${encodeURIComponent(alert.objectId || '')}`, '_blank');
  }, [survey, alert.objectId]);

  return (
    <div
      ref={cardRef}
      className="flex gap-4 p-4 border rounded-lg hover:bg-accent/50 cursor-pointer transition-colors"
      onClick={handleClick}
    >
      {/* Fixed-width image strip — always reserves the same space to prevent layout shifts. */}
      <div className="flex gap-2 shrink-0">
        {isLoading ? (
          <>
            <Skeleton className="h-24 w-24" />
            <Skeleton className="h-24 w-24" />
            <Skeleton className="h-24 w-24" />
          </>
        ) : images === null ? (
          // Off-screen: invisible placeholders keep the row height stable.
          <>
            <div className="h-24 w-24" />
            <div className="h-24 w-24" />
            <div className="h-24 w-24" />
          </>
        ) : (
          <>
            {images.science    && <img src={images.science}    alt="Science"    className="h-24 w-24 object-contain border rounded" title="Science"    style={{ imageRendering: 'pixelated' }} />}
            {images.template   && <img src={images.template}   alt="Template"   className="h-24 w-24 object-contain border rounded" title="Template"   style={{ imageRendering: 'pixelated' }} />}
            {images.difference && <img src={images.difference} alt="Difference" className="h-24 w-24 object-contain border rounded" title="Difference" style={{ imageRendering: 'pixelated' }} />}
            {!images.science && !images.template && !images.difference && (
              <>
                <div className="h-24 w-24 bg-muted rounded" />
                <div className="h-24 w-24 bg-muted rounded flex items-center justify-center text-xs text-muted-foreground">No cutouts</div>
                <div className="h-24 w-24 bg-muted rounded" />
              </>
            )}
          </>
        )}
      </div>
      <div className="flex-1 grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
        <div><span className="text-muted-foreground">Object ID:</span> <span className="font-mono">{alert.objectId}</span></div>
        <div><span className="text-muted-foreground">Candid:</span> <span className="font-mono">{alert.candid}</span></div>
        <div><span className="text-muted-foreground">JD:</span> <span className="font-mono">{alert.candidate.jd.toFixed(5)}</span></div>
        {drb !== null && <div><span className="text-muted-foreground">DRB/Reliability:</span> <span className="font-mono">{drb.toFixed(3)}</span></div>}
        {alert.candidate.magpsf !== undefined && <div><span className="text-muted-foreground">Magnitude:</span> <span className="font-mono">{alert.candidate.magpsf.toFixed(2)}</span></div>}
        {alert.candidate.fid !== undefined && <div><span className="text-muted-foreground">Band:</span> <span className="font-mono">{String(alert.candidate.band)}</span></div>}
      </div>
    </div>
  );
});
