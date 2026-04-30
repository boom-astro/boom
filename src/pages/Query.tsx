import { useState, FormEvent, useEffect, useCallback, useRef, memo } from "react";
import { Select, SelectTrigger, SelectContent, SelectItem, SelectValue } from "@/components/ui/select";
import { Card, CardHeader, CardTitle, CardDescription, CardContent, CardFooter } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import api, { Alert, AlertSearchParams, Cutouts } from "@/lib/api";
import { bytes2image } from "@/lib/imageProcessing";
import { Skeleton } from "@/components/ui/skeleton";
import { SearchContent } from "@/components/search-dialog"
import * as analytics from "@/lib/analytics";

export default function Query() {
  const [activeTab, setActiveTab] = useState<"object" | "alerts">("object");

  // Alert search state
  const [alertSurvey, setAlertSurvey] = useState<'ZTF' | 'LSST'>('ZTF');
  const [alertObjectId, setAlertObjectId] = useState('');
  const [ra, setRa] = useState('');
  const [dec, setDec] = useState('');
  const [radius, setRadius] = useState('');
  const [startJd, setStartJd] = useState('');
  const [endJd, setEndJd] = useState('');
  const [minMag, setMinMag] = useState('');
  const [maxMag, setMaxMag] = useState('');
  const [minDrb, setMinDrb] = useState('');
  const [maxDrb, setMaxDrb] = useState('');
  const [minSgscore1, setMinSgscore1] = useState('');
  const [maxSgscore1, setMaxSgscore1] = useState('');
  const [minDistpsnr1, setMinDistpsnr1] = useState('');
  const [maxDistpsnr1, setMaxDistpsnr1] = useState('');

  // Alert results state
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 25;
  const searchResultsRef = useRef<HTMLDivElement | null>(null);

  async function submitAlertSearch(e?: FormEvent) {
    e?.preventDefault();

    const hasObjectId = alertObjectId.trim();
    const hasCoords = ra && dec && radius;

    // Validate that only one search method is used
    if (hasObjectId && hasCoords) {
      setError("Please use either Object ID or Position search, not both");
      return;
    }

    if (!hasObjectId && !hasCoords) {
      setError("Please enter either an Object ID or Position search parameters");
      return;
    }

    const params: AlertSearchParams = {};
    const searchType = hasObjectId ? 'object_id' : 'position';

    if (hasObjectId) {
      params.object_id = alertObjectId.trim();
      analytics.trackAlertSearchSubmitted({
        survey: alertSurvey,
        search_type: searchType,
        object_id: alertObjectId,
      });
    } else {
      params.ra = parseFloat(ra);
      params.dec = parseFloat(dec);
      params.radius_arcsec = parseFloat(radius);
      analytics.trackAlertSearchSubmitted({
        survey: alertSurvey,
        search_type: searchType,
        ra: params.ra,
        dec: params.dec,
        radius_arcsec: params.radius_arcsec,
        has_date_filter: !!(startJd || endJd),
        has_magnitude_filter: !!(minMag || maxMag),
        has_drb_filter: !!(minDrb || maxDrb),
        has_sgscore_filter: !!(minSgscore1 || maxSgscore1),
        has_distpsnr_filter: !!(minDistpsnr1 || maxDistpsnr1),
      });
    }

    if (startJd) params.start_jd = parseFloat(startJd);
    if (endJd) params.end_jd = parseFloat(endJd);
    if (minMag) params.min_magpsf = parseFloat(minMag);
    if (maxMag) params.max_magpsf = parseFloat(maxMag);
    if (minDrb) params.min_drb = parseFloat(minDrb);
    if (maxDrb) params.max_drb = parseFloat(maxDrb);
    if (minSgscore1) params.min_sgscore1 = parseFloat(minSgscore1);
    if (maxSgscore1) params.max_sgscore1 = parseFloat(maxSgscore1);
    if (minDistpsnr1) params.min_distpsnr1 = parseFloat(minDistpsnr1);
    if (maxDistpsnr1) params.max_distpsnr1 = parseFloat(maxDistpsnr1);

    setLoading(true);
    setError(null);
    setAlerts([]);
    setCurrentPage(1);

    // Snap results into view on search start
    requestAnimationFrame(() => {
      searchResultsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });

    try {
      const results = await api.fetchAlerts(alertSurvey, params);
      setAlerts(results);
      analytics.trackAlertSearchCompleted({
        survey: alertSurvey,
        search_type: searchType,
        result_count: results.length,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch alerts");
      analytics.trackError('alert_search', err, { survey: alertSurvey, search_type: searchType });
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="px-4 lg:px-6 space-y-4">
      <Card className="max-w-6xl mx-auto">
        <CardHeader>
          <CardTitle>Query the Archive</CardTitle>
          <CardDescription>Search by object identifier or find alerts by position and filters.</CardDescription>
        </CardHeader>
        <CardContent>
          <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as "object" | "alerts")}>
            <TabsList className="grid w-full grid-cols-2 mb-4">
              <TabsTrigger value="object">Object Lookup</TabsTrigger>
              <TabsTrigger value="alerts">Alert Search</TabsTrigger>
            </TabsList>

            <TabsContent value="object">
              <SearchContent
                onResultClick={(result) => {
                  window.location.href = `/objects/${result.survey}/${result.objectId}`;
                }}
                maxResults={20}
                showFooter={false}
              />
              <div className="text-sm text-muted-foreground mt-2 w-full text-end">
                Showing up to 10 results.
              </div>
            </TabsContent>

            <TabsContent value="alerts">
              <div className="flex flex-col items-center justify-center py-12 gap-4 text-center text-muted-foreground">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" className="size-10 opacity-40" aria-hidden>
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" d="M12 6v6l4 2" />
                  <circle cx="12" cy="12" r="10" strokeWidth="1.5" />
                </svg>
                <div>
                  <p className="text-base font-semibold text-foreground">Coming Soon</p>
                  <p className="text-sm mt-1 pb-2">Alert Search from the web application is under development and will be available shortly (~late February).</p>
                  <p className="text-sm mt-1">In the meantime, please give a go at searching for alerts using the Python client (see Kafka docs for examples)!</p>
                </div>
              </div>
              <form onSubmit={submitAlertSearch} className="space-y-4 hidden">
                <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
                  <div className="sm:col-span-4">
                    <Label className="text-xs font-medium mb-1 block text-muted-foreground">Survey</Label>
                    <Select value={alertSurvey} onValueChange={(v) => setAlertSurvey(v as 'ZTF' | 'LSST')}>
                      <SelectTrigger className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="ZTF">ZTF</SelectItem>
                        <SelectItem value="LSST">LSST</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="sm:col-span-4">
                    <Label htmlFor="alertObjectId" className={`text-xs font-medium mb-1 block ${ra || dec || radius ? 'text-muted-foreground opacity-50' : 'text-muted-foreground'}`}>Object ID</Label>
                    <Input
                      id="alertObjectId"
                      value={alertObjectId}
                      onChange={e => setAlertObjectId(e.target.value)}
                      placeholder="Object ID (e.g. ZTF25aagbkaj)"
                      disabled={!!(ra || dec || radius)}
                      className={ra || dec || radius ? 'opacity-50' : ''}
                    />
                  </div>

                  <div className="sm:col-span-4 text-center text-sm text-muted-foreground my-1">Or by Position</div>

                  <div className={alertObjectId ? 'opacity-50' : ''}>
                    <Label htmlFor="ra" className="text-xs font-medium mb-1 block text-muted-foreground">RA (deg)</Label>
                    <Input
                      id="ra"
                      type="number"
                      step="any"
                      value={ra}
                      onChange={e => setRa(e.target.value)}
                      placeholder="150.0"
                      disabled={!!alertObjectId}
                    />
                  </div>

                  <div className={alertObjectId ? 'opacity-50' : ''}>
                    <Label htmlFor="dec" className="text-xs font-medium mb-1 block text-muted-foreground">Dec (deg)</Label>
                    <Input
                      id="dec"
                      type="number"
                      step="any"
                      value={dec}
                      onChange={e => setDec(e.target.value)}
                      placeholder="2.5"
                      disabled={!!alertObjectId}
                    />
                  </div>

                  <div className={alertObjectId ? 'opacity-50' : ''}>
                    <Label htmlFor="radius" className="text-xs font-medium mb-1 block text-muted-foreground">Radius (arcsec)</Label>
                    <Input
                      id="radius"
                      type="number"
                      step="any"
                      value={radius}
                      onChange={e => setRadius(e.target.value)}
                      placeholder="5.0"
                      disabled={!!alertObjectId}
                    />
                  </div>

                  <div className="sm:col-span-4">
                    <Separator className="my-2" />
                  </div>

                  <div className="sm:col-span-4">
                    <h3 className="font-semibold text-sm mb-2">Time Range (Optional)</h3>
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="startJd" className="text-xs font-medium mb-1 block text-muted-foreground">Start JD</Label>
                    <Input id="startJd" type="number" step="any" value={startJd} onChange={e => setStartJd(e.target.value)} placeholder="2459000.5" />
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="endJd" className="text-xs font-medium mb-1 block text-muted-foreground">End JD</Label>
                    <Input id="endJd" type="number" step="any" value={endJd} onChange={e => setEndJd(e.target.value)} placeholder="2459100.5" />
                  </div>

                  <div className="sm:col-span-4">
                    <h3 className="font-semibold text-sm mb-2 mt-2">Filters (Optional)</h3>
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="minMag" className="text-xs font-medium mb-1 block text-muted-foreground">Min Magnitude</Label>
                    <Input id="minMag" type="number" step="any" value={minMag} onChange={e => setMinMag(e.target.value)} placeholder="18.0" />
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="maxMag" className="text-xs font-medium mb-1 block text-muted-foreground">Max Magnitude</Label>
                    <Input id="maxMag" type="number" step="any" value={maxMag} onChange={e => setMaxMag(e.target.value)} placeholder="21.0" />
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="minDrb" className="text-xs font-medium mb-1 block text-muted-foreground">Min DRB Score</Label>
                    <Input id="minDrb" type="number" step="any" value={minDrb} onChange={e => setMinDrb(e.target.value)} placeholder="0.5" />
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="maxDrb" className="text-xs font-medium mb-1 block text-muted-foreground">Max DRB Score</Label>
                    <Input id="maxDrb" type="number" step="any" value={maxDrb} onChange={e => setMaxDrb(e.target.value)} placeholder="1.0" />
                  </div>

                  <div className="sm:col-span-4">
                    <h3 className="font-semibold text-sm mb-2 mt-2">ZTF-Specific Filters (Optional)</h3>
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="minSgscore1" className="text-xs font-medium mb-1 block text-muted-foreground">Min sgscore1</Label>
                    <Input id="minSgscore1" type="number" step="any" value={minSgscore1} onChange={e => setMinSgscore1(e.target.value)} placeholder="0.5" />
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="maxSgscore1" className="text-xs font-medium mb-1 block text-muted-foreground">Max sgscore1</Label>
                    <Input id="maxSgscore1" type="number" step="any" value={maxSgscore1} onChange={e => setMaxSgscore1(e.target.value)} placeholder="1.0" />
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="minDistpsnr1" className="text-xs font-medium mb-1 block text-muted-foreground">Min distpsnr1</Label>
                    <Input id="minDistpsnr1" type="number" step="any" value={minDistpsnr1} onChange={e => setMinDistpsnr1(e.target.value)} placeholder="0.0" />
                  </div>

                  <div className="sm:col-span-2">
                    <Label htmlFor="maxDistpsnr1" className="text-xs font-medium mb-1 block text-muted-foreground">Max distpsnr1</Label>
                    <Input id="maxDistpsnr1" type="number" step="any" value={maxDistpsnr1} onChange={e => setMaxDistpsnr1(e.target.value)} placeholder="2.0" />
                  </div>
                </div>

                <div className="flex justify-end gap-2 mt-4">
                  <Button type="submit" disabled={loading}>
                    {loading ? "Searching..." : "Search Alerts"}
                  </Button>
                </div>
              </form>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {/* Alert Results */}
      {activeTab === "alerts" && (
        <Card ref={searchResultsRef} className="max-w-6xl mx-auto" style={{ height: '94vh' }}>
          <CardHeader>
            <CardTitle>Search Results</CardTitle>
            <CardDescription>
              {loading ? "Loading..." : alerts.length > 0 ? `Found ${alerts.length} alert${alerts.length !== 1 ? 's' : ''}` : error ? "Error" : "No results yet"}
            </CardDescription>
          </CardHeader>
          <CardContent className="overflow-y-auto" style={{ maxHeight: 'calc(100vh - 200px)' }}>
            {error && (
              <div className="text-red-500 text-sm">{error}</div>
            )}

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
                {alerts
                  .slice((currentPage - 1) * pageSize, currentPage * pageSize)
                  .map((alert) => (
                    <AlertCard key={alert.candid} alert={alert} survey={alertSurvey} />
                  ))}
              </div>
            )}

            {!loading && !error && alerts.length === 0 && (
              <div className="text-center text-muted-foreground py-8">
                Enter search parameters and click "Search Alerts" to find alerts.
              </div>
            )}
          </CardContent>
          {activeTab === "alerts" && (
            <CardFooter className="border-t px-6 py-4">
              <PaginationBar
                currentPage={currentPage}
                totalItems={alerts.length}
                pageSize={pageSize}
                onPrev={() => setCurrentPage((p) => Math.max(1, p - 1))}
                onNext={() => setCurrentPage((p) => Math.min(Math.ceil(alerts.length / pageSize), p + 1))}
              />
            </CardFooter>
          )}
        </Card>
      )}
    </div>
  )
}

function PaginationBar({ currentPage, totalItems, pageSize, onPrev, onNext }: { currentPage: number; totalItems: number; pageSize: number; onPrev: () => void; onNext: () => void }) {
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));

  return (
    <div className="flex items-center justify-between text-sm gap-4 w-full">
      <div className="text-muted-foreground">
        Page {currentPage} of {totalPages} ({totalItems} total)
      </div>
      <div className="flex gap-2">
        <Button
          variant="outline"
          size="sm"
          onClick={onPrev}
          disabled={currentPage === 1}
        >
          Previous
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={onNext}
          disabled={currentPage >= totalPages}
        >
          Next
        </Button>
      </div>
    </div>
  );
}

const AlertCard = memo(function AlertCard({ alert, survey }: { alert: Alert; survey: 'ZTF' | 'LSST' }) {
  const [cutouts, setCutouts] = useState<Cutouts | null>(null);
  const [loadingCutouts, setLoadingCutouts] = useState(true);

  const drb = alert.candidate.drb ?? alert.candidate.reliability ?? null;

  const scienceImage = cutouts ? bytes2image(cutouts.cutoutScience, survey, "science", "bone") : null;
  const templateImage = cutouts ? bytes2image(cutouts.cutoutTemplate, survey, "template", "bone") : null;
  const differenceImage = cutouts ? bytes2image(cutouts.cutoutDifference, survey, "difference", "bone") : null;

  useEffect(() => {
    const fetchCutouts = async () => {
      setLoadingCutouts(true);
      try {
        const data = await api.fetchAlertCutouts(survey, alert.candid);
        setCutouts(data);
      } catch (err) {
        console.error("Failed to load cutouts:", err);
        setCutouts({});
      } finally {
        setLoadingCutouts(false);
      }
    };
    fetchCutouts();
  }, [survey, alert.candid]);

  const handleClick = useCallback(() => {
    const objectUrl = `/objects/${encodeURIComponent(survey)}/${encodeURIComponent(alert.objectId || '')}`;
    window.open(objectUrl, '_blank');
  }, [survey, alert.objectId]);

  return (
    <div
      className="flex gap-4 p-4 border rounded-lg hover:bg-accent/50 cursor-pointer transition-colors"
      onClick={handleClick}
    >
      {/* Cutouts */}
      <div className="flex gap-2 shrink-0">
        {loadingCutouts ? (
          <>
            <Skeleton className="h-24 w-24" />
            <Skeleton className="h-24 w-24" />
            <Skeleton className="h-24 w-24" />
          </>
        ) : (
          <>
            {scienceImage && (
              <img src={scienceImage} alt="Science" className="h-24 w-24 object-contain border rounded" title="Science" style={{ imageRendering: 'pixelated' }} />
            )}
            {templateImage && (
              <img src={templateImage} alt="Template" className="h-24 w-24 object-contain border rounded" title="Template" style={{ imageRendering: 'pixelated' }} />
            )}
            {differenceImage && (
              <img src={differenceImage} alt="Difference" className="h-24 w-24 object-contain border rounded" title="Difference" style={{ imageRendering: 'pixelated' }} />
            )}
            {!scienceImage && !templateImage && !differenceImage && (
              <div className="h-24 w-24 bg-muted rounded flex items-center justify-center text-xs text-muted-foreground">
                No cutouts
              </div>
            )}
          </>
        )}
      </div>

      {/* Alert Info */}
      <div className="flex-1 grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
        <div>
          <span className="text-muted-foreground">Object ID:</span> <span className="font-mono">{alert.objectId}</span>
        </div>
        <div>
          <span className="text-muted-foreground">Candid:</span> <span className="font-mono">{alert.candid}</span>
        </div>
        <div>
          <span className="text-muted-foreground">JD:</span> <span className="font-mono">{alert.candidate.jd.toFixed(5)}</span>
        </div>
        {drb !== null && (
          <div>
            <span className="text-muted-foreground">DRB/Reliability:</span> <span className="font-mono">{drb.toFixed(3)}</span>
          </div>
        )}
        {alert.candidate.magpsf !== undefined && (
          <div>
            <span className="text-muted-foreground">Magnitude:</span> <span className="font-mono">{alert.candidate.magpsf.toFixed(2)}</span>
          </div>
        )}
        {alert.candidate.fid !== undefined && (
          <div>
            <span className="text-muted-foreground">Band:</span> <span className="font-mono">{String(alert.candidate.band)}</span>
          </div>
        )}
      </div>
    </div>
  );
});
