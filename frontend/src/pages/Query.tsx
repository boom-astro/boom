import { useState, FormEvent, useRef } from "react";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";
import api, { Alert, AlertSearchParams } from "@/lib/api";
import { SearchContent } from "@/components/search-dialog"
import * as analytics from "@/lib/analytics";
import { AlertFilterForm, AlertFilterFormProps, TimeFormat, BoolFilter, toJd, timeFormatDefaults } from "@/components/alert-filter-form";
import { AlertSearchResults } from "@/components/alert-search-results";
import { PositionInput, ParsedPosition } from "@/components/position-input";

const PAGE_SIZE = 50;

export default function Query() {
  const [activeTab, setActiveTab] = useState<"object" | "alerts">("object");

  // Alert search state
  const [alertSurvey, setAlertSurvey] = useState<'ZTF' | 'LSST'>('ZTF');
  const [alertSearchTab, setAlertSearchTab] = useState<'filters' | 'object' | 'position'>('filters');

  function handleAlertSearchTabChange(tab: 'filters' | 'object' | 'position') {
    setAlertSearchTab(tab);
    setError(null);
  }
  const [alertObjectId, setAlertObjectId] = useState('');
  const [parsedPosition, setParsedPosition] = useState<ParsedPosition | null>(null);
  const [radius, setRadius] = useState('');
  const [timeFormat, setTimeFormat] = useState<TimeFormat>('local');
  const [startTime, setStartTime] = useState(() => timeFormatDefaults('local').start);
  const [endTime, setEndTime] = useState(() => timeFormatDefaults('local').end);
  const [minMag, setMinMag] = useState('');
  const [maxMag, setMaxMag] = useState('');
  const [minDrb, setMinDrb] = useState('0.5');
  const [maxDrb, setMaxDrb] = useState('');
  const [isRock, setIsRock] = useState<BoolFilter>('any');
  const [isStar, setIsStar] = useState<BoolFilter>('any');
  const [isNearBrightstar, setIsNearBrightstar] = useState<BoolFilter>('any');
  const [isStationary, setIsStationary] = useState<BoolFilter>('any');
  const [isPositive, setIsPositive] = useState<BoolFilter>('any');

  // Alert results state
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const searchResultsRef = useRef<HTMLDivElement | null>(null);
  const lastSearchRef = useRef<{ survey: 'ZTF' | 'LSST'; params: AlertSearchParams; searchType: string } | null>(null);

  function handleTimeFormatChange(fmt: TimeFormat) {
    setTimeFormat(fmt);
  }

  const filterProps: AlertFilterFormProps = {
    survey: alertSurvey,
    timeFormat, onTimeFormatChange: handleTimeFormatChange,
    startTime, setStartTime, endTime, setEndTime,
    minMag, setMinMag, maxMag, setMaxMag,
    minDrb, setMinDrb, maxDrb, setMaxDrb,
    isRock, setIsRock, isStar, setIsStar,
    isNearBrightstar, setIsNearBrightstar,
    isStationary, setIsStationary,
    isPositive, setIsPositive,
  };

  async function executeSearch(survey: 'ZTF' | 'LSST', params: AlertSearchParams, searchType: string) {
    setLoading(true);
    setError(null);
    setAlerts([]);
    requestAnimationFrame(() => {
      searchResultsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
    try {
      const results = await api.fetchAlerts(survey, params);
      setAlerts(results);
      analytics.trackAlertSearchCompleted({ survey, search_type: searchType, result_count: results.length });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch alerts");
      analytics.trackError('alert_search', err, { survey, search_type: searchType });
    } finally {
      setLoading(false);
    }
  }

  async function submitAlertSearch(e?: FormEvent) {
    e?.preventDefault();
    const baseParams: AlertSearchParams = { limit: PAGE_SIZE };
    let searchType: string;

    if (alertSearchTab === 'object') {
      if (!alertObjectId.trim()) { setError("Please enter an Object ID"); return; }
      baseParams.object_id = alertObjectId.trim();
      searchType = 'object_id';
      analytics.trackAlertSearchSubmitted({ survey: alertSurvey, search_type: searchType, object_id: alertObjectId });
    } else if (alertSearchTab === 'position') {
      if (!parsedPosition || !radius) { setError("Please enter a valid position and radius"); return; }
      baseParams.ra = parsedPosition.ra;
      baseParams.dec = parsedPosition.dec;
      baseParams.radius_arcsec = parseFloat(radius);
      searchType = 'position';
      analytics.trackAlertSearchSubmitted({ survey: alertSurvey, search_type: searchType, ra: baseParams.ra, dec: baseParams.dec, radius_arcsec: baseParams.radius_arcsec });
    } else {
      searchType = 'filters';
      const startJd = toJd(startTime, timeFormat);
      const endJd = toJd(endTime, timeFormat);
      if (startJd !== undefined && endJd !== undefined) {
        const windowMs = (endJd - startJd) * 86_400_000;
        if (windowMs <= 0) { setError("End time must be after start time"); return; }
        if (windowMs > 24 * 60 * 60 * 1000) { setError("Time range cannot exceed 24 hours"); return; }
      }
      if (startJd !== undefined) baseParams.start_jd = startJd;
      if (endJd !== undefined) baseParams.end_jd = endJd;
      if (minMag) baseParams.min_magpsf = parseFloat(minMag);
      if (maxMag) baseParams.max_magpsf = parseFloat(maxMag);
      if (minDrb) baseParams.min_drb = parseFloat(minDrb);
      if (maxDrb) baseParams.max_drb = parseFloat(maxDrb);
      if (isRock !== 'any') baseParams.is_rock = isRock === 'true';
      if (isStar !== 'any') baseParams.is_star = isStar === 'true';
      if (isNearBrightstar !== 'any') baseParams.is_near_brightstar = isNearBrightstar === 'true';
      if (isStationary !== 'any') baseParams.is_stationary = isStationary === 'true';
      if (isPositive !== 'any') baseParams.is_positive = isPositive === 'true';
      analytics.trackAlertSearchSubmitted({
        survey: alertSurvey, search_type: searchType,
        has_date_filter: !!(startJd || endJd),
        has_magnitude_filter: !!(minMag || maxMag),
        has_drb_filter: !!(minDrb || maxDrb),
        has_classification_filter: [isRock, isStar, isNearBrightstar, isStationary].some(v => v !== 'any'),
      });
    }

    setCurrentPage(1);
    lastSearchRef.current = { survey: alertSurvey, params: baseParams, searchType };
    await executeSearch(alertSurvey, { ...baseParams, skip: 0 }, searchType);
  }

  async function goToPage(page: number) {
    if (!lastSearchRef.current) return;
    const { survey, params, searchType } = lastSearchRef.current;
    setCurrentPage(page);
    await executeSearch(survey, { ...params, skip: (page - 1) * PAGE_SIZE }, searchType);
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
              <TabsTrigger value="object">Objects</TabsTrigger>
              <TabsTrigger value="alerts">Alerts</TabsTrigger>
            </TabsList>

            <TabsContent value="object">
              <SearchContent
                onResultClick={(result) => { window.location.href = `/objects/${result.survey}/${result.objectId}`; }}
                maxResults={20}
                showFooter={false}
              />
              <div className="text-sm text-muted-foreground mt-2 w-full text-end">
                Showing up to 20 results.
              </div>
            </TabsContent>

            <TabsContent value="alerts">
              <form onSubmit={submitAlertSearch} className="space-y-4">
                <Tabs value={alertSearchTab} onValueChange={(v) => handleAlertSearchTabChange(v as 'filters' | 'object' | 'position')}>
                  <div className="flex flex-col sm:flex-row gap-3">
                    <div className="rounded-lg border bg-muted/40 p-4 flex flex-col gap-3 sm:min-w-48">
                      <span className="text-base font-semibold">Survey</span>
                      <div className="flex h-9 items-center gap-1.5 text-sm">
                        {(['ZTF', 'LSST'] as const).map(s => (
                          <button
                            key={s}
                            type="button"
                            onClick={() => setAlertSurvey(s)}
                            className={cn(
                              "flex h-full flex-1 items-center justify-center rounded-md border px-4 font-medium whitespace-nowrap transition-colors",
                              alertSurvey === s
                                ? "bg-primary text-primary-foreground border-primary"
                                : "border-border text-muted-foreground hover:bg-muted hover:text-foreground"
                            )}
                          >{s}</button>
                        ))}
                      </div>
                    </div>
                    <div className="rounded-lg border bg-muted/40 p-4 flex flex-col gap-3 flex-1">
                      <span className="text-base font-semibold">Query Type</span>
                      <div className="flex h-9 items-center gap-1.5 text-sm">
                        {([['filters', 'Filters'], ['object', 'Object ID'], ['position', 'Position']] as const).map(([val, label]) => (
                          <button
                            key={val}
                            type="button"
                            onClick={() => handleAlertSearchTabChange(val)}
                            className={cn(
                              "flex h-full flex-1 items-center justify-center rounded-md border px-3 font-medium whitespace-nowrap transition-colors",
                              alertSearchTab === val
                                ? "bg-primary text-primary-foreground border-primary"
                                : "border-border text-muted-foreground hover:bg-muted hover:text-foreground"
                            )}
                          >{label}</button>
                        ))}
                      </div>
                    </div>
                  </div>

                  <TabsContent value="filters" className="mt-4">
                    <AlertFilterForm {...filterProps} />
                  </TabsContent>

                  <TabsContent value="object" className="mt-4">
                    <div>
                      <Label htmlFor="alertObjectId" className="text-xs font-medium mb-1 block text-muted-foreground">Object ID</Label>
                      <Input id="alertObjectId" value={alertObjectId} onChange={e => setAlertObjectId(e.target.value)} placeholder="e.g. ZTF25aagbkaj" />
                    </div>
                  </TabsContent>

                  <TabsContent value="position" className="mt-4">
                    <div className="flex flex-col sm:flex-row gap-3">
                      <div className="flex-1">
                        <Label className="text-xs font-medium mb-1 block text-muted-foreground">Position</Label>
                        <PositionInput onChange={setParsedPosition} />
                      </div>
                      <div className="sm:w-36">
                        <Label htmlFor="radius" className="text-xs font-medium mb-1 block text-muted-foreground">Radius (arcsec)</Label>
                        <Input id="radius" type="number" step="any" value={radius} onChange={e => setRadius(e.target.value)} placeholder="5.0" />
                      </div>
                    </div>
                  </TabsContent>
                </Tabs>

                <div className="flex justify-end mt-2">
                  <Button type="submit" disabled={loading}>
                    {loading ? "Searching..." : "Search Alerts"}
                  </Button>
                </div>
              </form>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {activeTab === "alerts" && (
        <AlertSearchResults
          searchResultsRef={searchResultsRef}
          loading={loading}
          error={error}
          alerts={alerts}
          survey={alertSurvey}
          currentPage={currentPage}
          pageSize={PAGE_SIZE}
          onPrev={() => goToPage(currentPage - 1)}
          onNext={() => goToPage(currentPage + 1)}
        />
      )}
    </div>
  );
}
