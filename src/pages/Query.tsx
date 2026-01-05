import { useState, FormEvent, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { Select, SelectTrigger, SelectContent, SelectItem, SelectValue } from "@/components/ui/select";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import api, { Alert, AlertSearchParams, Cutouts } from "@/lib/api";
import { bytes2image } from "@/lib/imageProcessing";
import { Skeleton } from "@/components/ui/skeleton";

export default function Query() {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<"object" | "alerts">("object");
  
  // Object lookup state
  const [survey, setSurvey] = useState('ZTF');
  const [objectId, setObjectId] = useState('');

  // Alert search state
  const [alertSurvey, setAlertSurvey] = useState('ZTF');
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

  function submitObjectSearch(e?: FormEvent) {
    e?.preventDefault();
    if (!survey || !objectId) return;
    navigate(`/objects/${encodeURIComponent(survey)}/${encodeURIComponent(objectId)}`);
  }

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
    
    if (hasObjectId) {
      params.object_id = alertObjectId.trim();
    } else {
      params.ra = parseFloat(ra);
      params.dec = parseFloat(dec);
      params.radius_arcsec = parseFloat(radius);
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

    try {
      const results = await api.fetchAlerts(alertSurvey, params);
      setAlerts(results);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch alerts");
    } finally {
      setLoading(false);
    }
  }

  const gotoExample = () => navigate(`/objects/${encodeURIComponent('ZTF')}/${encodeURIComponent('ZTF25abxeyzt')}`);

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
              <form onSubmit={submitObjectSearch} className="grid grid-cols-1 sm:grid-cols-3 gap-3 items-start">
                <div className="sm:col-span-1">
                  <Label className="text-xs font-medium mb-1 block text-muted-foreground">Survey</Label>
                  <Select value={survey} onValueChange={(v) => setSurvey(v)}>
                    <SelectTrigger className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="ZTF">ZTF</SelectItem>
                      <SelectItem value="LSST">LSST</SelectItem>
                      {/* <SelectItem value="Decam">Decam</SelectItem> */}
                    </SelectContent>
                  </Select>
                </div>

                <div className="sm:col-span-2">
                  <Label className="text-xs font-medium mb-1 block text-muted-foreground">Object ID</Label>
                  <Input value={objectId} onChange={e => setObjectId(e.target.value)} placeholder="Object ID (e.g. ZTF25aagbkaj)" />
                </div>

                <div className="sm:col-span-3 mt-2 text-sm text-muted-foreground">Tip: try the example object if you're exploring the UI.</div>

                <div className="sm:col-span-3 flex justify-end gap-2 mt-1">
                  <Button variant="ghost" onClick={gotoExample} type="button">Open example</Button>
                  <Button type="submit">Open</Button>
                </div>
              </form>
            </TabsContent>

            <TabsContent value="alerts">
              <form onSubmit={submitAlertSearch} className="space-y-4">
                <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
                  <div className="sm:col-span-4">
                    <Label className="text-xs font-medium mb-1 block text-muted-foreground">Survey</Label>
                    <Select value={alertSurvey} onValueChange={(v) => setAlertSurvey(v)}>
                      <SelectTrigger className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="ZTF">ZTF</SelectItem>
                        <SelectItem value="LSST">LSST</SelectItem>
                        {/* <SelectItem value="Decam">Decam</SelectItem> */}
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
        <Card className="max-w-6xl mx-auto">
          <CardHeader>
            <CardTitle>Search Results</CardTitle>
            <CardDescription>
              {loading ? "Loading..." : alerts.length > 0 ? `Found ${alerts.length} alert${alerts.length !== 1 ? 's' : ''}` : error ? "Error" : "No results yet"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {error && (
              <div className="text-red-500 text-sm">{error}</div>
            )}
            
            {loading && (
              <div className="space-y-4">
                {[1, 2, 3].map((i) => (
                  <div key={i} className="flex gap-4 p-4 border rounded-lg">
                    <Skeleton className="h-32 w-32 flex-shrink-0" />
                    <div className="flex-1 space-y-2">
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
        </Card>
      )}
    </div>
  )
}

function AlertCard({ alert, survey }: { alert: Alert; survey: string }) {
  const navigate = useNavigate();
  const [cutouts, setCutouts] = useState<Cutouts | null>(null);
  const [loadingCutouts, setLoadingCutouts] = useState(true);

  const drb = alert.candidate.drb ?? alert.candidate.reliability ?? null;

  const scienceImage = cutouts ? bytes2image(cutouts.cutout_science, "science") : null;
  const templateImage = cutouts ? bytes2image(cutouts.cutout_template, "template") : null;
  const differenceImage = cutouts ? bytes2image(cutouts.cutout_difference, "difference") : null;

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

  const handleClick = () => {
    navigate(`/objects/${encodeURIComponent(survey)}/${encodeURIComponent(alert.objectId || '')}`);
  };

  return (
    <div 
      className="flex gap-4 p-4 border rounded-lg hover:bg-accent/50 cursor-pointer transition-colors"
      onClick={handleClick}
    >
      {/* Cutouts */}
      <div className="flex gap-2 flex-shrink-0">
        {loadingCutouts ? (
          <div className="h-24 w-24 bg-muted rounded flex items-center justify-center text-xs text-muted-foreground">
            Loading...
          </div>
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
            <span className="text-muted-foreground">DRB/Reliability:</span> <span className="font-mono">{typeof drb === 'number' ? drb.toFixed(3) : drb}</span>
          </div>
        )}
        {alert.candidate.magpsf !== undefined && typeof alert.candidate.magpsf === 'number' && (
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
}
