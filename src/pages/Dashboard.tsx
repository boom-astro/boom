import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Bar, BarChart, CartesianGrid, ReferenceArea, XAxis, YAxis } from "recharts";
import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from "@/components/ui/chart";
import { Toggle } from "@/components/ui/toggle";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import { IconZoomReset } from "@tabler/icons-react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import api, { CollectionEntry, NightlyStat, type TopicInfo } from "@/lib/api";
import { SURVEYS, type Survey } from "@/lib/utils";
import { Switch } from "@/components/ui/switch.tsx";
import { Label } from "@/components/ui/label.tsx";
import KafkaAlertCounts from "@/components/kafka/KafkaAlertCounts.tsx";

const SURVEY_COLORS: Record<string, string> = {
  ztf: "var(--chart-1)",
  lsst: "var(--chart-2)",
};

const chartConfig = {
  ztf: { label: "ZTF", color: SURVEY_COLORS.ztf },
  lsst: { label: "LSST", color: SURVEY_COLORS.lsst },
} satisfies ChartConfig;

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined) return "";
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const val = bytes / Math.pow(1024, i);
  return `${val < 10 ? val.toFixed(1) : Math.round(val)} ${units[i]}`;
}

export default function Dashboard() {
  const todayUTC = formatDate(new Date());
  const defaultEnd = new Date();
  const defaultStart = new Date();
  defaultStart.setMonth(defaultStart.getMonth() - 2);

  const [surveys, setSurveys] = useState<Set<Survey>>(new Set(SURVEYS));
  const [startDate, setStartDate] = useState(formatDate(defaultStart));
  const [endDate, setEndDate] = useState(formatDate(defaultEnd));
  const [statsData, setStatsData] = useState<NightlyStat[]>([]);
  const [collections, setCollections] = useState<CollectionEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Kafka topics state
  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [splitByMatch, setSplitByMatch] = useState(false);
  const [topicsLoading, setTopicsLoading] = useState(true);
  const [topicsError, setTopicsError] = useState<string | null>(null);

  // Zoom: drag-select on chart to zoom, double-click to reset
  const [zoomLeft, setZoomLeft] = useState<string | null>(null);
  const [zoomRight, setZoomRight] = useState<string | null>(null);
  const selectingRef = useRef(false);
  const [zoomSlice, setZoomSlice] = useState<[number, number] | null>(null);

  function toggleSurvey(s: Survey) {
    setSurveys(prev => new Set(prev.has(s) ? [...prev].filter(x => x !== s) : [...prev, s]));
  }

  const loadStats = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await api.fetchStats(startDate, endDate);
      setStatsData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch stats");
    } finally {
      setLoading(false);
    }
  }, [startDate, endDate]);

  useEffect(() => {
    loadStats();
  }, [loadStats]);

  useEffect(() => {
    api.fetchCollectionStats()
      .then((s) => setCollections(s.collections.sort((a, b) => a.name.localeCompare(b.name))))
      .catch(() => {});
  }, []);

  useEffect(() => {
    api.fetchTopics()
      .then(setTopics)
      .catch((e) => setTopicsError(e instanceof Error ? e.message : "Failed to fetch topics"))
      .finally(() => setTopicsLoading(false));
  }, []);

  const visibleData = useMemo(() => {
    if (surveys.has("ztf") && surveys.has("lsst")) return statsData;
    return statsData.map((d) => ({
      date: d.date,
      ...(surveys.has("ztf") ? {ztf: d.ztf} : {}),
      ...(surveys.has("lsst") ? {lsst: d.lsst} : {}),
    }));
  }, [statsData, surveys]);

  const chartData = useMemo(() => {
    if (!zoomSlice) return visibleData;
    return visibleData.slice(zoomSlice[0], zoomSlice[1] + 1);
  }, [visibleData, zoomSlice]);

  function startZoomSelection(e: { activeLabel?: string }) {
    if (e?.activeLabel) {
      selectingRef.current = true;
      setZoomLeft(e.activeLabel);
      setZoomRight(null);
    }
  }

  function updateZoomSelection(e: { activeLabel?: string }) {
    if (selectingRef.current && e?.activeLabel) {
      setZoomRight(e.activeLabel);
    }
  }

  function zoomIntoSelection() {
    if (selectingRef.current && zoomLeft && zoomRight && zoomLeft !== zoomRight) {
      const dates = visibleData.map(d => d.date);
      let li = dates.indexOf(zoomLeft);
      let ri = dates.indexOf(zoomRight);
      if (li > ri) [li, ri] = [ri, li];
      if (li >= 0 && ri >= 0 && ri - li >= 1) {
        setZoomSlice([li, ri]);
      }
    }
    selectingRef.current = false;
    setZoomLeft(null);
    setZoomRight(null);
  }

  function resetZoom() {
    setZoomSlice(null);
  }

  const totalAlerts = useMemo(() =>
      visibleData.reduce((s, d) => s + (d.ztf ?? 0) + (d.lsst ?? 0), 0),
    [visibleData]);
  const nbNightsWithAlerts = useMemo(() =>
      visibleData.filter(d => (d.ztf ?? 0) + (d.lsst ?? 0) > 0).length,
    [visibleData]);
  const avgAlerts = useMemo(() =>
      nbNightsWithAlerts ? Math.round(totalAlerts / nbNightsWithAlerts) : 0,
    [totalAlerts, nbNightsWithAlerts]);
  const maxNight = useMemo(() =>
      visibleData.reduce<{ date: string; total: number } | null>((max, d) => {
        const total = (d.ztf ?? 0) + (d.lsst ?? 0);
        return !max || total > max.total ? {date: d.date, total} : max;
      }, null),
    [visibleData]);

  const isAlertCollection = (name: string) =>
    name.startsWith("ZTF_") || name.startsWith("LSST_");

  const ALERT_TYPE_LABELS: Record<string, string> = {
    alerts: "alerts",
    alerts_aux: "objects",
    alerts_cutouts: "alert cutouts",
  };

  function parseAlertCollection(name: string): { survey: string; type: string } {
    const m = name.match(/^(ZTF|LSST)_(.+)$/);
    if (!m) return { survey: "", type: name };
    return { survey: m[1], type: ALERT_TYPE_LABELS[m[2]] ?? m[2] };
  }

  return (
    <div className="px-4 lg:px-6 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Dashboard</h1>
      </div>
      {visibleData.length > 0 ? (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardDescription>Total Alerts</CardDescription>
              <CardTitle className="text-2xl">{totalAlerts.toLocaleString()}</CardTitle>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardDescription>Avg / Night</CardDescription>
              <CardTitle className="text-2xl">{avgAlerts.toLocaleString()}</CardTitle>
            </CardHeader>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardDescription>Peak Night</CardDescription>
              <CardTitle className="text-2xl">
                {maxNight?.total ? `${maxNight.total.toLocaleString()} (${maxNight.date})` : "-"}
              </CardTitle>
            </CardHeader>
          </Card>
        </div>
      ) : (
        <Card>
          <CardContent className="py-8 text-center text-muted-foreground">
            No data...
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader className="space-y-3">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <CardTitle>Alerts per Night
              </CardTitle>
              <CardDescription>
                ({chartData.length} nights{zoomSlice ? " — zoomed" : ""})
              </CardDescription>
            </div>
            <div className="flex flex-wrap items-center gap-6">
              <div className="flex flex-wrap items-center gap-3">
                {(["ztf", "lsst"] as const).map((s) => (
                  <Toggle
                    key={s}
                    variant="outline"
                    size="sm"
                    pressed={surveys.has(s)}
                    onPressedChange={() => toggleSurvey(s)}
                    style={surveys.has(s) ? {
                      borderColor: SURVEY_COLORS[s],
                      color: SURVEY_COLORS[s],
                      backgroundColor: `color-mix(in oklch, ${SURVEY_COLORS[s]} 15%, transparent)`,
                    } : {}}
                  >
                    {s.toUpperCase()}
                  </Toggle>
                ))}
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <Input
                  type="date"
                  value={startDate}
                  min="2018-01-01"
                  max={todayUTC}
                  onChange={(e) => setStartDate(e.target.value)}
                  className="w-37 h-8 text-xs"
                />
                <span className="text-muted-foreground text-sm">to</span>
                <Input
                  type="date"
                  value={endDate}
                  min="2018-01-01"
                  max={todayUTC}
                  onChange={(e) => setEndDate(e.target.value)}
                  className="w-37 h-8 text-xs"
                />
              </div>
            </div>
          </div>
          {error && <p className="text-sm text-destructive">{error}</p>}
        </CardHeader>
        <CardContent className="relative">
          {zoomSlice && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Toggle
                  onPressedChange={resetZoom}
                  className="absolute top-0 right-8 z-1"
                  aria-label="Reset zoom"
                >
                  <IconZoomReset className="w-4 h-4" />
                </Toggle>
              </TooltipTrigger>
              <TooltipContent side="left">Reset zoom</TooltipContent>
            </Tooltip>
          )}
          {!loading ? (
            <ChartContainer config={chartConfig} className="h-87.5 w-full select-none">
              <BarChart
                data={chartData}
                margin={{top: 4, right: 4, bottom: 0, left: 4}}
                onMouseDown={startZoomSelection}
                onMouseMove={updateZoomSelection}
                onMouseUp={zoomIntoSelection}
                onMouseLeave={zoomIntoSelection}
                onDoubleClick={resetZoom}
              >
                <CartesianGrid vertical={false}/>
                <XAxis
                  dataKey="date"
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  minTickGap={32}
                  tickFormatter={(v: string) => {
                    const d = new Date(v + "T00:00:00");
                    return d.toLocaleDateString("en-US", {month: "short", day: "numeric"});
                  }}
                />
                <YAxis
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  tickFormatter={(v: number) => v >= 1_000_000 ? `${(v / 1_000_000).toFixed(1)}M` : v >= 1000 ? `${(v / 1000).toFixed(0)}k` : String(v)}
                />
                <ChartTooltip
                  content={
                    <ChartTooltipContent
                      labelFormatter={(_, payload) => {
                        if (!payload?.[0]?.payload?.date) return "";
                        const d = new Date(payload[0].payload.date + "T00:00:00");
                        return d.toLocaleDateString("en-US", {
                          weekday: "short",
                          month: "short",
                          day: "numeric",
                          year: "numeric"
                        });
                      }}
                    />
                  }
                />
                {surveys.has("ztf") && (
                  <Bar dataKey="ztf" fill="var(--color-ztf)" radius={[2, 2, 0, 0]}/>
                )}
                {surveys.has("lsst") && (
                  <Bar dataKey="lsst" fill="var(--color-lsst)" radius={[2, 2, 0, 0]}/>
                )}
                {zoomLeft && zoomRight && (
                  <ReferenceArea x1={zoomLeft} x2={zoomRight} strokeOpacity={0.3} fill="hsl(var(--accent))" fillOpacity={0.3} />
                )}
              </BarChart>
            </ChartContainer>
            ) : <div className="h-87.5 w-full shimmer" />
          }
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Alert Counts by Kafka Topic</CardTitle>
          <CardDescription>
            <div className="flex justify-between items-center flex-wrap gap-y-2">
              <div>
                More information about the topics on the <a href="/docs/kafka" className="underline">Kafka documentation page</a>.
              </div>
              <div className="flex items-center gap-2">
                <Switch
                  id="split-by-match"
                  checked={splitByMatch}
                  onCheckedChange={(v) => setSplitByMatch(v)}
                />
                <Label htmlFor="split-by-match" className="text-sm font-normal cursor-pointer">
                  Split by match
                </Label>
              </div>
            </div>
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <KafkaAlertCounts topics={topics} loading={topicsLoading} error={topicsError} splitByMatch={splitByMatch} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Catalogs</CardTitle>
          <CardDescription>{collections.length} catalogs available</CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead className="text-right">Size</TableHead>
                <TableHead className="text-right">Entries</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {collections.filter(c => !isAlertCollection(c.name)).map((c) => (
                <TableRow key={c.name}>
                  <TableCell className="font-mono text-sm">{c.name}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatBytes(c?.size_bytes) || "-"}</TableCell>
                  <TableCell className="text-right tabular-nums">{c?.count?.toLocaleString() || ""}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Alert Collections</CardTitle>
          <CardDescription>ZTF and LSST collections</CardDescription>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead className="text-right">Size</TableHead>
                <TableHead className="text-right">Entries</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {collections.filter(c => isAlertCollection(c.name)).map((c) => {
                const { survey, type } = parseAlertCollection(c.name);
                return (
                  <TableRow key={c.name}>
                    <TableCell className="text-sm">{survey} {type}</TableCell>
                    <TableCell className="text-right tabular-nums">{formatBytes(c?.size_bytes) || "-"}</TableCell>
                    <TableCell className="text-right tabular-nums">{c?.count?.toLocaleString()}</TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
