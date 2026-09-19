import { useState, useEffect, useMemo, useRef } from "react";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Bar, BarChart, CartesianGrid, ReferenceArea, XAxis, YAxis } from "recharts";
import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from "@/components/ui/chart";
import { Toggle } from "@/components/ui/toggle";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import { IconInfoCircle, IconRefresh, IconZoomReset } from "@tabler/icons-react";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import api, { CollectionEntry, fetchTopics, NightlyStat, type TopicInfo } from "@/lib/api";
import { type Survey } from "@/lib/constants";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch.tsx";
import { Label } from "@/components/ui/label.tsx";
import KafkaAlertCounts from "@/components/kafka/KafkaAlertCounts.tsx";
import { toast } from "sonner";

const SURVEY_ORDER = ["ztf", "lsst"] as const satisfies readonly Survey[];

const SURVEY_COLORS: Record<Survey, string> = {
  ztf: "var(--chart-1)",
  lsst: "var(--chart-2)",
};

const chartConfig = {
  ztf: { label: "ZTF", color: SURVEY_COLORS.ztf },
  lsst: { label: "LSST", color: SURVEY_COLORS.lsst },
} satisfies ChartConfig;

const FIRST_NIGHT = "2018-01-01";

// The API refuses to recount a longer range in one refresh.
const MAX_REFRESH_MONTHS = 6;

const NIGHT_CONVENTION =
  "Alerts are grouped by observing night, local noon to local noon at the " +
  "observatory (Palomar, UTC−7, for ZTF; Cerro Pachón, UTC−3, for LSST). " +
  "A night is labeled by its evening date.";

const ALERT_TYPE_LABELS: Record<string, string> = {
  alerts: "alerts",
  alerts_aux: "objects",
  alerts_cutouts: "alert cutouts",
};

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function monthsBefore(date: string, months: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCMonth(d.getUTCMonth() - months);
  return formatDate(d);
}

function twoMonthsAgo(): string {
  const d = new Date();
  d.setMonth(d.getMonth() - 2);
  return formatDate(d);
}

const parseNight = (date: string) => new Date(`${date}T00:00:00`);

const morningAfter = (evening: Date) =>
  new Date(evening.getFullYear(), evening.getMonth(), evening.getDate() + 1);

const nightDay = (date: string) => String(parseNight(date).getDate());

const nightMonth = (date: string) =>
  parseNight(date).toLocaleDateString("en-US", { month: "short" });

function formatNightRange(date: string): string {
  const evening = parseNight(date);
  const morning = morningAfter(evening);
  const from = evening.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  const to =
    morning.getMonth() === evening.getMonth()
      ? String(morning.getDate())
      : morning.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  return `${from} → ${to}`;
}

function formatNightRangeLong(date: string): string {
  return `${formatNightRange(date)}, ${parseNight(date).getFullYear()}`;
}

function describeNight(date: string): string {
  const evening = parseNight(date);
  const morning = morningAfter(evening);
  const weekday = (d: Date) => d.toLocaleDateString("en-US", { weekday: "short" });
  return `${weekday(evening)} → ${weekday(morning)}`;
}

function formatCount(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1000) return `${(v / 1000).toFixed(0)}k`;
  return String(v);
}

function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined) return "";
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const val = bytes / Math.pow(1024, i);
  return `${val < 10 ? val.toFixed(1) : Math.round(val)} ${units[i]}`;
}

const isAlertCollection = (name: string) =>
  name.startsWith("ZTF_") || name.startsWith("LSST_");

function alertCollectionLabel(name: string): string {
  const m = name.match(/^(ZTF|LSST)_(.+)$/);
  if (!m) return name;
  return `${m[1]} ${ALERT_TYPE_LABELS[m[2]] ?? m[2]}`;
}

function StatCard({ label, value, hint }: { label: string; value: string; hint: string }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardDescription>{label}</CardDescription>
        <CardTitle className="text-2xl">{value}</CardTitle>
        <p className="text-muted-foreground text-xs">{hint}</p>
      </CardHeader>
    </Card>
  );
}

function NightInput({ value, max, onChange }: {
  value: string;
  max: string;
  onChange: (value: string) => void;
}) {
  return (
    <Input
      type="date"
      value={value}
      min={FIRST_NIGHT}
      max={max}
      onChange={(e) => onChange(e.target.value)}
      className="w-37 h-8 text-xs"
    />
  );
}

function CollectionsCard({ title, description, collections, formatName, nameClassName }: {
  title: string;
  description: string;
  collections: CollectionEntry[];
  formatName?: (name: string) => string;
  nameClassName?: string;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
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
            {collections.map((c) => (
              <TableRow key={c.name}>
                <TableCell className={nameClassName ?? "text-sm"}>
                  {formatName ? formatName(c.name) : c.name}
                </TableCell>
                <TableCell className="text-right tabular-nums">{formatBytes(c.size_bytes) || "-"}</TableCell>
                <TableCell className="text-right tabular-nums">{c.count?.toLocaleString()}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

export default function Dashboard() {
  const todayUTC = formatDate(new Date());

  const [surveys, setSurveys] = useState<Set<Survey>>(new Set(SURVEY_ORDER));
  const [startDate, setStartDate] = useState(twoMonthsAgo);
  const [endDate, setEndDate] = useState(todayUTC);
  const [statsData, setStatsData] = useState<NightlyStat[]>([]);
  const [collections, setCollections] = useState<CollectionEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);

  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [splitByMatch, setSplitByMatch] = useState(false);
  const [topicsLoading, setTopicsLoading] = useState(true);
  const [topicsError, setTopicsError] = useState<string | null>(null);

  const [zoomLeft, setZoomLeft] = useState<string | null>(null);
  const [zoomRight, setZoomRight] = useState<string | null>(null);
  const [zoomSlice, setZoomSlice] = useState<[number, number] | null>(null);
  const selectingRef = useRef(false);

  const chartRef = useRef<HTMLDivElement>(null);
  const [chartWidth, setChartWidth] = useState(0);

  useEffect(() => {
    setLoading(true);
    setError(null);
    api.fetchStats(startDate, endDate)
      .then(setStatsData)
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to fetch stats"))
      .finally(() => setLoading(false));
  }, [startDate, endDate, reloadKey]);

  useEffect(() => {
    api.fetchCollectionStats()
      .then((s) => setCollections(s.collections.sort((a, b) => a.name.localeCompare(b.name))))
      .catch(() => {});
  }, [reloadKey]);

  useEffect(() => {
    setTopicsLoading(true);
    setTopicsError(null);
    fetchTopics()
      .then(setTopics)
      .catch((e) => setTopicsError(e instanceof Error ? e.message : "Failed to fetch topics"))
      .finally(() => setTopicsLoading(false));
  }, [reloadKey]);

  useEffect(() => {
    const el = chartRef.current;
    if (!el) return;
    const observer = new ResizeObserver(([entry]) => setChartWidth(entry.contentRect.width));
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const visibleData = useMemo(() =>
      statsData.map((d) => ({
        date: d.date,
        ...(surveys.has("ztf") ? {ztf: d.ztf} : {}),
        ...(surveys.has("lsst") ? {lsst: d.lsst} : {}),
      })),
    [statsData, surveys]);

  const chartData = useMemo(() =>
      zoomSlice ? visibleData.slice(zoomSlice[0], zoomSlice[1] + 1) : visibleData,
    [visibleData, zoomSlice]);

  // Recharts drops colliding ticks one at a time, leaving the days unevenly spaced.
  const dayTicks = useMemo(() => {
    const fits = Math.max(2, Math.floor((chartWidth - 60) / 22));
    const step = Math.max(1, Math.ceil(chartData.length / fits));
    return chartData.filter((_, i) => i % step === 0).map((d) => d.date);
  }, [chartData, chartWidth]);

  const monthTicks = useMemo(() => {
    const months = new Map<string, string[]>();
    for (const d of chartData) {
      const key = d.date.slice(0, 7);
      months.set(key, [...(months.get(key) ?? []), d.date]);
    }
    const groups = [...months.values()];
    return groups
      .filter((dates) => groups.length === 1 || dates.length >= 4)
      .map((dates) => dates[Math.floor((dates.length - 1) / 2)]);
  }, [chartData]);

  const stats = useMemo(() => {
    let total = 0;
    let nights = 0;
    let peak: { date: string; total: number } | null = null;
    for (const d of visibleData) {
      const n = (d.ztf ?? 0) + (d.lsst ?? 0);
      total += n;
      if (n > 0) nights += 1;
      if (!peak || n > peak.total) peak = {date: d.date, total: n};
    }
    return {total, nights, avg: nights ? Math.round(total / nights) : 0, peak};
  }, [visibleData]);

  async function refreshCaches() {
    setRefreshing(true);
    const from = monthsBefore(endDate, MAX_REFRESH_MONTHS);
    try {
      await api.refreshStats(startDate > from ? startDate : from, endDate);
      setReloadKey((k) => k + 1);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Failed to refresh the dashboard");
    } finally {
      setRefreshing(false);
    }
  }

  function toggleSurvey(s: Survey) {
    setSurveys(prev => new Set(prev.has(s) ? [...prev].filter(x => x !== s) : [...prev, s]));
  }

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

  const busy = loading || refreshing;
  const isLoggedIn = !!api.getTokenRecord();

  return (
    <div className="px-4 lg:px-6 space-y-4">
      <div className="flex items-center justify-between gap-4">
        <h1 className="text-2xl font-bold">Dashboard</h1>
        {isLoggedIn && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Button variant="outline" size="sm" onClick={refreshCaches} disabled={busy}>
                <IconRefresh className={busy ? "animate-spin" : ""} />
                Refresh
              </Button>
            </TooltipTrigger>
            <TooltipContent side="left" className="max-w-xs">
              Drop the cached stats and recount the collections, the Kafka topics, and the displayed
              nights, up to the last {MAX_REFRESH_MONTHS} months.
            </TooltipContent>
          </Tooltip>
        )}
      </div>
      {visibleData.length > 0 ? (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <StatCard
            label="Total Alerts"
            value={stats.total.toLocaleString()}
            hint={`${stats.nights.toLocaleString()} nights with alerts`}
          />
          <StatCard
            label="Avg / Night"
            value={stats.avg.toLocaleString()}
            hint="nights without alerts excluded"
          />
          <StatCard
            label="Peak Night"
            value={stats.peak?.total ? stats.peak.total.toLocaleString() : "-"}
            hint={stats.peak?.total ? `night of ${formatNightRange(stats.peak.date)}` : "no alerts in range"}
          />
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
            <div className="space-y-1.5">
              <CardTitle className="flex items-center gap-1.5">
                Alerts per Night
                <Tooltip>
                  <TooltipTrigger asChild>
                    <IconInfoCircle className="text-muted-foreground size-4 cursor-help" />
                  </TooltipTrigger>
                  <TooltipContent side="right" className="max-w-xs">{NIGHT_CONVENTION}</TooltipContent>
                </Tooltip>
              </CardTitle>
              <CardDescription>
                {chartData.length} nights{zoomSlice ? " — zoomed" : ""}
              </CardDescription>
            </div>
            <div className="flex flex-wrap items-center gap-6">
              <div className="flex flex-wrap items-center gap-3">
                {SURVEY_ORDER.map((s) => (
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
                <span className="text-muted-foreground text-sm">Nights</span>
                <NightInput value={startDate} max={todayUTC} onChange={setStartDate} />
                <span className="text-muted-foreground text-sm">to</span>
                <NightInput value={endDate} max={todayUTC} onChange={setEndDate} />
              </div>
            </div>
          </div>
          {error && <p className="text-sm text-destructive">{error}</p>}
        </CardHeader>
        <CardContent className="relative" ref={chartRef}>
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
          {loading ? (
            <div className="h-87.5 w-full shimmer" />
          ) : (
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
                  ticks={dayTicks}
                  interval={0}
                  tickFormatter={nightDay}
                />
                <XAxis
                  dataKey="date"
                  xAxisId="month"
                  ticks={monthTicks}
                  interval={0}
                  tickLine={false}
                  axisLine={false}
                  tickMargin={0}
                  height={18}
                  tick={{ style: { fill: "var(--foreground)" }, fontSize: 11 }}
                  tickFormatter={nightMonth}
                />
                <YAxis
                  tickLine={false}
                  axisLine={false}
                  tickMargin={8}
                  tickFormatter={formatCount}
                />
                <ChartTooltip
                  content={
                    <ChartTooltipContent
                      labelFormatter={(_, payload) => {
                        const date: string | undefined = payload?.[0]?.payload?.date;
                        if (!date) return "";
                        return (
                          <div className="space-y-0.5">
                            <div>{formatNightRangeLong(date)}</div>
                            <div className="text-muted-foreground font-normal">{describeNight(date)}</div>
                          </div>
                        );
                      }}
                    />
                  }
                />
                {SURVEY_ORDER.filter((s) => surveys.has(s)).map((s) => (
                  <Bar key={s} dataKey={s} fill={`var(--color-${s})`} radius={[2, 2, 0, 0]}/>
                ))}
                {zoomLeft && zoomRight && (
                  <ReferenceArea x1={zoomLeft} x2={zoomRight} strokeOpacity={0.3} fill="hsl(var(--accent))" fillOpacity={0.3} />
                )}
              </BarChart>
            </ChartContainer>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Alert Counts by Kafka Topic</CardTitle>
          <CardDescription className="flex justify-between items-center flex-wrap gap-y-2">
            <span>
              More information about the topics on the <a href="/docs/kafka" className="underline">Kafka documentation page</a>.
            </span>
            <span className="flex items-center gap-2">
              <Switch id="split-by-match" checked={splitByMatch} onCheckedChange={setSplitByMatch} />
              <Label htmlFor="split-by-match" className="text-sm font-normal cursor-pointer">
                Split by match
              </Label>
            </span>
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <KafkaAlertCounts topics={topics} loading={topicsLoading} error={topicsError} splitByMatch={splitByMatch} />
        </CardContent>
      </Card>

      <CollectionsCard
        title="Catalogs"
        description={`${collections.length} catalogs available`}
        collections={collections.filter((c) => !isAlertCollection(c.name))}
        nameClassName="font-mono text-sm"
      />

      <CollectionsCard
        title="Alert Collections"
        description="ZTF and LSST collections"
        collections={collections.filter((c) => isAlertCollection(c.name))}
        formatName={alertCollectionLabel}
      />
    </div>
  );
}
