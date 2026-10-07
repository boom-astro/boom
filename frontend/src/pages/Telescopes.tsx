import { useCallback, useEffect, useRef, useState } from "react";
import { IconClock, IconInfoCircle, IconLock, IconMoonStars, IconSun, IconSunset2 } from "@tabler/icons-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import NightMap from "@/components/telescopes/NightMap";
import NightTimeline from "@/components/telescopes/NightTimeline";
import api, { type NightlyStat } from "@/lib/api";
import { CATALOG_COLOR, CATALOGS, type Coverage } from "@/lib/coverage";
import { nextCrossing, skyState, sunAltitudeAt, type SkyState } from "@/lib/sun";
import { formatClock, NIGHT_COLOR, SITES, type Site, type Telescope } from "@/lib/telescopes";

const DAY_MS = 86_400_000;
const STATS_REFRESH_MS = 10 * 60_000;

const STATES: Record<SkyState, { title: string; icon: typeof IconSun }> = {
  night: { title: "Night", icon: IconMoonStars },
  twilight: { title: "Twilight", icon: IconSunset2 },
  day: { title: "Day", icon: IconSun },
};

const OBSERVING_CONVENTION =
  "A survey observes once the Sun is 12° below the horizon at its observatory. " +
  "Drag the timeline, or focus it and use the arrow keys, to move through time.";

const NIGHT_CONVENTION =
  "Alerts are grouped by observing night, local noon to local noon at the observatory. " +
  "A night is labeled by its evening date.";

const COVERAGE_CONVENTION =
  "Click a telescope or a catalog to show the sky it covers. The map shades the places where " +
  "that part of the sky is overhead at the selected time, so footprints that follow right ascension " +
  "drift west as the Earth turns.";

const TELESCOPES = SITES.flatMap((site) => site.telescopes.map((telescope) => ({ site, telescope })));

const COVERAGES = new Map<string, Coverage>([
  ...TELESCOPES.map(({ site, telescope }): [string, Coverage] => [telescope.id, {
    id: telescope.id,
    color: telescope.color,
    origin: [site.lon, site.lat],
    footprint: telescope.footprint,
  }]),
  ...CATALOGS.map((catalog): [string, Coverage] => [catalog.id, {
    id: catalog.id,
    color: CATALOG_COLOR,
    origin: null,
    footprint: catalog.footprint,
  }]),
]);

const utcFormat = new Intl.DateTimeFormat("en-US", {
  timeZone: "UTC",
  weekday: "short",
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});

function formatDuration(ms: number): string {
  const minutes = Math.round(Math.abs(ms) / 60_000);
  const days = Math.floor(minutes / 1440);
  const hours = Math.floor((minutes % 1440) / 60);
  if (days) return `${days}d ${hours}h`;
  return hours ? `${hours}h ${String(minutes % 60).padStart(2, "0")}m` : `${minutes % 60}m`;
}

function formatOffset(ms: number): string {
  if (Math.abs(ms) < 60_000) return "now";
  return ms > 0 ? `${formatDuration(ms)} from now` : `${formatDuration(ms)} ago`;
}

function useClock() {
  const timeRef = useRef(Date.now());
  const [time, setTime] = useState(timeRef.current);
  const [live, setLive] = useState(true);

  useEffect(() => {
    if (!live) return;
    const tick = () => {
      timeRef.current = Date.now();
      setTime(timeRef.current);
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [live]);

  const seek = useCallback((ms: number) => {
    timeRef.current = ms;
    setTime(ms);
    setLive(false);
  }, [timeRef]);

  return { timeRef, time, live, goLive: () => setLive(true), seek };
}

function siteState(site: Site, ms: number): SkyState {
  return skyState(sunAltitudeAt(ms, site.lat, site.lon));
}

function InfoTooltip({ children }: { children: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <IconInfoCircle className="text-muted-foreground size-4 cursor-help" />
      </TooltipTrigger>
      <TooltipContent side="right" className="max-w-xs">{children}</TooltipContent>
    </Tooltip>
  );
}

function PrivateCount({ name }: { name: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <IconLock className="text-muted-foreground inline-block size-4 cursor-help" />
      </TooltipTrigger>
      <TooltipContent side="left">{name} alert stream is private.</TooltipContent>
    </Tooltip>
  );
}

function SkyBadge({ state }: { state: SkyState }) {
  const { title, icon: Icon } = STATES[state];
  return (
    <Badge
      variant="outline"
      className="text-muted-foreground px-1.5"
      style={state === "night" ? {
        borderColor: NIGHT_COLOR,
        color: NIGHT_COLOR,
        backgroundColor: `color-mix(in oklch, ${NIGHT_COLOR} 15%, transparent)`,
      } : undefined}
    >
      <Icon />
      {title}
    </Badge>
  );
}

function CoverageChip({ name, color, detail, selected, onClick }: {
  name: string;
  color: string;
  detail: string;
  selected: boolean;
  onClick: () => void;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-pressed={selected}
          onClick={onClick}
          className="hover:bg-accent flex cursor-pointer items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs transition-colors"
          style={selected ? {
            borderColor: color,
            backgroundColor: `color-mix(in oklch, ${color} 18%, transparent)`,
          } : undefined}
        >
          <span className="size-1.5 shrink-0 rounded-full" style={{ backgroundColor: color }} />
          {name}
        </button>
      </TooltipTrigger>
      <TooltipContent side="bottom">{detail}</TooltipContent>
    </Tooltip>
  );
}

function CoverageLegend({ selected, onSelect }: { selected: string | null; onSelect: (id: string) => void }) {
  return (
    <div className="mt-4 space-y-2">
      <div className="flex flex-wrap items-center gap-1.5 sm:hidden">
        <span className="text-muted-foreground mr-1 text-xs">Telescopes</span>
        {TELESCOPES.map(({ telescope }) => (
          <CoverageChip
            key={telescope.id}
            name={telescope.name}
            color={telescope.color}
            detail={`${telescope.survey} · ${telescope.extent}`}
            selected={selected === telescope.id}
            onClick={() => onSelect(telescope.id)}
          />
        ))}
      </div>
      <div className="flex flex-wrap items-center gap-1.5">
        <span className="text-muted-foreground mr-1 flex items-center gap-1 text-xs">
          Crossmatched catalogs
          <InfoTooltip>{COVERAGE_CONVENTION}</InfoTooltip>
        </span>
        {CATALOGS.map((catalog) => (
          <CoverageChip
            key={catalog.id}
            name={catalog.name}
            color={CATALOG_COLOR}
            detail={`${catalog.description} · ${catalog.extent}`}
            selected={selected === catalog.id}
            onClick={() => onSelect(catalog.id)}
          />
        ))}
      </div>
    </div>
  );
}

function Legend() {
  return (
    <div className="text-muted-foreground flex flex-wrap items-center gap-3 pt-0.5 text-xs">
      <span className="flex items-center gap-1.5">
        <span className="inline-block size-3 shrink-0 rounded-full bg-amber-300" />
        Sun
      </span>
      <span className="flex items-center gap-1.5">
        <span className="inline-block w-3 shrink-0 border-t border-amber-500" />
        Sunset line
      </span>
      <span className="flex items-center gap-1.5">
        <span className="inline-block w-3 shrink-0 border-t border-dashed border-indigo-400" />
        Sun 12° below the horizon
      </span>
    </div>
  );
}

export default function Telescopes() {
  const { timeRef, time, live, goLive, seek } = useClock();
  const [nights, setNights] = useState<NightlyStat[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const select = useCallback((id: string) => setSelected((current) => (current === id ? null : id)), []);

  useEffect(() => {
    const load = () => {
      const now = Date.now();
      const start = new Date(now - 2 * DAY_MS).toISOString().slice(0, 10);
      const end = new Date(now).toISOString().slice(0, 10);
      api.fetchStats(start, end).then(setNights).catch(() => {});
    };
    load();
    const id = setInterval(load, STATS_REFRESH_MS);
    return () => clearInterval(id);
  }, []);

  const now = Date.now();
  const alerts = (telescope: Telescope, nightsAgo: number) => {
    const tonight = nights.findIndex((night) => {
      const window = night.windows?.[telescope.id];
      return window !== undefined && Date.parse(window.start) <= now && now < Date.parse(window.end);
    });
    return tonight < nightsAgo ? undefined : nights[tonight - nightsAgo][telescope.id];
  };

  const states = new Map(SITES.map((site) => [site.id, siteState(site, time)]));
  const changes = new Map(SITES.map((site) => [site.id, nextCrossing(time, site.lat, site.lon)]));
  const counted = TELESCOPES.filter(({ telescope }) => alerts(telescope, 0) !== undefined);
  const tonight = counted.reduce((n, { telescope }) => n + (alerts(telescope, 0) ?? 0), 0);

  return (
    <div className="px-4 lg:px-6 space-y-4">
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="space-y-1.5">
              <CardTitle>Telescopes</CardTitle>
              <CardDescription>Observatories whose alerts BOOM ingests</CardDescription>
            </div>
            <Legend />
          </div>
        </CardHeader>
        <CardContent>
          <NightMap
            sites={SITES}
            timeRef={timeRef}
            time={time}
            coverage={selected ? COVERAGES.get(selected) ?? null : null}
            onSelect={select}
          />
          <CoverageLegend selected={selected} onSelect={select} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div className="space-y-1.5">
              <CardTitle className="flex items-center gap-1.5">
                Night Windows
                <InfoTooltip>{OBSERVING_CONVENTION}</InfoTooltip>
              </CardTitle>
              <CardDescription className="tabular-nums">
                {utcFormat.format(time)} UTC · {live ? "live" : formatOffset(time - now)}
              </CardDescription>
            </div>
            {!live && (
              <Button variant="outline" size="sm" onClick={goLive}>
                <IconClock />
                Now
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          <NightTimeline sites={SITES} timeRef={timeRef} time={time} onSeek={seek} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-1.5">
            Telescopes
            <InfoTooltip>{NIGHT_CONVENTION}</InfoTooltip>
          </CardTitle>
          <CardDescription>
            {TELESCOPES.length} telescopes at {SITES.length} observatories
          </CardDescription>
          {counted.length > 0 && (
            <CardAction className="text-right">
              <div className="text-2xl font-semibold tabular-nums">{tonight.toLocaleString()}</div>
              <div className="text-muted-foreground text-xs">alerts tonight</div>
            </CardAction>
          )}
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Telescope</TableHead>
                <TableHead>Observatory</TableHead>
                <TableHead>Local time</TableHead>
                <TableHead>Sky</TableHead>
                <TableHead>Next change</TableHead>
                <TableHead className="text-right">Tonight</TableHead>
                <TableHead className="text-right">Last night</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {TELESCOPES.map(({ site, telescope }) => {
                const state = states.get(site.id)!;
                const change = changes.get(site.id);
                return (
                  <TableRow key={telescope.id}>
                    <TableCell>
                      <div className="flex items-start gap-2">
                        <span
                          className="mt-1.5 size-2.5 shrink-0 rounded-full"
                          style={{ backgroundColor: telescope.color }}
                        />
                        <div>
                          <div className="font-medium">
                            {telescope.name}
                            <span className="text-muted-foreground ml-2 text-xs font-normal">
                              {telescope.survey}
                            </span>
                          </div>
                          <div className="text-muted-foreground text-xs">{telescope.instrument}</div>
                        </div>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div>{site.name}</div>
                      <div className="text-muted-foreground text-xs">{site.place}</div>
                    </TableCell>
                    <TableCell className="tabular-nums">{formatClock(time, site.timeZone, true)}</TableCell>
                    <TableCell>
                      <SkyBadge state={state} />
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {change != null ? (
                        <>
                          <div>
                            {state === "night" ? "Dark until" : "Dark from"} {formatClock(change, site.timeZone, true)}
                          </div>
                          <div className="text-muted-foreground text-xs">in {formatDuration(change - time)}</div>
                        </>
                      ) : "-"}
                    </TableCell>
                    {[0, 1].map((nightsAgo) => {
                      const count = alerts(telescope, nightsAgo);
                      return (
                        <TableCell key={nightsAgo} className="text-right tabular-nums">
                          {count !== undefined
                            ? count.toLocaleString()
                            : telescope.private ? <PrivateCount name={telescope.name} /> : "-"}
                        </TableCell>
                      );
                    })}
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
