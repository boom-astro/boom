import { useEffect, useMemo, useRef, useState } from "react";
import { Toggle } from "@/components/ui/toggle";
import { type LiveBin, type LiveIngest as LiveIngestData } from "@/lib/api";
import { SURVEY_COLORS } from "@/lib/constants";
import { sendingSurveys } from "@/lib/liveIngest";
import { darkIntervals } from "@/lib/sun";
import { SITES, formatClock } from "@/lib/telescopes";

const TICK_MS = 15_000;
const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const CUMULATIVE_HEIGHT = 140;
const BATCH_HEIGHT = 120;
const LATENCY_HEIGHT = 96;
const LATENCY_TICKS: [number, string][] = [
  [1, "1 s"],
  [10, "10 s"],
  [60, "1 min"],
  [600, "10 min"],
  [3600, "1 h"],
  [36_000, "10 h"],
];
const ORDER = ["ztf", "lsst", "decam", "winter"];

const TELESCOPES = SITES.flatMap((site) => site.telescopes.map((telescope) => ({ ...telescope, site })));

const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

type Interval = { start: number; end: number };

type Lane = {
  id: string;
  label: string;
  color: string;
  total: number;
  processed: number;
  bins: Map<number, LiveBin>;
  night: Interval | null;
  sky: Interval | null;
};

const utc = (ms: number) => formatClock(ms, "UTC");

function formatLatency(seconds: number): string {
  if (seconds < 90) return `${seconds < 10 ? seconds.toFixed(1) : Math.round(seconds)} s`;
  if (seconds < 90 * 60) return `${Math.round(seconds / 60)} min`;
  return `${(seconds / 3600).toFixed(1)} h`;
}

const sumOf = (values: number[]) => values.reduce((sum, v) => sum + v, 0);

function union(intervals: Interval[]): Interval[] {
  const merged: Interval[] = [];
  for (const interval of [...intervals].sort((a, b) => a.start - b.start)) {
    const last = merged[merged.length - 1];
    if (last && interval.start <= last.end) last.end = Math.max(last.end, interval.end);
    else merged.push({ ...interval });
  }
  return merged;
}

function LiveDot() {
  return (
    <span className="relative flex size-2">
      <span className="absolute inline-flex size-full animate-ping rounded-full bg-emerald-500 opacity-75" />
      <span className="relative inline-flex size-2 rounded-full bg-emerald-500" />
    </span>
  );
}

export default function LiveIngest({ data }: { data: LiveIngestData | null }) {
  const [now, setNow] = useState(Date.now);
  const [width, setWidth] = useState(0);
  const [selected, setSelected] = useState<ReadonlySet<string>>(() => new Set(ORDER));
  const [hover, setHover] = useState<{ slot: number; x: number; y: number; right: number | null } | null>(null);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const axisRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), TICK_MS);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const el = axisRef.current;
    if (!el) return;
    const observer = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width));
    observer.observe(el);
    return () => observer.disconnect();
  }, [data]);

  const binMs = (data?.bin_minutes ?? 10) * MINUTE;

  const lanes = useMemo<Lane[]>(() => {
    if (!data) return [];
    return [...data.surveys]
      .sort((a, b) => ORDER.indexOf(a.survey) - ORDER.indexOf(b.survey))
      .flatMap((survey) => {
        const telescope = TELESCOPES.find((t) => t.id === survey.survey);
        if (!telescope) return [];
        const start = Date.parse(survey.start);
        const end = Date.parse(survey.end);
        const { lat, lon } = telescope.site;
        return [{
          id: survey.survey,
          label: telescope.name,
          color: SURVEY_COLORS[survey.survey as keyof typeof SURVEY_COLORS] ?? telescope.color,
          total: survey.received,
          processed: survey.processed,
          bins: new Map(survey.bins.map((bin) => [Date.parse(bin.start), bin])),
          night: darkIntervals(start, end, lat, lon)[0] ?? null,
          sky: darkIntervals(start, end, lat, lon, 0)[0] ?? null,
        }];
      });
  }, [data]);

  const shown = useMemo(() => lanes.filter((lane) => selected.has(lane.id)), [lanes, selected]);

  const [from, to] = useMemo(() => {
    const edges = lanes.flatMap((lane) => [
      ...(lane.sky ? [lane.sky.start, lane.sky.end] : []),
      ...[...lane.bins.keys()].flatMap((t) => [t, t + binMs]),
    ]);
    if (!edges.length) return [0, 1];
    return [
      Math.floor(Math.min(...edges) / HOUR) * HOUR,
      Math.ceil(Math.max(...edges) / HOUR) * HOUR,
    ];
  }, [lanes, binMs]);

  const span = to - from;
  const pct = (ms: number) => ((ms - from) / span) * 100;
  const slotPx = width * (binMs / span);
  const gap = slotPx > 6 ? 2 : slotPx > 3 ? 1 : 0;
  const nowInView = now >= from && now <= to;

  const ticks = useMemo(() => {
    const hours = Math.round(span / HOUR);
    const step = [1, 2, 3, 4, 6].find((s) => width / (hours / s) >= 48) ?? 6;
    return Array.from({ length: hours + 1 }, (_, i) => from + i * HOUR)
      .filter((t) => new Date(t).getUTCHours() % step === 0);
  }, [from, span, width]);

  const batches = useMemo(() => {
    const slots = [...new Set(shown.flatMap((lane) => [...lane.bins.keys()]))].sort((a, b) => a - b);
    const rows = slots.map((t) => ({
      t,
      parts: shown.map((lane) => ({ lane, n: lane.bins.get(t)?.received ?? 0 })).filter((part) => part.n > 0),
    })).map((row) => ({ ...row, total: sumOf(row.parts.map((part) => part.n)) }));
    const peak = rows.reduce<(typeof rows)[number] | null>((best, row) => (!best || row.total > best.total ? row : best), null);
    return { rows, peak };
  }, [shown]);

  const cumulative = useMemo(() => {
    const starts = lanes.flatMap((lane) => [...lane.bins.keys()]);
    if (!starts.length) return null;
    const first = Math.min(...starts);
    const last = Math.max(...starts) + binMs;
    const running = shown.map(() => 0);
    const points = [{ t: first, values: [...running] }];
    for (let t = first; t < last; t += binMs) {
      shown.forEach((lane, i) => {
        running[i] += lane.bins.get(t)?.received ?? 0;
      });
      points.push({ t: t + binMs, values: [...running] });
    }
    const end = Math.min(to, Math.max(now, last));
    if (end > last) points.push({ t: end, values: [...running] });
    const total = sumOf(running);
    const x = (ms: number) => ((ms - from) / span) * width;
    const y = (n: number) => CUMULATIVE_HEIGHT - (n / (total || 1)) * (CUMULATIVE_HEIGHT - 8);
    const bands = shown.map((lane, i) => {
      const upper = points.map((p) => `${x(p.t)},${y(sumOf(p.values.slice(0, i + 1)))}`);
      const lower = points.map((p) => `${x(p.t)},${y(sumOf(p.values.slice(0, i)))}`).reverse();
      return { lane, area: [...upper, ...lower].join(" "), line: upper.join(" ") };
    }).filter(({ lane }) => lane.total > 0);
    return { total, top: y(total), bands };
  }, [lanes, shown, binMs, from, to, span, width, now]);

  const latency = useMemo(() => {
    const values = shown.flatMap((lane) =>
      [...lane.bins.values()].flatMap((bin) => (bin.latency_s && bin.latency_s > 0 ? [bin.latency_s] : [])),
    );
    if (!values.length) return null;
    const lo = Math.log10(Math.min(1, ...values));
    const hi = Math.log10(Math.max(60, ...values));
    const y = (seconds: number) =>
      LATENCY_HEIGHT - 4 - ((Math.log10(Math.max(seconds, 10 ** lo)) - lo) / (hi - lo)) * (LATENCY_HEIGHT - 10);
    const x = (ms: number) => ((ms + binMs / 2 - from) / span) * width;
    const series = shown.map((lane) => {
      const runs: { t: number; seconds: number }[][] = [];
      for (const [t, bin] of [...lane.bins.entries()].sort(([a], [b]) => a - b)) {
        if (!bin.latency_s || bin.latency_s <= 0) continue;
        const run = runs[runs.length - 1];
        const previous = run?.[run.length - 1];
        if (run && previous && t - previous.t === binMs) run.push({ t, seconds: bin.latency_s });
        else runs.push([{ t, seconds: bin.latency_s }]);
      }
      return { lane, runs };
    });
    const gridlines = LATENCY_TICKS.filter(([seconds]) => {
      const v = Math.log10(seconds);
      return v >= lo && v <= hi;
    });
    return { y, x, series, gridlines };
  }, [shown, from, span, width, binMs]);

  const sending = sendingSurveys(data, now);
  const inProgress = sumOf(shown.map((lane) => lane.total - lane.processed));

  const label = shown.length === lanes.length ? "All surveys" : shown.map((lane) => lane.label).join(" + ") || "No survey";
  const nights = union(shown.flatMap((lane) => (lane.night ? [lane.night] : [])));
  const skies = union(shown.flatMap((lane) => (lane.sky ? [lane.sky] : [])));

  function toggle(id: string) {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function track(e: React.PointerEvent<HTMLDivElement>) {
    const wrapper = wrapperRef.current;
    if (!wrapper) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const ms = from + ((e.clientX - rect.left) / rect.width) * span;
    if (ms < from || ms >= to) return setHover(null);
    const outer = wrapper.getBoundingClientRect();
    const x = e.clientX - outer.left;
    setHover({
      slot: Math.floor(ms / binMs) * binMs,
      x,
      y: e.clientY - outer.top,
      right: x > outer.width * 0.6 ? outer.width - x : null,
    });
  }

  function overlay() {
    return (
      <>
        {hover && (
          <div
            className="bg-foreground/10 pointer-events-none absolute inset-y-0"
            style={{ left: `${pct(hover.slot)}%`, width: `${(binMs / span) * 100}%` }}
          />
        )}
        {nowInView && (
          <div className="bg-destructive pointer-events-none absolute inset-y-0 w-0.5" style={{ left: `${pct(now)}%` }} />
        )}
      </>
    );
  }

  function sky() {
    return (
      <>
        {skies.map((interval) => (
          <div
            key={`sky-${interval.start}`}
            className="bg-foreground/[0.035] absolute inset-y-0"
            style={{ left: `${pct(interval.start)}%`, width: `${pct(interval.end) - pct(interval.start)}%` }}
          />
        ))}
        {nights.map((interval) => (
          <div
            key={`night-${interval.start}`}
            className="bg-foreground/[0.06] absolute inset-y-0"
            style={{ left: `${pct(interval.start)}%`, width: `${pct(interval.end) - pct(interval.start)}%` }}
          />
        ))}
      </>
    );
  }

  if (!data) return <div className="shimmer h-87.5 w-full rounded-md" />;

  const hoveredSoFar = hover
    ? sumOf(shown.map((lane) => sumOf([...lane.bins.entries()].filter(([t]) => t <= hover.slot).map(([, bin]) => bin.received))))
    : 0;

  return (
    <div
      ref={wrapperRef}
      className="relative grid grid-cols-[7.5rem_minmax(0,1fr)_2.5rem] gap-x-3 gap-y-3 select-none sm:grid-cols-[11rem_minmax(0,1fr)_3rem]"
      onPointerLeave={() => setHover(null)}
    >
      <div className="flex min-w-0 flex-col justify-center" style={{ minHeight: CUMULATIVE_HEIGHT }}>
        <span className="truncate text-sm font-medium">{label}</span>
        <span className="text-xl leading-tight font-semibold tabular-nums sm:text-2xl">
          {(cumulative?.total ?? 0).toLocaleString()}
        </span>
        <span className="text-muted-foreground text-xs">
          alerts received tonight{inProgress > 0 ? `, ${inProgress.toLocaleString()} in progress` : ""}
        </span>
        <div className="mt-2 flex flex-wrap gap-1.5">
          {lanes.map((lane) => {
            const on = selected.has(lane.id);
            return (
              <Toggle
                key={lane.id}
                variant="outline"
                size="sm"
                className="h-6 min-w-0 gap-1.5 px-2 text-[11px]"
                pressed={on}
                onPressedChange={() => toggle(lane.id)}
                style={on ? {
                  borderColor: lane.color,
                  color: lane.color,
                  backgroundColor: `color-mix(in oklch, ${lane.color} 15%, transparent)`,
                } : {}}
              >
                {sending.has(lane.id) && <LiveDot />}
                {lane.label}
              </Toggle>
            );
          })}
        </div>
      </div>
      <div
        className="border-border relative border-b"
        style={{ height: CUMULATIVE_HEIGHT }}
        onPointerMove={track}
        role="img"
        aria-label={`${label}: ${(cumulative?.total ?? 0).toLocaleString()} alerts tonight`}
      >
        {sky()}
        {cumulative && (
          <svg width={width} height={CUMULATIVE_HEIGHT} className="absolute inset-0">
            <line x1={0} x2={width} y1={cumulative.top} y2={cumulative.top} stroke="var(--border)" strokeWidth={1} />
            {cumulative.bands.map(({ lane, area, line }) => (
              <g key={lane.id}>
                <polygon points={area} fill={lane.color} fillOpacity={0.35} />
                <polyline points={line} fill="none" stroke={lane.color} strokeWidth={2} strokeLinejoin="round" />
              </g>
            ))}
          </svg>
        )}
        {overlay()}
      </div>
      <div className="text-muted-foreground relative text-[11px] tabular-nums">
        {cumulative && cumulative.total > 0 && (
          <span className="absolute -translate-y-1/2" style={{ top: cumulative.top }}>
            {compact.format(cumulative.total)}
          </span>
        )}
      </div>

      <div className="flex min-w-0 flex-col justify-center" style={{ minHeight: BATCH_HEIGHT }}>
        <span className="text-sm font-medium">Per {data.bin_minutes} minutes</span>
        <span className="text-muted-foreground text-xs">
          {batches.peak ? `peak ${batches.peak.total.toLocaleString()} at ${utc(batches.peak.t)}` : "no alerts yet"}
        </span>
      </div>
      <div
        className="border-border relative overflow-hidden border-b"
        style={{ height: BATCH_HEIGHT }}
        onPointerMove={track}
        role="img"
        aria-label={`${label}: alerts per ${data.bin_minutes} minutes`}
      >
        {sky()}
        {batches.rows.map(({ t, parts, total }) => (
          <div
            key={t}
            className="absolute bottom-0 flex flex-col-reverse overflow-hidden rounded-t-[3px]"
            style={{
              left: `${pct(t)}%`,
              width: `max(1px, calc(${(binMs / span) * 100}% - ${gap}px))`,
              height: `max(2px, ${(total / (batches.peak?.total || 1)) * 100}%)`,
            }}
          >
            {parts.map(({ lane, n }, i) => (
              <div
                key={lane.id}
                style={{
                  height: `${(n / total) * 100}%`,
                  background: lane.color,
                  borderTop: i > 0 ? "1px solid var(--card)" : undefined,
                }}
              />
            ))}
          </div>
        ))}
        {overlay()}
      </div>
      <div className="text-muted-foreground pt-0.5 text-[11px] tabular-nums">
        {batches.peak && compact.format(batches.peak.total)}
      </div>

      {latency && (
        <>
          <div className="flex min-w-0 flex-col justify-center" style={{ minHeight: LATENCY_HEIGHT }}>
            <span className="text-sm font-medium">BOOM latency</span>
            <span className="text-muted-foreground text-xs">arrival to enrichment</span>
          </div>
          <div className="relative" style={{ height: LATENCY_HEIGHT }} onPointerMove={track}>
            <svg width={width} height={LATENCY_HEIGHT} className="absolute inset-0 overflow-visible">
              {latency.gridlines.map(([seconds]) => (
                <line
                  key={seconds}
                  x1={0}
                  x2={width}
                  y1={latency.y(seconds)}
                  y2={latency.y(seconds)}
                  stroke="var(--border)"
                  strokeWidth={1}
                />
              ))}
              {latency.series.map(({ lane, runs }) =>
                runs.map((run) =>
                  run.length > 1 ? (
                    <polyline
                      key={`${lane.id}-${run[0].t}`}
                      points={run.map((p) => `${latency.x(p.t)},${latency.y(p.seconds)}`).join(" ")}
                      fill="none"
                      stroke={lane.color}
                      strokeWidth={2}
                      strokeLinejoin="round"
                      strokeLinecap="round"
                    />
                  ) : (
                    <circle
                      key={`${lane.id}-${run[0].t}`}
                      cx={latency.x(run[0].t)}
                      cy={latency.y(run[0].seconds)}
                      r={3}
                      fill={lane.color}
                    />
                  ),
                ),
              )}
            </svg>
            {overlay()}
          </div>
          <div className="text-muted-foreground relative text-[11px] tabular-nums" style={{ height: LATENCY_HEIGHT }}>
            {latency.gridlines.map(([seconds, text]) => (
              <span key={seconds} className="absolute -translate-y-1/2 whitespace-nowrap" style={{ top: latency.y(seconds) }}>
                {text}
              </span>
            ))}
          </div>
        </>
      )}

      <div className="text-muted-foreground flex items-start text-[11px]">UTC</div>
      <div ref={axisRef} className="relative h-5">
        {ticks.filter((t) => !nowInView || Math.abs(t - now) / span * width > 40).map((t) => (
          <span
            key={t}
            className="text-muted-foreground absolute top-0 -translate-x-1/2 text-[11px] whitespace-nowrap tabular-nums"
            style={{ left: `${pct(t)}%` }}
          >
            {utc(t)}
          </span>
        ))}
        {nowInView && (
          <span
            className="text-destructive absolute top-0 -translate-x-1/2 bg-card px-1 text-[11px] font-medium tabular-nums"
            style={{ left: `${pct(now)}%` }}
          >
            {utc(now)}
          </span>
        )}
      </div>
      <div />

      {hover && (
        <div
          className="bg-background border-border/50 pointer-events-none absolute z-10 min-w-44 rounded-lg border px-2.5 py-1.5 text-xs shadow-xl"
          style={{
            top: hover.y + 12,
            ...(hover.right !== null ? { right: hover.right + 12 } : { left: hover.x + 12 }),
          }}
        >
          <div className="font-medium tabular-nums">
            {utc(hover.slot)} – {utc(hover.slot + binMs)} UTC
          </div>
          <div className="text-muted-foreground mb-1 tabular-nums">{hoveredSoFar.toLocaleString()} alerts so far</div>
          {shown.map((lane) => {
            const bin = lane.bins.get(hover.slot);
            return (
              <div key={lane.id} className="flex items-center gap-2 py-0.5">
                <span className="size-2.5 shrink-0 rounded-[3px]" style={{ background: lane.color }} />
                <span className="text-muted-foreground">{lane.label}</span>
                <span className="ml-auto font-mono font-medium tabular-nums">
                  {bin ? bin.received.toLocaleString() : "–"}
                </span>
                <span className="text-muted-foreground w-12 text-right tabular-nums">
                  {bin?.latency_s ? formatLatency(bin.latency_s) : ""}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
