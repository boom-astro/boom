import { useEffect, useMemo, useRef, useState } from "react";
import { type LiveBin, type LiveIngest as LiveIngestData } from "@/lib/api";
import { formatAgo } from "@/lib/liveIngest";
import { darkIntervals } from "@/lib/sun";
import { SITES, formatClock } from "@/lib/telescopes";

const TICK_MS = 15_000;
const LIVE_MS = 20 * 60_000;
const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const LANE_HEIGHT = 64;
const CUMULATIVE_HEIGHT = 140;
const LATENCY_HEIGHT = 72;
const LATENCY_TICKS: [number, string][] = [
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
  timeZone: string;
  total: number;
  bins: Map<number, LiveBin>;
  max: number;
  night: Interval | null;
  sky: Interval | null;
  lastIngest: number | null;
  latency: number | null;
};

function formatLatency(seconds: number): string {
  if (seconds < 90) return `${Math.max(0, Math.round(seconds))} s`;
  if (seconds < 90 * 60) return `${Math.round(seconds / 60)} min`;
  return `${(seconds / 3600).toFixed(1)} h`;
}

const utc = (ms: number) => formatClock(ms, "UTC");

function status(lane: Lane, now: number): { label: string; live: boolean } {
  if (lane.lastIngest !== null && now - lane.lastIngest < LIVE_MS) return { label: "Live", live: true };
  if (lane.night && now < lane.night.start) return { label: `Dark at ${utc(lane.night.start)}`, live: false };
  if (lane.night && now < lane.night.end) return { label: "Quiet", live: false };
  return { label: lane.total ? "Ended" : "No alerts", live: false };
}

function StatusDot({ live }: { live: boolean }) {
  if (!live) return <span className="bg-muted-foreground/40 size-2 rounded-full" />;
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
        const { lat, lon, timeZone } = telescope.site;
        const bins = new Map(survey.bins.map((bin) => [Date.parse(bin.start), bin]));
        const last = survey.bins[survey.bins.length - 1];
        const ingested = survey.latest?.ingested_at ?? survey.latest?.observed_at;
        return [{
          id: survey.survey,
          label: telescope.name,
          color: telescope.color,
          timeZone,
          total: survey.n_alerts,
          bins,
          max: Math.max(0, ...survey.bins.map((bin) => bin.n_alerts)),
          night: darkIntervals(start, end, lat, lon)[0] ?? null,
          sky: darkIntervals(start, end, lat, lon, 0)[0] ?? null,
          lastIngest: ingested ? Date.parse(ingested) : null,
          latency: last?.latency_s ?? null,
        }];
      });
  }, [data]);

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

  const latency = useMemo(() => {
    const values = lanes.flatMap((lane) =>
      [...lane.bins.values()].flatMap((bin) => (bin.latency_s && bin.latency_s > 0 ? [bin.latency_s] : [])),
    );
    if (!values.length) return null;
    const lo = Math.log10(Math.min(60, ...values));
    const hi = Math.log10(Math.max(600, ...values));
    const y = (seconds: number) =>
      LATENCY_HEIGHT - 4 - ((Math.log10(Math.max(seconds, 10 ** lo)) - lo) / (hi - lo)) * (LATENCY_HEIGHT - 10);
    const x = (ms: number) => ((ms + binMs / 2 - from) / span) * width;
    const series = lanes.map((lane) => {
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
  }, [lanes, from, span, width, binMs]);

  const cumulative = useMemo(() => {
    const starts = lanes.flatMap((lane) => [...lane.bins.keys()]);
    if (!starts.length) return null;
    const first = Math.min(...starts);
    const last = Math.max(...starts) + binMs;
    const running = lanes.map(() => 0);
    const points = [{ t: first, values: [...running] }];
    for (let t = first; t < last; t += binMs) {
      lanes.forEach((lane, i) => {
        running[i] += lane.bins.get(t)?.n_alerts ?? 0;
      });
      points.push({ t: t + binMs, values: [...running] });
    }
    const end = Math.min(to, Math.max(now, last));
    if (end > last) points.push({ t: end, values: [...running] });
    const total = running.reduce((sum, n) => sum + n, 0);
    const x = (ms: number) => ((ms - from) / span) * width;
    const y = (n: number) => CUMULATIVE_HEIGHT - (n / (total || 1)) * (CUMULATIVE_HEIGHT - 8);
    const stacked = (values: number[], n: number) => values.slice(0, n).reduce((sum, v) => sum + v, 0);
    const bands = lanes.map((lane, i) => {
      const upper = points.map((p) => `${x(p.t)},${y(stacked(p.values, i + 1))}`);
      const lower = points.map((p) => `${x(p.t)},${y(stacked(p.values, i))}`).reverse();
      return { lane, area: [...upper, ...lower].join(" "), line: upper.join(" ") };
    }).filter(({ lane }) => lane.total > 0);
    return { total, top: y(total), bands };
  }, [lanes, binMs, from, to, span, width, now]);

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

  function sky(lane: Lane) {
    return (
      <>
        {lane.sky && (
          <div
            className="bg-foreground/[0.035] absolute inset-y-0"
            style={{ left: `${pct(lane.sky.start)}%`, width: `${pct(lane.sky.end) - pct(lane.sky.start)}%` }}
          />
        )}
        {lane.night && (
          <div
            className="bg-foreground/[0.06] absolute inset-y-0"
            style={{ left: `${pct(lane.night.start)}%`, width: `${pct(lane.night.end) - pct(lane.night.start)}%` }}
          />
        )}
      </>
    );
  }

  const hovered = hover
    ? lanes.map((lane) => ({ lane, bin: lane.bins.get(hover.slot) }))
    : [];
  const hoveredTotal = hover
    ? lanes.reduce((sum, lane) =>
      sum + [...lane.bins.entries()].reduce((n, [t, bin]) => (t <= hover.slot ? n + bin.n_alerts : n), 0), 0)
    : 0;
  if (!data) return <div className="shimmer h-87.5 w-full rounded-md" />;

  return (
    <div
      ref={wrapperRef}
      className="relative grid grid-cols-[7.5rem_minmax(0,1fr)_2.5rem] gap-x-3 gap-y-2 select-none sm:grid-cols-[11rem_minmax(0,1fr)_3rem]"
      onPointerLeave={() => setHover(null)}
    >
      {cumulative && (
        <div className="contents">
          <div className="flex min-w-0 flex-col justify-center" style={{ minHeight: CUMULATIVE_HEIGHT }}>
            <span className="text-sm font-medium">All surveys</span>
            <span className="text-xl leading-tight font-semibold tabular-nums sm:text-2xl">
              {cumulative.total.toLocaleString()}
            </span>
            <span className="text-muted-foreground text-xs">alerts tonight</span>
          </div>
          <div
            className="border-border relative border-b"
            style={{ height: CUMULATIVE_HEIGHT }}
            onPointerMove={track}
            role="img"
            aria-label={`${cumulative.total.toLocaleString()} alerts tonight, all surveys`}
          >
            <svg width={width} height={CUMULATIVE_HEIGHT} className="absolute inset-0">
              <line x1={0} x2={width} y1={cumulative.top} y2={cumulative.top} stroke="var(--border)" strokeWidth={1} />
              {cumulative.bands.map(({ lane, area, line }) => (
                <g key={lane.id}>
                  <polygon points={area} fill={lane.color} fillOpacity={0.35} />
                  <polyline points={line} fill="none" stroke={lane.color} strokeWidth={2} strokeLinejoin="round" />
                </g>
              ))}
            </svg>
            {overlay()}
          </div>
          <div className="text-muted-foreground relative text-[11px] tabular-nums">
            <span className="absolute -translate-y-1/2" style={{ top: cumulative.top }}>
              {compact.format(cumulative.total)}
            </span>
          </div>
          <div className="text-muted-foreground col-span-3 pt-3 text-xs">Alerts per {data.bin_minutes} minutes</div>
        </div>
      )}

      {lanes.map((lane) => {
        const { label, live } = status(lane, now);
        const detail = lane.lastIngest !== null
          ? [formatAgo(now - lane.lastIngest), lane.latency !== null ? `${formatLatency(lane.latency)} latency` : null]
            .filter(Boolean).join(" · ")
          : "";
        return (
          <div key={lane.id} className="contents">
            <div className="flex min-w-0 flex-col justify-center" style={{ minHeight: LANE_HEIGHT }}>
              <div className="flex items-center gap-2">
                <span className="size-2.5 shrink-0 rounded-[3px]" style={{ background: lane.color }} />
                <span className="text-sm font-medium">{lane.label}</span>
                <span className={`ml-auto flex items-center gap-1.5 text-xs whitespace-nowrap ${live ? "text-foreground" : "text-muted-foreground"}`}>
                  <StatusDot live={live} />
                  <span className="hidden sm:inline">{label}</span>
                </span>
              </div>
              <span className="text-lg leading-tight font-semibold tabular-nums sm:text-xl">
                {lane.total.toLocaleString()}
              </span>
              <span className="text-muted-foreground truncate text-xs">
                <span className="sm:hidden">{detail ? `${label} · ` : label}</span>
                {detail || "\u00a0"}
              </span>
            </div>
            <div
              className="border-border relative overflow-hidden rounded-sm border-b"
              style={{ height: LANE_HEIGHT }}
              onPointerMove={track}
              role="img"
              aria-label={`${lane.label}: ${lane.total.toLocaleString()} alerts tonight`}
            >
              {sky(lane)}
              {[...lane.bins.entries()].map(([t, bin]) => (
                <div
                  key={t}
                  className="absolute bottom-0 rounded-t-[3px]"
                  style={{
                    left: `${pct(t)}%`,
                    width: `max(1px, calc(${(binMs / span) * 100}% - ${gap}px))`,
                    height: `max(2px, ${(bin.n_alerts / (lane.max || 1)) * 100}%)`,
                    background: lane.color,
                  }}
                />
              ))}
              {overlay()}
            </div>
            <div className="text-muted-foreground pt-0.5 text-[11px] tabular-nums">
              {lane.max > 0 && compact.format(lane.max)}
            </div>
          </div>
        );
      })}

      {latency && (
        <div className="contents">
          <div className="flex flex-col justify-center" style={{ minHeight: LATENCY_HEIGHT }}>
            <span className="text-sm font-medium">Latency</span>
            <span className="text-muted-foreground text-xs">observation to ingestion</span>
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
        </div>
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
          <div className="text-muted-foreground mb-1 tabular-nums">{hoveredTotal.toLocaleString()} alerts so far</div>
          {hovered.map(({ lane, bin }) => (
            <div key={lane.id} className="flex items-center gap-2 py-0.5">
              <span className="size-2.5 shrink-0 rounded-[3px]" style={{ background: lane.color }} />
              <span className="text-muted-foreground">{lane.label}</span>
              <span className="ml-auto font-mono font-medium tabular-nums">
                {bin ? bin.n_alerts.toLocaleString() : "–"}
              </span>
              <span className="text-muted-foreground w-12 text-right tabular-nums">
                {bin?.latency_s ? formatLatency(bin.latency_s) : ""}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
