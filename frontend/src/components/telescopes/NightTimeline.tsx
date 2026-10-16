import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { darkIntervals, sunAltitudeAt } from "@/lib/sun";
import { formatClock, formatUtcOffset, type Site } from "@/lib/telescopes";

const HOUR = 3_600_000;
const VISIBLE_HOURS = 24;
const STRIP_HOURS = 72;
const RECENTER_HOURS = 6;
const SAMPLE_MS = 10 * 60_000;
const LABELED_NIGHT_HOURS = 3;

type Rgba = [number, number, number, number];

const SKY_COLORS: [number, Rgba][] = [
  [-18, [12, 12, 40, 0.95]],
  [-12, [30, 27, 90, 0.9]],
  [-6, [109, 60, 170, 0.65]],
  [-1, [236, 110, 90, 0.6]],
  [3, [251, 170, 90, 0.4]],
  [12, [56, 189, 248, 0.22]],
];

const tickDate = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" });

function rgba([r, g, b, a]: Rgba): string {
  return `rgba(${Math.round(r)}, ${Math.round(g)}, ${Math.round(b)}, ${a.toFixed(2)})`;
}

function skyColor(altitude: number): string {
  if (altitude <= SKY_COLORS[0][0]) return rgba(SKY_COLORS[0][1]);
  for (let i = 1; i < SKY_COLORS.length; i++) {
    const [high, to] = SKY_COLORS[i];
    if (altitude > high) continue;
    const [low, from] = SKY_COLORS[i - 1];
    const t = (altitude - low) / (high - low);
    return rgba(from.map((v, k) => v + (to[k] - v) * t) as Rgba);
  }
  return rgba(SKY_COLORS[SKY_COLORS.length - 1][1]);
}

function skyGradient(site: Site, start: number): string {
  const samples = (STRIP_HOURS * HOUR) / SAMPLE_MS;
  const colors = Array.from({ length: samples + 1 }, (_, i) =>
    skyColor(sunAltitudeAt(start + i * SAMPLE_MS, site.lat, site.lon)),
  );
  const stops = colors.flatMap((color, i) =>
    color === colors[i - 1] && color === colors[i + 1] ? [] : [`${color} ${((i / samples) * 100).toFixed(2)}%`],
  );
  return `linear-gradient(to right, ${stops.join(", ")})`;
}

const roundToHour = (ms: number) => Math.round(ms / HOUR) * HOUR;

const percent = (ms: number) => `${(ms / (STRIP_HOURS * HOUR)) * 100}%`;

export default function NightTimeline({ sites, timeRef, time, onSeek }: {
  sites: Site[];
  timeRef: React.RefObject<number>;
  time: number;
  onSeek: (ms: number) => void;
}) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const stripRef = useRef<HTMLDivElement>(null);
  const nowRef = useRef<HTMLDivElement>(null);
  const dragRef = useRef<{ x: number; ms: number; moved: boolean } | null>(null);
  const [center, setCenter] = useState(() => roundToHour(timeRef.current));
  const centerRef = useRef(center);
  const start = center - (STRIP_HOURS / 2) * HOUR;

  const rows = useMemo(() => sites.map((site) => ({
    site,
    gradient: skyGradient(site, start),
    nights: darkIntervals(start, start + STRIP_HOURS * HOUR, site.lat, site.lon),
  })), [sites, start]);

  const ticks = useMemo(
    () => Array.from({ length: STRIP_HOURS + 1 }, (_, i) => start + i * HOUR),
    [start],
  );

  const place = useCallback(() => {
    const origin = centerRef.current - (STRIP_HOURS / 2) * HOUR;
    const left = timeRef.current - (VISIBLE_HOURS / 2) * HOUR - origin;
    if (stripRef.current) stripRef.current.style.transform = `translateX(-${percent(left)})`;
    if (nowRef.current) nowRef.current.style.left = percent(Date.now() - origin);
  }, [timeRef]);

  useLayoutEffect(() => {
    centerRef.current = center;
    place();
  }, [center, place]);

  useEffect(() => {
    let raf = 0;
    const frame = () => {
      raf = requestAnimationFrame(frame);
      if (Math.abs(timeRef.current - centerRef.current) > RECENTER_HOURS * HOUR) {
        setCenter(roundToHour(timeRef.current));
      }
      place();
    };
    raf = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(raf);
  }, [place, timeRef]);

  const msPerPixel = () => (VISIBLE_HOURS * HOUR) / (viewportRef.current?.clientWidth || 1);

  function startDrag(e: React.PointerEvent<HTMLDivElement>) {
    e.currentTarget.setPointerCapture(e.pointerId);
    dragRef.current = { x: e.clientX, ms: timeRef.current, moved: false };
  }

  function drag(e: React.PointerEvent<HTMLDivElement>) {
    const d = dragRef.current;
    if (!d) return;
    const dx = e.clientX - d.x;
    if (Math.abs(dx) > 3) d.moved = true;
    if (d.moved) onSeek(d.ms - dx * msPerPixel());
  }

  function endDrag(e: React.PointerEvent<HTMLDivElement>) {
    const d = dragRef.current;
    dragRef.current = null;
    if (!d || d.moved) return;
    const rect = e.currentTarget.getBoundingClientRect();
    onSeek(d.ms + (e.clientX - rect.left - rect.width / 2) * msPerPixel());
  }

  function nudge(e: React.KeyboardEvent<HTMLDivElement>) {
    const step = e.shiftKey ? HOUR : 15 * 60_000;
    if (e.key === "ArrowLeft") onSeek(timeRef.current - step);
    else if (e.key === "ArrowRight") onSeek(timeRef.current + step);
    else return;
    e.preventDefault();
  }

  return (
    <div className="flex gap-3">
      <div className="flex w-24 shrink-0 flex-col gap-1.5 sm:w-48">
        <div className="text-muted-foreground flex h-6 items-end text-[11px]">UTC</div>
        {rows.map(({ site }) => (
          <div key={site.id} className="flex h-10 flex-col justify-center leading-tight">
            <span className="truncate text-sm font-medium">{site.name}</span>
            <span className="text-muted-foreground text-xs tabular-nums">
              {formatClock(time, site.timeZone, true)}
            </span>
          </div>
        ))}
      </div>
      <div
        ref={viewportRef}
        role="slider"
        tabIndex={0}
        aria-label="Time"
        aria-valuemin={time - (VISIBLE_HOURS / 2) * HOUR}
        aria-valuemax={time + (VISIBLE_HOURS / 2) * HOUR}
        aria-valuenow={time}
        aria-valuetext={`${formatClock(time, "UTC")} UTC`}
        className="focus-visible:ring-ring/50 relative min-w-0 flex-1 cursor-grab touch-pan-y overflow-hidden rounded-md outline-none select-none focus-visible:ring-[3px] active:cursor-grabbing"
        onPointerDown={startDrag}
        onPointerMove={drag}
        onPointerUp={endDrag}
        onPointerCancel={() => {
          dragRef.current = null;
        }}
        onKeyDown={nudge}
      >
        <div
          ref={stripRef}
          className="relative flex flex-col gap-1.5 will-change-transform"
          style={{ width: `${(STRIP_HOURS / VISIBLE_HOURS) * 100}%` }}
        >
          <div className="relative h-6">
            {ticks.map((tick) => {
              const hour = new Date(tick).getUTCHours();
              return (
                <div key={tick} className="absolute bottom-0 h-full" style={{ left: percent(tick - start) }}>
                  <div className={`bg-border absolute bottom-0 w-px ${hour % 3 ? "h-1" : "h-2"}`} />
                  {hour % 3 === 0 && (
                    <span
                      className={`text-muted-foreground absolute bottom-2.5 -translate-x-1/2 text-[10px] whitespace-nowrap tabular-nums ${hour % 6 ? "hidden sm:inline" : ""}`}
                    >
                      {hour === 0 ? tickDate.format(tick) : `${String(hour).padStart(2, "0")}:00`}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
          {rows.map(({ site, gradient, nights }) => (
            <div key={site.id} className="relative h-10 rounded-sm" style={{ background: gradient }}>
              {nights.map((night) => (
                <div
                  key={night.start}
                  className="absolute inset-y-1 flex items-center justify-center overflow-hidden rounded-sm border border-indigo-300/25 text-[10px] whitespace-nowrap text-indigo-100/90 tabular-nums"
                  style={{ left: percent(night.start - start), width: percent(night.end - night.start) }}
                >
                  {night.end - night.start >= LABELED_NIGHT_HOURS * HOUR && (
                    <>
                      {formatClock(night.start, site.timeZone)} → {formatClock(night.end, site.timeZone)}
                      <span className="hidden sm:inline">&nbsp;{formatUtcOffset(night.end, site.timeZone)}</span>
                    </>
                  )}
                </div>
              ))}
            </div>
          ))}
          <div
            ref={nowRef}
            title="Now"
            className="border-foreground/50 pointer-events-none absolute top-6 bottom-0 border-l border-dashed"
          />
        </div>
        <div className="bg-foreground pointer-events-none absolute top-6 bottom-0 left-1/2 w-0.5 -translate-x-1/2 rounded-full" />
        <div className="bg-foreground text-background pointer-events-none absolute top-0 left-1/2 -translate-x-1/2 rounded px-1.5 py-0.5 text-[10px] font-medium tabular-nums">
          {formatClock(time, "UTC")}
        </div>
      </div>
    </div>
  );
}
