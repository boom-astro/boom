import { useEffect, useMemo, useRef, useState } from "react";
import {
  geoCircle,
  geoGraticule10,
  geoNaturalEarth1,
  geoPath,
  type GeoPermissibleObjects,
  type GeoProjection,
  type GeoSphere,
} from "d3-geo";
import type { LineString } from "geojson";
import { feature } from "topojson-client";
import landUrl from "world-atlas/land-110m.json?url";
import {
  NIGHT_SUN_ALTITUDE,
  skyState,
  subsolarPoint,
  sunAltitude,
  wrapLongitude,
} from "@/lib/sun";
import { type Site } from "@/lib/telescopes";
import boomLogo from "@/assets/boom-logo.png";

const DEG = Math.PI / 180;
const SPHERE: GeoSphere = { type: "Sphere" };
const MASK_CELL = 4;
const REPAINT_SIM_MS = 20_000;
const DAYLIGHT_EDGES = [Math.sin(-16 * DEG), Math.sin(3 * DEG)] as const;
const RINGS = 3;
const RING_PERIOD_MS = 2600;
const FLOW_PX_PER_SECOND = 70;
const FLOW_DOT_SPACING = 56;

const PALETTES = {
  dark: {
    nightOcean: "#050816",
    dayOcean: "#14335a",
    nightLand: "rgba(148, 163, 184, 0.3)",
    dayLand: "rgba(226, 232, 240, 0.9)",
    nightGrid: "rgba(148, 163, 184, 0.06)",
    dayGrid: "rgba(226, 232, 240, 0.1)",
    outline: "rgba(148, 163, 184, 0.25)",
    sunGlow: "rgba(255, 190, 110, 0.22)",
    terminator: "rgba(251, 191, 36, 0.45)",
    darkEdge: "rgba(129, 140, 248, 0.55)",
    markerEdge: "rgba(255, 255, 255, 0.9)",
    flow: "rgba(148, 163, 184, 0.45)",
  },
  light: {
    nightOcean: "#0b1433",
    dayOcean: "#dbe7f6",
    nightLand: "rgba(148, 163, 184, 0.42)",
    dayLand: "rgba(51, 65, 85, 0.7)",
    nightGrid: "rgba(148, 163, 184, 0.07)",
    dayGrid: "rgba(51, 65, 85, 0.07)",
    outline: "rgba(100, 116, 139, 0.35)",
    sunGlow: "rgba(255, 170, 80, 0.28)",
    terminator: "rgba(217, 119, 6, 0.6)",
    darkEdge: "rgba(129, 140, 248, 0.7)",
    markerEdge: "rgba(255, 255, 255, 0.95)",
    flow: "rgba(71, 85, 105, 0.4)",
  },
};

type Palette = typeof PALETTES.dark;

type LandTopology = Parameters<typeof feature>[0];

type Point = [number, number];

type Flow = { from: Point; control: Point; to: Point; length: number };

type Scene = {
  width: number;
  height: number;
  dpr: number;
  projection: GeoProjection;
  points: Point[];
  flows: Flow[];
  night: HTMLCanvasElement;
  day: HTMLCanvasElement;
  work: HTMLCanvasElement;
  base: HTMLCanvasElement;
  mask: HTMLCanvasElement;
  maskPixels: ImageData;
  sinLat: Float32Array;
  cosLat: Float32Array;
  sinLon: Float32Array;
  cosLon: Float32Array;
};

function createCanvas(width: number, height: number): HTMLCanvasElement {
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  return canvas;
}

function smoothstep(edge0: number, edge1: number, x: number): number {
  const t = Math.min(1, Math.max(0, (x - edge0) / (edge1 - edge0)));
  return t * t * (3 - 2 * t);
}

function finiteOr(value: number, fallback: number): number {
  return Number.isFinite(value) ? value : fallback;
}

function ring(center: [number, number], radius: number): LineString {
  return { type: "LineString", coordinates: geoCircle().center(center).radius(radius)().coordinates[0] };
}

function flowTo(from: Point, to: Point): Flow {
  const dx = to[0] - from[0];
  const dy = to[1] - from[1];
  const length = Math.hypot(dx, dy) || 1;
  const side = dx / length > 0 ? -1 : 1;
  const bend = length * 0.18 * side;
  return {
    from,
    control: [(from[0] + to[0]) / 2 - (dy / length) * bend, (from[1] + to[1]) / 2 + (dx / length) * bend],
    to,
    length,
  };
}

function alongFlow({ from, control, to }: Flow, t: number): Point {
  const u = 1 - t;
  return [
    u * u * from[0] + 2 * u * t * control[0] + t * t * to[0],
    u * u * from[1] + 2 * u * t * control[1] + t * t * to[1],
  ];
}

function fitProjection(width: number): { projection: GeoProjection; height: number } {
  const height = Math.ceil(geoPath(geoNaturalEarth1().fitWidth(width, SPHERE)).bounds(SPHERE)[1][1]);
  const projection = geoNaturalEarth1().fitExtent([[1, 1], [width - 1, height - 1]], SPHERE);
  return { projection, height };
}

function landDots(projection: GeoProjection, land: GeoPermissibleObjects, width: number, height: number) {
  const g = createCanvas(width, height).getContext("2d", { willReadFrequently: true })!;
  g.beginPath();
  geoPath(projection, g)(land);
  g.fill();
  const pixels = g.getImageData(0, 0, width, height).data;
  const spacing = Math.min(7, Math.max(3.5, width / 190));
  const dots: number[] = [];
  for (let row = 0, y = spacing / 2; y < height; row++, y += spacing * 0.87) {
    for (let x = row % 2 ? spacing : spacing / 2; x < width; x += spacing) {
      if (pixels[(Math.floor(y) * width + Math.floor(x)) * 4 + 3] > 127) dots.push(x, y);
    }
  }
  return { dots, radius: spacing * 0.28 };
}

function buildScene(width: number, land: GeoPermissibleObjects, sites: Site[], palette: Palette): Scene {
  const dpr = Math.max(1, window.devicePixelRatio || 1);
  const { projection, height } = fitProjection(width);
  const { dots, radius } = landDots(projection, land, width, height);

  const layer = (ocean: string, grid: string, dot: string) => {
    const canvas = createCanvas(Math.round(width * dpr), Math.round(height * dpr));
    const g = canvas.getContext("2d")!;
    g.scale(dpr, dpr);
    const path = geoPath(projection, g);
    g.beginPath();
    path(SPHERE);
    g.fillStyle = ocean;
    g.fill();
    g.beginPath();
    path(geoGraticule10());
    g.strokeStyle = grid;
    g.lineWidth = 0.5;
    g.stroke();
    g.beginPath();
    for (let i = 0; i < dots.length; i += 2) {
      g.moveTo(dots[i] + radius, dots[i + 1]);
      g.arc(dots[i], dots[i + 1], radius, 0, 2 * Math.PI);
    }
    g.fillStyle = dot;
    g.fill();
    return canvas;
  };

  const maskWidth = Math.ceil(width / MASK_CELL);
  const maskHeight = Math.ceil(height / MASK_CELL);
  const cells = maskWidth * maskHeight;
  const sinLat = new Float32Array(cells);
  const cosLat = new Float32Array(cells);
  const sinLon = new Float32Array(cells);
  const cosLon = new Float32Array(cells);
  for (let j = 0; j < maskHeight; j++) {
    for (let i = 0; i < maskWidth; i++) {
      const k = j * maskWidth + i;
      const [lon, lat] = projection.invert!([(i + 0.5) * MASK_CELL, (j + 0.5) * MASK_CELL]) ?? [0, 0];
      const phi = Math.max(-90, Math.min(90, finiteOr(lat, 0))) * DEG;
      const lambda = Math.max(-180, Math.min(180, finiteOr(lon, 0))) * DEG;
      sinLat[k] = Math.sin(phi);
      cosLat[k] = Math.cos(phi);
      sinLon[k] = Math.sin(lambda);
      cosLon[k] = Math.cos(lambda);
    }
  }
  const mask = createCanvas(maskWidth, maskHeight);
  const maskPixels = new ImageData(maskWidth, maskHeight);
  maskPixels.data.fill(255);

  const points = sites.map((site): Point => projection([site.lon, site.lat]) ?? [0, 0]);
  const hub: Point = [width / 2, height / 2];

  return {
    width,
    height,
    dpr,
    projection,
    points,
    flows: points.map((point) => flowTo(point, hub)),
    night: layer(palette.nightOcean, palette.nightGrid, palette.nightLand),
    day: layer(palette.dayOcean, palette.dayGrid, palette.dayLand),
    work: createCanvas(Math.round(width * dpr), Math.round(height * dpr)),
    base: createCanvas(Math.round(width * dpr), Math.round(height * dpr)),
    mask,
    maskPixels,
    sinLat,
    cosLat,
    sinLon,
    cosLon,
  };
}

function paintBase(scene: Scene, palette: Palette, ms: number) {
  const { width, height, dpr, projection } = scene;
  const sun = subsolarPoint(ms);
  const sinDec = Math.sin(sun.lat * DEG);
  const cosDec = Math.cos(sun.lat * DEG);
  const sinSunLon = Math.sin(sun.lon * DEG);
  const cosSunLon = Math.cos(sun.lon * DEG);
  const data = scene.maskPixels.data;
  for (let k = 0; k < scene.sinLat.length; k++) {
    const sinAltitude =
      scene.sinLat[k] * sinDec +
      scene.cosLat[k] * cosDec * (scene.cosLon[k] * cosSunLon + scene.sinLon[k] * sinSunLon);
    data[k * 4 + 3] = 255 * smoothstep(DAYLIGHT_EDGES[0], DAYLIGHT_EDGES[1], sinAltitude);
  }
  scene.mask.getContext("2d")!.putImageData(scene.maskPixels, 0, 0);

  const work = scene.work.getContext("2d")!;
  work.globalCompositeOperation = "copy";
  work.drawImage(scene.day, 0, 0);
  work.globalCompositeOperation = "destination-in";
  work.imageSmoothingQuality = "high";
  work.drawImage(scene.mask, 0, 0, scene.mask.width * MASK_CELL * dpr, scene.mask.height * MASK_CELL * dpr);

  const base = scene.base.getContext("2d")!;
  base.setTransform(1, 0, 0, 1, 0, 0);
  base.globalCompositeOperation = "copy";
  base.drawImage(scene.night, 0, 0);
  base.globalCompositeOperation = "source-over";
  base.drawImage(scene.work, 0, 0);
  base.setTransform(dpr, 0, 0, dpr, 0, 0);

  const path = geoPath(projection, base);
  const [sunX, sunY] = projection([sun.lon, sun.lat]) ?? [0, 0];
  base.save();
  base.beginPath();
  path(SPHERE);
  base.clip();
  const glow = base.createRadialGradient(sunX, sunY, 0, sunX, sunY, width * 0.22);
  glow.addColorStop(0, palette.sunGlow);
  glow.addColorStop(1, "rgba(255, 190, 110, 0)");
  base.fillStyle = glow;
  base.fillRect(0, 0, width, height);

  const antisolar: [number, number] = [wrapLongitude(sun.lon + 180), -sun.lat];
  base.lineWidth = 1;
  base.strokeStyle = palette.terminator;
  base.beginPath();
  path(ring(antisolar, 90));
  base.stroke();
  base.setLineDash([3, 4]);
  base.strokeStyle = palette.darkEdge;
  base.beginPath();
  path(ring(antisolar, 90 + NIGHT_SUN_ALTITUDE));
  base.stroke();
  base.setLineDash([]);

  const disc = base.createRadialGradient(sunX, sunY, 0, sunX, sunY, 16);
  disc.addColorStop(0, "rgba(255, 247, 222, 1)");
  disc.addColorStop(0.28, "rgba(253, 186, 116, 0.95)");
  disc.addColorStop(1, "rgba(251, 146, 60, 0)");
  base.fillStyle = disc;
  base.beginPath();
  base.arc(sunX, sunY, 16, 0, 2 * Math.PI);
  base.fill();
  base.restore();

  base.beginPath();
  path(SPHERE);
  base.strokeStyle = palette.outline;
  base.stroke();
}

function resolveColors(container: HTMLElement, sites: Site[]): string[][] {
  const probe = document.createElement("span");
  container.appendChild(probe);
  const colors = sites.map((site) =>
    site.telescopes.map((telescope) => {
      probe.style.color = telescope.color;
      return getComputedStyle(probe).color;
    }),
  );
  probe.remove();
  return colors;
}

export default function NightMap({ sites, timeRef, time }: {
  sites: Site[];
  timeRef: React.RefObject<number>;
  time: number;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [land, setLand] = useState<GeoPermissibleObjects | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch(landUrl)
      .then((res) => res.json() as Promise<LandTopology>)
      .then((topology) => !cancelled && setLand(feature(topology, topology.objects.land)))
      .catch(() => !cancelled && setLand({ type: "FeatureCollection", features: [] }));
    return () => {
      cancelled = true;
    };
  }, []);

  const labels = useMemo(() => {
    const { projection, height } = fitProjection(1000);
    return sites.map((site) => {
      const [x, y] = projection([site.lon, site.lat]) ?? [0, 0];
      return { site, left: x / 10, top: (y / height) * 100 };
    });
  }, [sites]);

  useEffect(() => {
    const container = containerRef.current;
    const view = canvasRef.current;
    if (!land || !container || !view) return;
    const ctx = view.getContext("2d")!;
    const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    let palette = PALETTES.dark;
    let colors: string[][] = [];
    let scene: Scene | null = null;
    let paintedAt = Number.NaN;
    let raf = 0;

    const rebuild = () => {
      const width = Math.floor(container.clientWidth);
      if (width < 50) return;
      palette = document.documentElement.classList.contains("dark") ? PALETTES.dark : PALETTES.light;
      colors = resolveColors(container, sites);
      scene = buildScene(width, land, sites, palette);
      view.width = scene.base.width;
      view.height = scene.base.height;
      view.style.height = `${scene.height}px`;
      paintedAt = Number.NaN;
    };

    function paintFlow(flow: Flow, siteColors: string[], now: number) {
      ctx.globalAlpha = 1;
      ctx.strokeStyle = palette.flow;
      ctx.lineWidth = 0.75;
      ctx.setLineDash([2, 3]);
      ctx.beginPath();
      ctx.moveTo(...flow.from);
      ctx.quadraticCurveTo(...flow.control, ...flow.to);
      ctx.stroke();
      ctx.setLineDash([]);
      if (reducedMotion) return;
      const travelMs = (flow.length / FLOW_PX_PER_SECOND) * 1000;
      const dots = Math.max(2, Math.round(flow.length / FLOW_DOT_SPACING));
      siteColors.forEach((color, j) => {
        ctx.fillStyle = color;
        for (let k = 0; k < dots; k++) {
          const t = (now / travelMs + (k + j / siteColors.length) / dots) % 1;
          const [x, y] = alongFlow(flow, t);
          ctx.globalAlpha = Math.min(1, t * 8, (1 - t) * 8);
          ctx.beginPath();
          ctx.arc(x, y, 1.3, 0, 2 * Math.PI);
          ctx.fill();
        }
      });
      ctx.globalAlpha = 1;
    }

    function paintSites(scene: Scene, now: number) {
      const sun = subsolarPoint(timeRef.current);
      const states = sites.map((site) => skyState(sunAltitude(sun, site.lat, site.lon)));
      states.forEach((state, i) => state === "night" && paintFlow(scene.flows[i], colors[i], now));

      sites.forEach((_, i) => {
        const [x, y] = scene.points[i];
        const siteColors = colors[i];
        const state = states[i];
        const intensity = state === "night" ? 1 : state === "twilight" ? 0.35 : 0;

        if (intensity > 0) {
          for (let k = 0; k < RINGS; k++) {
            const phase = reducedMotion ? 0.15 + k / RINGS : (now / RING_PERIOD_MS + k / RINGS) % 1;
            ctx.globalAlpha = intensity * (1 - phase) ** 2;
            ctx.strokeStyle = siteColors[k % siteColors.length];
            ctx.lineWidth = 1.5;
            ctx.beginPath();
            ctx.arc(x, y, 6 + phase * 30, 0, 2 * Math.PI);
            ctx.stroke();
          }
        }

        ctx.globalAlpha = intensity > 0 ? 1 : 0.55;
        const slice = (2 * Math.PI) / siteColors.length;
        siteColors.forEach((color, k) => {
          ctx.fillStyle = color;
          ctx.beginPath();
          ctx.moveTo(x, y);
          ctx.arc(x, y, 5, -Math.PI / 2 + k * slice, -Math.PI / 2 + (k + 1) * slice);
          ctx.closePath();
          ctx.fill();
        });
        ctx.globalAlpha = 1;
        ctx.lineWidth = 1.25;
        ctx.strokeStyle = palette.markerEdge;
        ctx.beginPath();
        ctx.arc(x, y, 5.5, 0, 2 * Math.PI);
        ctx.stroke();
      });
    }

    function frame(now: number) {
      raf = requestAnimationFrame(frame);
      if (!scene) return;
      const ms = timeRef.current;
      if (!(Math.abs(ms - paintedAt) < REPAINT_SIM_MS)) {
        paintBase(scene, palette, ms);
        paintedAt = ms;
      }
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.globalCompositeOperation = "copy";
      ctx.drawImage(scene.base, 0, 0);
      ctx.globalCompositeOperation = "source-over";
      ctx.setTransform(scene.dpr, 0, 0, scene.dpr, 0, 0);
      paintSites(scene, now);
    }

    rebuild();
    raf = requestAnimationFrame(frame);

    let lastWidth = container.clientWidth;
    const resizeObserver = new ResizeObserver(() => {
      if (Math.abs(container.clientWidth - lastWidth) < 1) return;
      lastWidth = container.clientWidth;
      rebuild();
    });
    resizeObserver.observe(container);
    const themeObserver = new MutationObserver(rebuild);
    themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });

    return () => {
      cancelAnimationFrame(raf);
      resizeObserver.disconnect();
      themeObserver.disconnect();
    };
  }, [land, sites, timeRef]);

  const sun = subsolarPoint(time);
  const receiving = sites.some((site) => skyState(sunAltitude(sun, site.lat, site.lon)) === "night");

  return (
    <div ref={containerRef} className="relative">
      <canvas ref={canvasRef} className="block w-full" style={{ aspectRatio: "1000 / 520" }} />
      {labels.map(({ site, left, top }) => (
        <div
          key={site.id}
          className="pointer-events-none absolute mr-4 hidden -translate-y-1/2 sm:block"
          style={{ right: `${100 - left}%`, top: `${top}%` }}
        >
          <div className="bg-background/80 text-foreground rounded-md border px-2 py-1 text-[11px] leading-tight shadow-lg backdrop-blur-sm">
            <div className="font-medium">{site.name}</div>
            {site.telescopes.map((telescope) => (
              <div key={telescope.id} className="mt-0.5 flex items-center gap-1">
                <span className="size-1.5 rounded-full" style={{ backgroundColor: telescope.color }} />
                {telescope.name}
              </div>
            ))}
          </div>
        </div>
      ))}
      <div className="pointer-events-none absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2">
        <div className="bg-background/40 absolute -inset-4 rounded-full backdrop-blur-[3px] [mask-image:radial-gradient(closest-side,black_55%,transparent)] sm:-inset-6" />
        <div
          className={`absolute -inset-1 rounded-full blur-lg transition-colors duration-700 ${receiving ? "bg-indigo-400/60" : "bg-indigo-400/25"}`}
        />
        {receiving && (
          <div className="absolute inset-0 animate-ping rounded-full ring-2 ring-indigo-300/60 [animation-duration:2.4s]" />
        )}
        <img
          src={boomLogo}
          alt="BOOM"
          className="relative size-8 max-w-none rounded-full shadow-2xl ring-2 ring-white/90 sm:size-11 lg:size-14"
        />
      </div>
    </div>
  );
}
