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
import { followsSiderealTime, footprintCoverage, type Coverage } from "@/lib/coverage";
import {
  NIGHT_SUN_ALTITUDE,
  siderealAngle,
  skyState,
  subsolarPoint,
  sunAltitude,
  wrapLongitude,
} from "@/lib/sun";
import { type Site } from "@/lib/telescopes";

const DEG = Math.PI / 180;
const SPHERE: GeoSphere = { type: "Sphere" };
const MASK_CELL = 4;
const COVER_CELL = 2;
const REPAINT_SIM_MS = 20_000;
const DAYLIGHT_EDGES = [Math.sin(-16 * DEG), Math.sin(3 * DEG)] as const;
const RINGS = 3;
const RING_PERIOD_MS = 2600;
const FLOW_PX_PER_SECOND = 70;
const FLOW_DOT_SPACING = 56;
const REVEAL_MS = 1600;
const PING_MS = 5000;
const FADE_MS = 400;
const COVER_FILL = 0.24;
const COVER_EDGE = 0.55;
const COVER_RIM = 0.75;
const COVER_PING = 0.22;
const COVER_DIM = 0.55;
const COVER_SHADE = [2, 6, 23] as const;
const RIM_WIDTH = 7;
const PING_WIDTH = 9;

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

type ActiveCoverage = { coverage: Coverage; since: number; endedAt?: number };

type CoverageLayer = {
  coverage: Coverage;
  color: string;
  rgb: Uint8ClampedArray;
  distances: Float32Array;
  reach: number;
  values: Float32Array;
  edges: Float32Array;
  limits: LineString[];
  computedAt: number;
};

type Grid = {
  cell: number;
  canvas: HTMLCanvasElement;
  pixels: ImageData;
  lat: Float32Array;
  lon: Float32Array;
  sinLat: Float32Array;
  cosLat: Float32Array;
  sinLon: Float32Array;
  cosLon: Float32Array;
};

type Scene = {
  width: number;
  height: number;
  dpr: number;
  projection: GeoProjection;
  points: Point[];
  hub: Point;
  lift: number;
  flows: Flow[];
  night: HTMLCanvasElement;
  day: HTMLCanvasElement;
  work: HTMLCanvasElement;
  base: HTMLCanvasElement;
  daylight: Grid;
  cover: Grid;
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

function gaussian(x: number): number {
  return Math.exp(-x * x);
}

function parallel(lat: number): LineString {
  return { type: "LineString", coordinates: Array.from({ length: 361 }, (_, i) => [i - 180, lat]) };
}

function ring(center: [number, number], radius: number): LineString {
  return { type: "LineString", coordinates: geoCircle().center(center).radius(radius)().coordinates[0] };
}

function flowTo(from: Point, to: Point): Flow {
  return { from, control: [to[0], from[1]], to, length: Math.hypot(to[0] - from[0], to[1] - from[1]) || 1 };
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

function projectGrid(projection: GeoProjection, width: number, height: number, cell: number): Grid {
  const columns = Math.ceil(width / cell);
  const rows = Math.ceil(height / cell);
  const cells = columns * rows;
  const lat = new Float32Array(cells);
  const lon = new Float32Array(cells);
  const sinLat = new Float32Array(cells);
  const cosLat = new Float32Array(cells);
  const sinLon = new Float32Array(cells);
  const cosLon = new Float32Array(cells);
  for (let j = 0; j < rows; j++) {
    for (let i = 0; i < columns; i++) {
      const k = j * columns + i;
      const [x, y] = projection.invert!([(i + 0.5) * cell, (j + 0.5) * cell]) ?? [0, 0];
      lat[k] = Math.max(-90, Math.min(90, finiteOr(y, 0)));
      lon[k] = Math.max(-180, Math.min(180, finiteOr(x, 0)));
      sinLat[k] = Math.sin(lat[k] * DEG);
      cosLat[k] = Math.cos(lat[k] * DEG);
      sinLon[k] = Math.sin(lon[k] * DEG);
      cosLon[k] = Math.cos(lon[k] * DEG);
    }
  }
  return {
    cell,
    canvas: createCanvas(columns, rows),
    pixels: new ImageData(columns, rows),
    lat,
    lon,
    sinLat,
    cosLat,
    sinLon,
    cosLon,
  };
}

function buildScene(width: number, land: GeoPermissibleObjects, sites: Site[], palette: Palette, hub: Point): Scene {
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

  const daylight = projectGrid(projection, width, height, MASK_CELL);
  daylight.pixels.data.fill(255);

  const points = sites.map((site): Point => projection([site.lon, site.lat]) ?? [0, 0]);

  return {
    width,
    height,
    dpr,
    projection,
    points,
    hub,
    lift: Math.ceil(Math.max(0, -hub[1])),
    flows: points.map((point) => flowTo(point, hub)),
    night: layer(palette.nightOcean, palette.nightGrid, palette.nightLand),
    day: layer(palette.dayOcean, palette.dayGrid, palette.dayLand),
    work: createCanvas(Math.round(width * dpr), Math.round(height * dpr)),
    base: createCanvas(Math.round(width * dpr), Math.round(height * dpr)),
    daylight,
    cover: projectGrid(projection, width, height, COVER_CELL),
  };
}

function paintBase(scene: Scene, palette: Palette, ms: number) {
  const { width, height, dpr, projection } = scene;
  const sun = subsolarPoint(ms);
  const sinDec = Math.sin(sun.lat * DEG);
  const cosDec = Math.cos(sun.lat * DEG);
  const sinSunLon = Math.sin(sun.lon * DEG);
  const cosSunLon = Math.cos(sun.lon * DEG);
  const { daylight } = scene;
  const data = daylight.pixels.data;
  for (let k = 0; k < daylight.sinLat.length; k++) {
    const sinAltitude =
      daylight.sinLat[k] * sinDec +
      daylight.cosLat[k] * cosDec * (daylight.cosLon[k] * cosSunLon + daylight.sinLon[k] * sinSunLon);
    data[k * 4 + 3] = 255 * smoothstep(DAYLIGHT_EDGES[0], DAYLIGHT_EDGES[1], sinAltitude);
  }
  daylight.canvas.getContext("2d")!.putImageData(daylight.pixels, 0, 0);

  const work = scene.work.getContext("2d")!;
  work.globalCompositeOperation = "copy";
  work.drawImage(scene.day, 0, 0);
  work.globalCompositeOperation = "destination-in";
  work.imageSmoothingQuality = "high";
  work.drawImage(
    daylight.canvas,
    0,
    0,
    daylight.canvas.width * daylight.cell * dpr,
    daylight.canvas.height * daylight.cell * dpr,
  );

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

function resolveColor(container: HTMLElement, color: string): string {
  const probe = document.createElement("span");
  probe.style.color = color;
  container.appendChild(probe);
  const resolved = getComputedStyle(probe).color;
  probe.remove();
  return resolved;
}

function coverageLayer(scene: Scene, coverage: Coverage, color: string): CoverageLayer {
  const swatch = createCanvas(1, 1).getContext("2d", { willReadFrequently: true })!;
  swatch.fillStyle = color;
  swatch.fillRect(0, 0, 1, 1);
  const { cover } = scene;
  const cells = cover.lat.length;
  const columns = cover.canvas.width;
  const [x0, y0] = coverage.origin ? scene.projection(coverage.origin) ?? scene.hub : scene.hub;
  const distances = new Float32Array(cells);
  let reach = 0;
  for (let k = 0; k < cells; k++) {
    const x = ((k % columns) + 0.5) * cover.cell;
    const y = (Math.floor(k / columns) + 0.5) * cover.cell;
    distances[k] = (Math.hypot(x - x0, y - y0) * 360) / scene.width;
    reach = Math.max(reach, distances[k]);
  }
  const { decMin, decMax } = coverage.footprint;
  const limits = followsSiderealTime(coverage.footprint)
    ? []
    : [decMin, decMax].filter((dec) => dec !== undefined).map(parallel);
  return {
    coverage,
    color,
    rgb: swatch.getImageData(0, 0, 1, 1).data,
    distances,
    reach: reach + 3 * RIM_WIDTH,
    values: new Float32Array(cells),
    edges: new Float32Array(cells),
    limits,
    computedAt: Number.NaN,
  };
}

function computeCoverage(scene: Scene, layer: CoverageLayer, ms: number) {
  const { values, edges } = layer;
  const { cover } = scene;
  const width = cover.canvas.width;
  const sidereal = siderealAngle(ms);
  for (let k = 0; k < values.length; k++) {
    values[k] = footprintCoverage(layer.coverage.footprint, cover.lon[k] + sidereal, cover.lat[k]);
  }
  for (let k = 0; k < values.length; k++) {
    const i = k % width;
    const value = values[k];
    let edge = 0;
    if (i > 0) edge = Math.max(edge, Math.abs(value - values[k - 1]));
    if (i < width - 1) edge = Math.max(edge, Math.abs(value - values[k + 1]));
    if (k >= width) edge = Math.max(edge, Math.abs(value - values[k - width]));
    if (k + width < values.length) edge = Math.max(edge, Math.abs(value - values[k + width]));
    edges[k] = edge;
  }
  layer.computedAt = ms;
}

export default function NightMap({ sites, timeRef, hubRef, coverage, onSelect }: {
  sites: Site[];
  timeRef: React.RefObject<number>;
  hubRef: React.RefObject<HTMLElement | null>;
  coverage: Coverage | null;
  onSelect: (id: string) => void;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const coverageRef = useRef<ActiveCoverage | null>(null);
  const [land, setLand] = useState<GeoPermissibleObjects | null>(null);

  useEffect(() => {
    const now = performance.now();
    if (coverage) coverageRef.current = { coverage, since: now };
    else if (coverageRef.current) coverageRef.current = { ...coverageRef.current, endedAt: now };
  }, [coverage]);

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
    let layer: CoverageLayer | null = null;
    let paintedAt = Number.NaN;
    let raf = 0;

    const rebuild = () => {
      const width = Math.floor(container.clientWidth);
      if (width < 50) return;
      palette = document.documentElement.classList.contains("dark") ? PALETTES.dark : PALETTES.light;
      colors = sites.map((site) => site.telescopes.map((telescope) => resolveColor(container, telescope.color)));
      const box = container.getBoundingClientRect();
      const logo = hubRef.current?.getBoundingClientRect();
      const hub: Point = logo
        ? [logo.left + logo.width / 2 - box.left, logo.top + logo.height / 2 - box.top]
        : [width / 2, 0];
      scene = buildScene(width, land, sites, palette, hub);
      layer = null;
      view.width = scene.base.width;
      view.height = scene.base.height + Math.round(scene.lift * scene.dpr);
      view.style.height = `${scene.height + scene.lift}px`;
      view.style.marginTop = `${-scene.lift}px`;
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

    function paintCoverage(scene: Scene, layer: CoverageLayer, active: ActiveCoverage, now: number) {
      const elapsed = reducedMotion ? Number.POSITIVE_INFINITY : now - active.since;
      const reveal = Math.min(1, elapsed / REVEAL_MS);
      const front = layer.reach * (1 - (1 - reveal) ** 3);
      const rim = COVER_RIM * (1 - reveal);
      const pingAge = elapsed - REVEAL_MS;
      const ping = Number.isFinite(pingAge) && pingAge > 0 ? ((pingAge / PING_MS) % 1) * layer.reach : 0;
      const pulse = ping > 0 ? COVER_PING * (1 - ping / layer.reach) : 0;
      const fade = active.endedAt === undefined ? 1 : Math.max(0, 1 - (now - active.endedAt) / FADE_MS);

      const [red, green, blue] = layer.rgb;
      const { cover } = scene;
      const data = cover.pixels.data;
      for (let k = 0; k < layer.values.length; k++) {
        const value = layer.values[k];
        const distance = layer.distances[k];
        const shown = smoothstep(front, front - 12, distance) * fade;
        let inside = shown * (COVER_FILL + COVER_EDGE * layer.edges[k]);
        if (rim > 0 && Math.abs(distance - front) < 3 * RIM_WIDTH) {
          inside += rim * fade * gaussian((distance - front) / RIM_WIDTH);
        }
        if (pulse > 0 && Math.abs(distance - ping) < 3 * PING_WIDTH) {
          inside += pulse * fade * gaussian((distance - ping) / PING_WIDTH);
        }
        inside *= value;
        const outside = (1 - value) * shown * COVER_DIM;
        const total = inside + outside;
        const mix = total > 0 ? inside / total : 0;
        data[k * 4] = COVER_SHADE[0] + (red - COVER_SHADE[0]) * mix;
        data[k * 4 + 1] = COVER_SHADE[1] + (green - COVER_SHADE[1]) * mix;
        data[k * 4 + 2] = COVER_SHADE[2] + (blue - COVER_SHADE[2]) * mix;
        data[k * 4 + 3] = 255 * Math.min(1, total);
      }
      cover.canvas.getContext("2d")!.putImageData(cover.pixels, 0, 0);

      const path = geoPath(scene.projection, ctx);
      ctx.save();
      ctx.beginPath();
      path(SPHERE);
      ctx.clip();
      ctx.imageSmoothingQuality = "high";
      ctx.drawImage(cover.canvas, 0, 0, cover.canvas.width * cover.cell, cover.canvas.height * cover.cell);
      ctx.restore();

      if (layer.limits.length) {
        ctx.globalAlpha = 0.9 * reveal * fade;
        ctx.strokeStyle = layer.color;
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 3]);
        ctx.beginPath();
        layer.limits.forEach((limit) => path(limit));
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.globalAlpha = 1;
      }
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
      const lift = Math.round(scene.lift * scene.dpr);
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.globalCompositeOperation = "copy";
      ctx.drawImage(scene.base, 0, lift);
      ctx.globalCompositeOperation = "source-over";
      ctx.setTransform(scene.dpr, 0, 0, scene.dpr, 0, lift);

      const active = coverageRef.current;
      if (active?.endedAt !== undefined && now - active.endedAt > FADE_MS) {
        coverageRef.current = null;
      } else if (active) {
        if (layer?.coverage !== active.coverage) {
          layer = coverageLayer(scene, active.coverage, resolveColor(container!, active.coverage.color));
        }
        const stale = !(Math.abs(ms - layer.computedAt) < REPAINT_SIM_MS);
        if (stale && (Number.isNaN(layer.computedAt) || followsSiderealTime(layer.coverage.footprint))) {
          computeCoverage(scene, layer, ms);
        }
        paintCoverage(scene, layer, active, now);
      }

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
  }, [land, sites, timeRef, hubRef]);

  return (
    <div ref={containerRef} className="relative flow-root">
      <canvas ref={canvasRef} className="pointer-events-none block w-full" style={{ aspectRatio: "1000 / 520" }} />
      {labels.map(({ site, left, top }) => (
        <div
          key={site.id}
          className="absolute mr-4 hidden -translate-y-1/2 sm:block"
          style={{ right: `${100 - left}%`, top: `${top}%` }}
        >
          <div className="bg-background/80 text-foreground rounded-md border px-1 py-1 text-[11px] leading-tight shadow-lg backdrop-blur-sm">
            <div className="px-1 font-medium">{site.name}</div>
            {site.telescopes.map((telescope) => {
              const selected = coverage?.id === telescope.id;
              return (
                <button
                  key={telescope.id}
                  type="button"
                  aria-pressed={selected}
                  onClick={() => onSelect(telescope.id)}
                  className="hover:bg-accent mt-0.5 flex w-full cursor-pointer items-center gap-1 rounded px-1 py-0.5 transition-colors"
                  style={selected ? { backgroundColor: `color-mix(in oklch, ${telescope.color} 22%, transparent)` } : undefined}
                >
                  <span className="size-1.5 rounded-full" style={{ backgroundColor: telescope.color }} />
                  {telescope.name}
                </button>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}
