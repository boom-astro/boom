const DEG = Math.PI / 180;
const DAY_MS = 86_400_000;
const J2000_MS = Date.UTC(2000, 0, 1, 12);

export const NIGHT_SUN_ALTITUDE = -12;

export type SkyState = "day" | "twilight" | "night";

export type SunPosition = { lat: number; lon: number };

export function wrapLongitude(lon: number): number {
  return ((((lon + 180) % 360) + 360) % 360) - 180;
}

export function subsolarPoint(ms: number): SunPosition {
  const d = (ms - J2000_MS) / DAY_MS;
  const meanAnomaly = (357.529 + 0.98560028 * d) * DEG;
  const meanLongitude = 280.459 + 0.98564736 * d;
  const eclipticLongitude =
    (meanLongitude + 1.915 * Math.sin(meanAnomaly) + 0.02 * Math.sin(2 * meanAnomaly)) * DEG;
  const obliquity = (23.439 - 0.00000036 * d) * DEG;
  const rightAscension = Math.atan2(
    Math.cos(obliquity) * Math.sin(eclipticLongitude),
    Math.cos(eclipticLongitude),
  ) / DEG;
  const declination = Math.asin(Math.sin(obliquity) * Math.sin(eclipticLongitude)) / DEG;
  const siderealTime = 280.46061837 + 360.98564736629 * d;
  return { lat: declination, lon: wrapLongitude(rightAscension - siderealTime) };
}

export function sunAltitude(sun: SunPosition, lat: number, lon: number): number {
  const sinAltitude =
    Math.sin(lat * DEG) * Math.sin(sun.lat * DEG) +
    Math.cos(lat * DEG) * Math.cos(sun.lat * DEG) * Math.cos((lon - sun.lon) * DEG);
  return Math.asin(sinAltitude) / DEG;
}

export function sunAltitudeAt(ms: number, lat: number, lon: number): number {
  return sunAltitude(subsolarPoint(ms), lat, lon);
}

export function skyState(altitude: number): SkyState {
  if (altitude >= 0) return "day";
  return altitude < NIGHT_SUN_ALTITUDE ? "night" : "twilight";
}

export function nextCrossing(
  ms: number,
  lat: number,
  lon: number,
  altitude = NIGHT_SUN_ALTITUDE,
  horizonMs = 2 * DAY_MS,
): number | null {
  const above = (t: number) => sunAltitudeAt(t, lat, lon) > altitude;
  const startsAbove = above(ms);
  const step = 10 * 60_000;
  for (let t = ms + step; t <= ms + horizonMs; t += step) {
    if (above(t) === startsAbove) continue;
    let lo = t - step;
    let hi = t;
    while (hi - lo > 1000) {
      const mid = (lo + hi) / 2;
      if (above(mid) === startsAbove) lo = mid;
      else hi = mid;
    }
    return hi;
  }
  return null;
}

export function darkIntervals(
  from: number,
  to: number,
  lat: number,
  lon: number,
): { start: number; end: number }[] {
  const intervals: { start: number; end: number }[] = [];
  let t = from;
  let dark = sunAltitudeAt(t, lat, lon) < NIGHT_SUN_ALTITUDE;
  while (t < to) {
    const next = nextCrossing(t, lat, lon, NIGHT_SUN_ALTITUDE, to - t) ?? to;
    if (dark) intervals.push({ start: t, end: Math.min(next, to) });
    dark = !dark;
    t = next;
  }
  return intervals;
}
