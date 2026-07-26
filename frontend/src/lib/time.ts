export type TimeFormat = 'local' | 'utc' | 'jd' | 'mjd';

export function toDatetimeLocalString(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function toDatetimeUTCString(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}T${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}`;
}

export function jdToFormatString(jd: number, format: TimeFormat): string {
  if (format === 'jd') return jd.toFixed(5);
  if (format === 'mjd') return (jd - 2400000.5).toFixed(5);
  const date = new Date((jd - 2440587.5) * 86400000);
  return format === 'utc' ? toDatetimeUTCString(date) : toDatetimeLocalString(date);
}

export function datetimeLocalDefaults() {
  const now = new Date();
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  return { start: toDatetimeLocalString(yesterday), end: toDatetimeLocalString(now) };
}

export function timeFormatDefaults(fmt: TimeFormat): { start: string; end: string } {
  return applyPreset(24 * 3_600_000, fmt);
}

export function toJd(value: string, format: TimeFormat): number | undefined {
  if (!value) return undefined;
  if (format === 'jd') { const n = parseFloat(value); return isNaN(n) ? undefined : n; }
  if (format === 'mjd') { const n = parseFloat(value); return isNaN(n) ? undefined : n + 2400000.5; }
  const date = format === 'utc' ? new Date(value + 'Z') : new Date(value);
  if (isNaN(date.getTime())) return undefined;
  return date.getTime() / 86400000 + 2440587.5;
}

export function getWindowMs(start: string, end: string, format: TimeFormat): number | null {
  const s = toJd(start, format);
  const e = toJd(end, format);
  if (s === undefined || e === undefined) return null;
  return (e - s) * 86_400_000;
}

export function applyPreset(ms: number, format: TimeFormat): { start: string; end: string } {
  const now = new Date();
  const start = new Date(now.getTime() - ms);
  const nowJd = now.getTime() / 86400000 + 2440587.5;
  const startJd = start.getTime() / 86400000 + 2440587.5;
  return { start: jdToFormatString(startJd, format), end: jdToFormatString(nowJd, format) };
}