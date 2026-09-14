/**
 * An observing night is labeled by the date it starts: the API counts alerts
 * from local noon of that date to local noon of the next one, so the night
 * `2025-08-16` runs from the evening of Aug 16 to the morning of Aug 17 at the
 * observatory.
 */

export const NIGHT_CONVENTION =
  "Alerts are grouped by observing night, local noon to local noon at the " +
  "observatory (Palomar, UTC−7, for ZTF; Cerro Pachón, UTC−3, for LSST). " +
  "A night is labeled by its evening date.";

const parseNight = (date: string) => new Date(`${date}T00:00:00`);

const morningAfter = (evening: Date) =>
  new Date(evening.getFullYear(), evening.getMonth(), evening.getDate() + 1);

export function formatNightRange(date: string): string {
  const evening = parseNight(date);
  const morning = morningAfter(evening);
  const from = evening.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  const to =
    morning.getMonth() === evening.getMonth()
      ? String(morning.getDate())
      : morning.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  return `${from} → ${to}`;
}

export function formatNightRangeLong(date: string): string {
  return `${formatNightRange(date)}, ${parseNight(date).getFullYear()}`;
}

export function describeNight(date: string): string {
  const evening = parseNight(date);
  const morning = morningAfter(evening);
  const weekday = (d: Date) => d.toLocaleDateString("en-US", { weekday: "short" });
  return `${weekday(evening)} evening → ${weekday(morning)} morning`;
}
