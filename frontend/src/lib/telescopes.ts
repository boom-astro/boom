import { type NightlyStat } from "@/lib/api";

export const NIGHT_COLOR = "var(--chart-1)";

export type Telescope = {
  id: Exclude<keyof NightlyStat, "date" | "windows">;
  name: string;
  survey: string;
  instrument: string;
  color: string;
  utcOffset: number;
  acl?: string;
};

export type Site = {
  id: string;
  name: string;
  place: string;
  lat: number;
  lon: number;
  timeZone: string;
  telescopes: Telescope[];
};

export const SITES: Site[] = [
  {
    id: "palomar",
    name: "Palomar Observatory",
    place: "California, USA",
    lat: 33.3563,
    lon: -116.865,
    timeZone: "America/Los_Angeles",
    telescopes: [
      {
        id: "ztf",
        name: "ZTF",
        survey: "Zwicky Transient Facility",
        instrument: "48-inch Samuel Oschin Schmidt telescope",
        color: "var(--ztf)",
        utcOffset: -7,
      },
      {
        id: "winter",
        name: "WINTER",
        survey: "Wide-field Infrared Transient Explorer",
        instrument: "1-m robotic infrared telescope",
        color: "var(--winter)",
        utcOffset: -7,
        acl: "winter",
      },
    ],
  },
  {
    id: "chile",
    name: "Cerro Pachón & Cerro Tololo",
    place: "Coquimbo, Chile",
    lat: -30.2069,
    lon: -70.7779,
    timeZone: "America/Santiago",
    telescopes: [
      {
        id: "lsst",
        name: "LSST",
        survey: "Legacy Survey of Space and Time",
        instrument: "8.4-m Simonyi Survey Telescope, Vera C. Rubin Observatory",
        color: "var(--lsst)",
        utcOffset: -3,
      },
      {
        id: "decam",
        name: "DECam",
        survey: "Dark Energy Camera",
        instrument: "4-m Víctor M. Blanco Telescope, CTIO",
        color: "var(--decam)",
        utcOffset: -4,
      },
    ],
  },
];

const clockFormats = new Map<string, Intl.DateTimeFormat>();

function clockFormat(timeZone: string, offset: boolean): Intl.DateTimeFormat {
  const key = `${timeZone}|${offset}`;
  let format = clockFormats.get(key);
  if (!format) {
    format = new Intl.DateTimeFormat("en-US", {
      timeZone,
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
      ...(offset ? { timeZoneName: "shortOffset" } : {}),
    });
    clockFormats.set(key, format);
  }
  return format;
}

export function formatUtcOffset(ms: number, timeZone: string): string {
  const zone = clockFormat(timeZone, true).formatToParts(ms).find((part) => part.type === "timeZoneName");
  return zone?.value.replace("GMT", "UTC") ?? "";
}

export function formatClock(ms: number, timeZone: string, withOffset = false): string {
  const time = clockFormat(timeZone, false).format(ms);
  return withOffset ? `${time} ${formatUtcOffset(ms, timeZone)}` : time;
}
