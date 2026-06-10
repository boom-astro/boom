import { useState } from "react";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

export type ParsedPosition = { ra: number; dec: number };

type FormatDef = {
  name: string;
  example: string;
  regex: RegExp;
  parse: (m: RegExpMatchArray) => ParsedPosition;
};

function hmsToRaDeg(h: number, m: number, s: number): number {
  return (h + m / 60 + s / 3600) * 15;
}

function dmsToDecDeg(sign: number, d: number, m: number, s: number): number {
  return sign * (d + m / 60 + s / 3600);
}

function isValid({ ra, dec }: ParsedPosition): boolean {
  return Number.isFinite(ra) && Number.isFinite(dec)
    && ra >= 0 && ra < 360
    && dec >= -90 && dec <= 90;
}

// Each entry: regex capturing groups, a human name, an example,
// and a pure parse function (called only when the regex matched).
export const POSITION_FORMATS: FormatDef[] = [

  // ── 1. Decimal degrees ─────────────────────────────────────────────────────
  // "150.123 -2.456"  /  "150.123, -2.456"  /  "150.123,-2.456"
  {
    name: "Decimal degrees",
    example: "150.123 -2.456",
    regex: /^\s*([+-]?\d+(?:\.\d+)?)\s*[,\s]\s*([+-]?\d+(?:\.\d+)?)\s*$/,
    parse([, r, d]) {
      return { ra: parseFloat(r), dec: parseFloat(d) };
    },
  },

  // ── 2. Decimal degrees, European style (comma as decimal separator) ────────
  // "105,5 32,5"  /  "105,5 -32,5"  /  "105,5; -32,5"
  {
    name: "Decimal degrees (European)",
    example: "105,5 32,5",
    regex: /^\s*([+-]?\d+,\d+)[\s;]+([+-]?\d+,\d+)\s*$/,
    parse([, r, d]) {
      return { ra: parseFloat(r.replace(',', '.')), dec: parseFloat(d.replace(',', '.')) };
    },
  },

  // ── 3. Decimal with unit suffix ────────────────────────────────────────────
  // "12.34567h-17.87654d"  — RA in decimal hours (×15 → deg)
  // "350.123456d-17.33333d" — RA in decimal degrees
  // The captured suffix character (h or d) determines the RA conversion.
  {
    name: "Decimal with unit suffix",
    example: "12.34567h-17.87654d",
    regex: /^\s*(\d+(?:\.\d+)?)([hd])[\s,]*([+-]?\d+(?:\.\d+)?)d\s*$/i,
    parse([, raVal, raSuffix, decVal]) {
      return {
        ra: raSuffix.toLowerCase() === 'h' ? parseFloat(raVal) * 15 : parseFloat(raVal),
        dec: parseFloat(decVal),
      };
    },
  },

  // ── 4. HH:MM:SS ±DD:MM:SS (colons, space or sign as separator) ────────────
  // "10:00:00.00 +02:30:00.0"  /  "10:12:45.3-45:17:50"
  // [\s,]* (not +) lets the sign act as the boundary when there is no space.
  // (?=[^\d]) after RA seconds blocks regex backtracking from "donating" decimal
  // digits to the Dec group (e.g. "10:12:45.345:17:50" must not match).
  {
    name: "HH:MM:SS ±DD:MM:SS",
    example: "10:00:00.00 +02:30:00.0",
    regex: /^\s*(\d{1,2}):(\d{2}):(\d{2}(?:\.\d+)?)(?=[^\d])[\s,]*([+-]?)(\d{1,2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*$/,
    parse([, rh, rm, rs, dsign, dd, dm, ds]) {
      return {
        ra: hmsToRaDeg(+rh, +rm, +rs),
        dec: dmsToDecDeg(dsign === '-' ? -1 : 1, +dd, +dm, +ds),
      };
    },
  },

  // ── 5. HHh MMm SSs ±DDd MMm SSs (letter separators, minutes & seconds optional)
  // "10h00m00.0s +02d30m00.0s"  — full
  // "15h17m-11d10m"              — no seconds
  // "15h17+89d15"                — no m suffix, no seconds
  // m after minutes is optional (m?); seconds group (?:NNs)? is optional entirely.
  {
    name: "HHhMMmSSs ±DDdMMmSSs",
    example: "10h00m00.0s +02d30m00.0s",
    regex: /^\s*(\d{1,2})h\s*(\d{1,2})m?\s*(?:(\d{1,2}(?:\.\d+)?)s)?\s*([+-]?)(\d{1,2})d\s*(\d{1,2})m?\s*(?:(\d{1,2}(?:\.\d+)?)s)?\s*$/i,
    parse([, rh, rm, rs, dsign, dd, dm, ds]) {
      return {
        ra: hmsToRaDeg(+rh, +rm, +(rs ?? 0)),
        dec: dmsToDecDeg(dsign === '-' ? -1 : 1, +dd, +dm, +(ds ?? 0)),
      };
    },
  },

  // ── 6. DDDdMMmSSs ±DDdMMmSSs (RA in angular degrees, not hours) ────────────
  // "275d11m15.6954s+17d59m59.876s"
  // Distinct from format 5: RA uses d (degrees), not h (hours), and can be 3 digits.
  {
    name: "DDDdMMmSSs ±DDdMMmSSs",
    example: "275d11m15.6954s+17d59m59.876s",
    regex: /^\s*(\d{1,3})d\s*(\d{1,2})m\s*(\d{1,2}(?:\.\d+)?)s[\s,]*([+-]?)(\d{1,2})d\s*(\d{1,2})m\s*(\d{1,2}(?:\.\d+)?)s\s*$/i,
    parse([, rd, rm, rs, dsign, dd, dm, ds]) {
      return {
        ra: +rd + +rm / 60 + +rs / 3600,
        dec: dmsToDecDeg(dsign === '-' ? -1 : 1, +dd, +dm, +ds),
      };
    },
  },

  // ── 7. HH MM SS.s ±DD MM SS.s (spaces only, Simbad-style) ────────────────
  // "20 54 05.689 +37 01 17.38"  /  "10 00 00 02 30 00"
  {
    name: "HH MM SS ±DD MM SS",
    example: "20 54 05.689 +37 01 17.38",
    regex: /^\s*(\d{1,2})\s+(\d{1,2})\s+(\d{1,2}(?:\.\d+)?)\s+([+-]?)(\d{1,2})\s+(\d{1,2})\s+(\d{1,2}(?:\.\d+)?)\s*$/,
    parse([, rh, rm, rs, dsign, dd, dm, ds]) {
      return {
        ra: hmsToRaDeg(+rh, +rm, +rs),
        dec: dmsToDecDeg(dsign === '-' ? -1 : 1, +dd, +dm, +ds),
      };
    },
  },

  // ── 8. DDD°MM′SS.s″ ±DD°MM′SS.s″ (degree symbol, RA in angular degrees) ──
  // "150°07'23.4\" +02°30'00.0\""
  {
    name: "DDD°MM′SS″ ±DD°MM′SS″",
    example: "150°07'23.4\" +02°30'00.0\"",
    regex: /^\s*([+-]?\d{1,3})°\s*(\d{1,2})[′']\s*(\d{1,2}(?:\.\d+)?)[″"]?\s+([+-]?\d{1,2})°\s*(\d{1,2})[′']\s*(\d{1,2}(?:\.\d+)?)[″"]?\s*$/,
    parse([, rd, rm, rs, dd, dm, ds]) {
      const rasign = rd.startsWith('-') ? -1 : 1;
      const decsign = dd.startsWith('-') ? -1 : 1;
      return {
        ra: rasign * (Math.abs(+rd) + +rm / 60 + +rs / 3600),
        dec: decsign * (Math.abs(+dd) + +dm / 60 + +ds / 3600),
      };
    },
  },

  // ── 9. IAU/SDSS compact: J190132.9+145808.7 ───────────────────────────────
  // Optional J/B prefix, RA as HHMMSS.s, Dec sign then DDMMSS.s — no separators
  {
    name: "IAU compact (JHHMMSS±DDMMSS)",
    example: "J190132.9+145808.7",
    regex: /^\s*[JjBb]?(\d{2})(\d{2})(\d{2}(?:\.\d+)?)([+-])(\d{2})(\d{2})(\d{2}(?:\.\d+)?)\s*$/,
    parse([, rh, rm, rs, dsign, dd, dm, ds]) {
      return {
        ra: hmsToRaDeg(+rh, +rm, +rs),
        dec: dmsToDecDeg(dsign === '-' ? -1 : 1, +dd, +dm, +ds),
      };
    },
  },
];

export function tryParsePosition(input: string): { format: string; position: ParsedPosition } | null {
  for (const fmt of POSITION_FORMATS) {
    const m = input.match(fmt.regex);
    if (!m) continue;
    const position = fmt.parse(m);
    if (isValid(position)) return { format: fmt.name, position };
  }
  return null;
}

type PositionInputProps = {
  onChange: (pos: ParsedPosition | null) => void;
  className?: string;
};

export function PositionInput({ onChange, className }: PositionInputProps) {
  const [value, setValue] = useState('');
  const [parsed, setParsed] = useState<{ format: string; position: ParsedPosition } | null>(null);

  function handleChange(raw: string) {
    setValue(raw);
    const result = raw.trim() ? tryParsePosition(raw) : null;
    setParsed(result);
    onChange(result?.position ?? null);
  }

  const isEmpty = !value.trim();
  const isError = !isEmpty && !parsed;

  return (
    <div className={cn("space-y-1.5", className)}>
      <Input
        value={value}
        onChange={e => handleChange(e.target.value)}
        placeholder='e.g. "150.5 2.3"  ·  "10:00:00 +02:30:00"  ·  "10h00m00s +02d30m00s"  ·  "J190132+145808"'
        className={cn(isError && "border-destructive focus-visible:ring-destructive")}
        spellCheck={false}
        autoComplete="off"
        autoCorrect="off"
        autoCapitalize="off"
      />
      {parsed && (
        <p className="text-xs text-emerald-500">
          <span className="font-medium">{parsed.format}</span>
          {' → '}RA {parsed.position.ra.toFixed(6)}°&ensp;Dec {parsed.position.dec.toFixed(6)}°
        </p>
      )}
      {isError && (
        <p className="text-xs text-destructive leading-relaxed">
          Unrecognized format. Supported:{' '}
          <span className="font-mono">150.5 -2.3</span>
          {' · '}<span className="font-mono">105,5 32,5</span>
          {' · '}<span className="font-mono">10:00:00 +02:30:00</span>
          {' · '}<span className="font-mono">10h00m00s +02d30m00s</span>
          {' · '}<span className="font-mono">15h17m-11d10m</span>
          {' · '}<span className="font-mono">275d11m15s+17d59m59s</span>
          {' · '}<span className="font-mono">12.345h-17.876d</span>
          {' · '}<span className="font-mono">350.12d-17.33d</span>
          {' · '}<span className="font-mono">J190132+145808</span>
        </p>
      )}
    </div>
  );
}
