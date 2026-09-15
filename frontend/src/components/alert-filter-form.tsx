import { type ReactNode, useState } from "react";
import { Info } from "lucide-react";
import { Select, SelectTrigger, SelectContent, SelectItem, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

// ─── Exported types and utilities used by Query ───────────────────────────────

export type TimeFormat = 'local' | 'utc' | 'jd' | 'mjd';
export type BoolFilter = 'any' | 'true' | 'false';

export type AlertFilterFormProps = {
  survey: 'ZTF' | 'LSST';
  timeFormat: TimeFormat;
  onTimeFormatChange: (f: TimeFormat) => void;
  startTime: string; setStartTime: (v: string) => void;
  endTime: string;   setEndTime: (v: string) => void;
  minMag: string;    setMinMag: (v: string) => void;
  maxMag: string;    setMaxMag: (v: string) => void;
  minDrb: string;    setMinDrb: (v: string) => void;
  maxDrb: string;    setMaxDrb: (v: string) => void;
  isRock: BoolFilter;           setIsRock: (v: BoolFilter) => void;
  isStar: BoolFilter;           setIsStar: (v: BoolFilter) => void;
  isNearBrightstar: BoolFilter; setIsNearBrightstar: (v: BoolFilter) => void;
  isStationary: BoolFilter;     setIsStationary: (v: BoolFilter) => void;
  isPositive: BoolFilter;       setIsPositive: (v: BoolFilter) => void;
};

export function toDatetimeLocalString(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function toDatetimeUTCString(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}T${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}`;
}

function jdToFormatString(jd: number, format: TimeFormat): string {
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

// ─── Shared primitives ────────────────────────────────────────────────────────

function TimeFormatSelect({ value, onChange }: { value: TimeFormat; onChange: (v: TimeFormat) => void }) {
  return (
    <Select value={value} onValueChange={v => onChange(v as TimeFormat)}>
      <SelectTrigger className="h-8 text-sm w-28"><SelectValue /></SelectTrigger>
      <SelectContent>
        <SelectItem value="local">Local</SelectItem>
        <SelectItem value="utc">UTC</SelectItem>
        <SelectItem value="jd">JD</SelectItem>
        <SelectItem value="mjd">MJD</SelectItem>
      </SelectContent>
    </Select>
  );
}

function TimeInput({ id, value, onChange, format, className }: {
  id: string; value: string; onChange: (v: string) => void; format: TimeFormat; className?: string;
}) {
  const base = { id, value, onChange: (e: React.ChangeEvent<HTMLInputElement>) => onChange(e.target.value) };
  if (format === 'local' || format === 'utc') return (
    <Input {...base} type="datetime-local" className={cn("pr-1.5", className)} />
  );
  return <Input {...base} type="number" step="any" placeholder={format === 'jd' ? '2459000.5' : '59000.0'} className={className} />;
}

// ─── Time presets ─────────────────────────────────────────────────────────────

const MAX_WINDOW_MS = 24 * 60 * 60 * 1000;

const TIME_PRESETS = [
  { label: '1h',  ms: 3_600_000 },
  { label: '6h',  ms: 6 * 3_600_000 },
  { label: '12h', ms: 12 * 3_600_000 },
  { label: '24h', ms: 24 * 3_600_000 },
] as const;

function getWindowMs(start: string, end: string, format: TimeFormat): number | null {
  const s = toJd(start, format);
  const e = toJd(end, format);
  if (s === undefined || e === undefined) return null;
  return (e - s) * 86_400_000;
}

function applyPreset(ms: number, format: TimeFormat): { start: string; end: string } {
  const now = new Date();
  const start = new Date(now.getTime() - ms);
  const nowJd = now.getTime() / 86400000 + 2440587.5;
  const startJd = start.getTime() / 86400000 + 2440587.5;
  return { start: jdToFormatString(startJd, format), end: jdToFormatString(nowJd, format) };
}

// ─── CycleTile ────────────────────────────────────────────────────────────────

function CycleTile({ label, value, onChange, tooltip }: {
  label: string; value: BoolFilter; onChange: (v: BoolFilter) => void; tooltip?: ReactNode;
}) {
  const cycle = () => onChange(value === 'any' ? 'true' : value === 'true' ? 'false' : 'any');

  const stateLabel = value === 'any' ? '○  Any' : value === 'true' ? '✓  Include' : '✗  Exclude';
  const stateColor = value === 'true' ? 'text-emerald-500' : value === 'false' ? 'text-rose-500' : 'text-muted-foreground/50';
  const borderBg   = value === 'true' ? 'border-emerald-500/60 bg-emerald-500/5'
                   : value === 'false' ? 'border-rose-500/60 bg-rose-500/5'
                   : 'border-border bg-muted/20 hover:bg-muted/40';

  return (
    <button type="button" onClick={cycle}
      className={`rounded-xl p-3 border-2 transition-all text-left w-full ${borderBg}`}
    >
      <div className={`flex items-center gap-1 text-sm font-medium mb-2 leading-tight ${value === 'false' ? 'opacity-40 line-through' : ''}`}>
        {label}
        {tooltip && (
          <Tooltip>
            <TooltipTrigger asChild>
              <span onClick={e => e.stopPropagation()} className="inline-flex ml-1 text-muted-foreground/70 hover:text-muted-foreground transition-colors cursor-default">
                <Info className="size-3.5" />
              </span>
            </TooltipTrigger>
            <TooltipContent side="top" className="max-w-sm leading-relaxed text-xs">
              {tooltip}
            </TooltipContent>
          </Tooltip>
        )}
      </div>
      <div className={`text-xs font-semibold tabular-nums ${stateColor}`}>
        {stateLabel}
      </div>
    </button>
  );
}

// ─── Property tooltips ────────────────────────────────────────────────────────

const T = { green: 'text-emerald-300', red: 'text-rose-300' };

function propertyTooltips(survey: 'ZTF' | 'LSST') {
  const stationary = (
    <>
      <span className={T.green}>True</span> if the temporal baseline (last &minus; first observation) &gt;&nbsp;0.01&nbsp;days (~14&nbsp;min).
      Combines <code>prv_candidates</code> and <code>fp_hists</code> (forced photometry, SNR&nbsp;&ge;&nbsp;3).
    </>
  );

  const positive = (
    <>
      <span className={T.green}>True</span> (<code>isdiffpos</code>) if the subtraction residual is positive — the source appears <em>brighter</em> than the reference image.
      <br /><span className={T.red}>False</span> for a negative residual (source fainter than reference).
      <br />Most real transients produce positive subtractions.
    </>
  );

  if (survey === 'ZTF') {
    return {
      rock: (
        <>
          <span className={T.green}>True</span> if a known Solar System object is within 12&nbsp;arcsec (<code>ssdistnr</code>) with a valid <code>ssmagnr</code>.
        </>
      ),
      star: (
        <>
          Stellar classification from PS1 star-galaxy scores (<code>sgscore1</code>, <code>distpsnr1</code>):
          <br />&#8226; <span className={T.green}>True</span> if <code>sgscore1</code> &gt; 0.76 and <code>distpsnr1</code> &le; 2.0&nbsp;arcsec.
          <br />&#8226; <span className={T.green}>True</span> if <code>sgscore1</code> &gt; 0.2, <code>distpsnr1</code> &le; 1.0&nbsp;arcsec and a strong red color (<code>srmag1&nbsp;&minus;&nbsp;szmag1</code> or <code>srmag1&nbsp;&minus;&nbsp;simag1</code> &gt; 3.0).
        </>
      ),
      near_brightstar: (
        <>
          Bright-star proximity combining Gaia and PS1:
          <br />&#8226; <span className={T.green}>True</span> if a Gaia bright star is within 20&nbsp;arcsec (<code>neargaiabright</code>) with <code>maggaiabright</code> &le; 12.0.
          <br />&#8226; <span className={T.green}>True</span> if any top-3 PS1 match has <code>sgscore</code> &gt; 0.49, distance &le; 20&nbsp;arcsec and <code>srmag</code> &le; 15.0.
          <br />&#8226; <span className={T.green}>True</span> if <code>sgscore1</code> = 0.5, <code>distpsnr1</code> &lt; 0.5&nbsp;arcsec and any of <code>sgmag1</code>/<code>srmag1</code>/<code>simag1</code> &lt; 17.0.
        </>
      ),
      stationary,
      positive,
    };
  }

  return {
    rock: (
      <>
        <span className={T.green}>True</span> if the alert has an <code>ss_object_id</code> (Solar System object identifier).
      </>
    ),
    star: (
      <>
        Cross-match with the LSPSC catalog:
        <br />&#8226; <span className={T.green}>True</span> if distance &le; 1.0&nbsp;arcsec and score &gt; 0.5.
        <br />&#8226; <span className={T.red}>False</span> if inside the footprint but no match.
        <br />&#8226; <span className={T.red}>None</span> if outside the LSST footprint.
      </>
    ),
    near_brightstar: (
      <>
        Same catalog (LSPSC) with wider criteria:
        <br />&#8226; <span className={T.green}>True</span> if distance &le; 20.0&nbsp;arcsec, score &gt; 0.5 and white magnitude &le; 15.0.
        <br />&#8226; <span className={T.red}>False</span> if inside the footprint but no bright star nearby.
        <br />&#8226; <span className={T.red}>None</span> if outside the LSST footprint.
      </>
    ),
    stationary,
    positive,
  };
}

// ─── Shared card sections ─────────────────────────────────────────────────────

type TimeCardProps = {
  timeFormat: TimeFormat; onTimeFormatChange: (f: TimeFormat) => void;
  startTime: string; setStartTime: (v: string) => void;
  endTime: string; setEndTime: (v: string) => void;
  activePreset: number | null; setActivePreset: (v: number | null) => void;
};

function TimeCard({ timeFormat, onTimeFormatChange, startTime, setStartTime, endTime, setEndTime, activePreset, setActivePreset }: TimeCardProps) {
  const handlePreset = (ms: number) => {
    const { start, end } = applyPreset(ms, timeFormat);
    setStartTime(start);
    setEndTime(end);
    setActivePreset(ms);
  };
  const onCustom = (setter: (v: string) => void) => (v: string) => { setter(v); setActivePreset(null); };

  const handleFormatChange = (newFmt: TimeFormat) => {
    if (activePreset !== null) {
      const { start, end } = applyPreset(activePreset, newFmt);
      setStartTime(start);
      setEndTime(end);
    } else {
      const startJd = toJd(startTime, timeFormat);
      const endJd = toJd(endTime, timeFormat);
      if (startJd !== undefined) setStartTime(jdToFormatString(startJd, newFmt));
      if (endJd !== undefined) setEndTime(jdToFormatString(endJd, newFmt));
    }
    onTimeFormatChange(newFmt);
  };

  const windowMs = activePreset === null ? getWindowMs(startTime, endTime, timeFormat) : null;
  const windowError =
    windowMs !== null && windowMs > MAX_WINDOW_MS ? 'Time range cannot exceed 24 hours' :
    windowMs !== null && windowMs <= 0 ? 'End time must be after start time' :
    null;

  return (
    <div className="rounded-lg border bg-muted/40 p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="text-base font-semibold">Time Range</span>
        {/* on small screens the select lives here in the header */}
        <div className="md:hidden">
          <TimeFormatSelect value={timeFormat} onChange={handleFormatChange} />
        </div>
      </div>
      <div className="flex gap-1.5 mb-3">
        {TIME_PRESETS.map(({ label, ms }) => (
          <button
            type="button" key={label} onClick={() => handlePreset(ms)}
            className={`flex-1 text-sm py-1.5 rounded-md border transition-colors ${activePreset === ms ? 'bg-primary text-primary-foreground border-primary' : 'border-border hover:bg-muted'}`}
          >
            {label}
          </button>
        ))}
        <button
          type="button" onClick={() => setActivePreset(null)}
          className={`flex-1 text-sm py-1.5 rounded-md border transition-colors ${activePreset === null ? 'bg-primary text-primary-foreground border-primary' : 'border-border hover:bg-muted'}`}
        >
          Custom
        </button>
      </div>
      <div className="flex flex-col sm:flex-row sm:items-end gap-3">
        <div className="flex-1">
          <Label className="text-sm text-muted-foreground mb-1 block">Start</Label>
          <TimeInput id="t-start" value={startTime} onChange={onCustom(setStartTime)} format={timeFormat}
            className={windowError ? 'border-destructive focus-visible:ring-destructive' : undefined} />
        </div>
        <div className="flex-1">
          <Label className="text-sm text-muted-foreground mb-1 block">End</Label>
          <TimeInput id="t-end" value={endTime} onChange={onCustom(setEndTime)} format={timeFormat}
            className={windowError ? 'border-destructive focus-visible:ring-destructive' : undefined} />
        </div>
        {/* on sm+ the select drops down here, inline with the inputs */}
        <div className="hidden md:block">
          <TimeFormatSelect value={timeFormat} onChange={handleFormatChange} />
        </div>
      </div>
      {windowError && <p className="text-xs text-destructive mt-2 text-center">{windowError}</p>}
    </div>
  );
}

type ProfileValues = { isRock: BoolFilter; isStar: BoolFilter; isNearBrightstar: BoolFilter; isStationary: BoolFilter; isPositive: BoolFilter };

const PROFILES: Array<{ label: string; values: ProfileValues }> = [
  { label: 'Transient', values: { isRock: 'false', isStar: 'false', isNearBrightstar: 'false', isStationary: 'true', isPositive: 'true' } },
  { label: 'Stellar',   values: { isRock: 'false', isStar: 'true',  isNearBrightstar: 'true',  isStationary: 'true', isPositive: 'any'  } },
  { label: 'SSO',       values: { isRock: 'true',  isStar: 'false', isNearBrightstar: 'false', isStationary: 'any',  isPositive: 'any'  } },
];

type PropertiesCardProps = {
  survey: 'ZTF' | 'LSST';
  isRock: BoolFilter; setIsRock: (v: BoolFilter) => void;
  isStar: BoolFilter; setIsStar: (v: BoolFilter) => void;
  isNearBrightstar: BoolFilter; setIsNearBrightstar: (v: BoolFilter) => void;
  isStationary: BoolFilter; setIsStationary: (v: BoolFilter) => void;
  isPositive: BoolFilter; setIsPositive: (v: BoolFilter) => void;
};

function PropertiesCard({ survey, isRock, setIsRock, isStar, setIsStar, isNearBrightstar, setIsNearBrightstar, isStationary, setIsStationary, isPositive, setIsPositive }: PropertiesCardProps) {
  const tips = propertyTooltips(survey);
  const fields: Array<{ label: string; value: BoolFilter; set: (v: BoolFilter) => void; tooltip: ReactNode }> = [
    { label: 'Rock',             value: isRock,           set: setIsRock,           tooltip: tips.rock },
    { label: 'Star',             value: isStar,           set: setIsStar,           tooltip: tips.star },
    { label: 'Near bright star', value: isNearBrightstar, set: setIsNearBrightstar, tooltip: tips.near_brightstar },
    { label: 'Stationary',       value: isStationary,     set: setIsStationary,     tooltip: tips.stationary },
    { label: 'Positive sub.',    value: isPositive,       set: setIsPositive,       tooltip: tips.positive },
  ];

  const current: ProfileValues = { isRock, isStar, isNearBrightstar, isStationary, isPositive };
  const resetAll = () => { setIsRock('any'); setIsStar('any'); setIsNearBrightstar('any'); setIsStationary('any'); setIsPositive('any'); };
  const isActive = (v: ProfileValues) => (Object.keys(v) as (keyof ProfileValues)[]).every(k => v[k] === current[k]);
  const toggleProfile = (v: ProfileValues) => isActive(v) ? resetAll() : (setIsRock(v.isRock), setIsStar(v.isStar), setIsNearBrightstar(v.isNearBrightstar), setIsStationary(v.isStationary), setIsPositive(v.isPositive));

  return (
    <div className="rounded-lg border bg-muted/40 p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="text-base font-semibold">Properties</span>
        <div className="flex items-center gap-1.5">
          <span className="text-xs text-muted-foreground/60 mr-1">Click to cycle</span>
          {PROFILES.map(({ label, values }) => (
            <button
              key={label}
              type="button"
              onClick={() => toggleProfile(values)}
              className={cn(
                "text-xs px-2 py-0.5 rounded-full border transition-colors",
                isActive(values)
                  ? "bg-primary text-primary-foreground border-primary"
                  : "border-border text-muted-foreground hover:bg-muted hover:text-foreground"
              )}
            >{label}</button>
          ))}
        </div>
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-2.5">
        {fields.map(({ label, value, set, tooltip }) => (
          <CycleTile key={label} label={label} value={value} onChange={set} tooltip={tooltip} />
        ))}
      </div>
    </div>
  );
}

export function AlertFilterForm(props: AlertFilterFormProps) {
  return <FiltersVisual {...props} />;
}

// ─── Filters ──────────────────────────────────────────────────────────────────
// Time presets and property tiles. Magnitude and DRB Score use an inline
// expression layout:  [ min ] ≤  label  ≤ [ max ]

function RangeRow({ label, minValue, onMinChange, maxValue, onMaxChange, minPlaceholder = "no min", maxPlaceholder = "no max", hardMin, hardMax }: {
  label: string;
  minValue: string; onMinChange: (v: string) => void;
  maxValue: string; onMaxChange: (v: string) => void;
  minPlaceholder?: string; maxPlaceholder?: string;
  hardMin?: number; hardMax?: number;
}) {
  const minNum = minValue !== '' ? parseFloat(minValue) : null;
  const maxNum = maxValue !== '' ? parseFloat(maxValue) : null;

  const minOutOfRange = minNum !== null && ((hardMin !== undefined && minNum < hardMin) || (hardMax !== undefined && minNum > hardMax));
  const maxOutOfRange = maxNum !== null && ((hardMin !== undefined && maxNum < hardMin) || (hardMax !== undefined && maxNum > hardMax));
  const crossInvalid = minNum !== null && maxNum !== null && !minOutOfRange && !maxOutOfRange && minNum > maxNum;

  const errors: string[] = [];
  if (minOutOfRange || maxOutOfRange) {
    const lo = hardMin ?? '–'; const hi = hardMax ?? '–';
    if (minOutOfRange) errors.push(`Min must be between ${lo} and ${hi}`);
    if (maxOutOfRange) errors.push(`Max must be between ${lo} and ${hi}`);
  }
  if (crossInvalid) errors.push('Min must not exceed max');

  const leq = <span className="text-base text-muted-foreground shrink-0 select-none">≤</span>;

  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-2">
        <Input type="number" step="any" min={hardMin} max={hardMax} value={minValue} onChange={e => onMinChange(e.target.value)} placeholder={minPlaceholder}
          className={cn("w-0 flex-1 text-center font-mono text-sm", (minOutOfRange || crossInvalid) && "border-destructive focus-visible:ring-destructive")} />
        {leq}
        <span className="text-sm font-semibold px-3 py-1.5 rounded-md bg-secondary shrink-0 whitespace-nowrap">{label}</span>
        {leq}
        <Input type="number" step="any" min={hardMin} max={hardMax} value={maxValue} onChange={e => onMaxChange(e.target.value)} placeholder={maxPlaceholder}
          className={cn("w-0 flex-1 text-center font-mono text-sm", (maxOutOfRange || crossInvalid) && "border-destructive focus-visible:ring-destructive")} />
      </div>
      {errors.map(e => <p key={e} className="text-xs text-destructive text-center">{e}</p>)}
    </div>
  );
}

function FiltersVisual({
  survey,
  timeFormat, onTimeFormatChange, startTime, setStartTime, endTime, setEndTime,
  minMag, setMinMag, maxMag, setMaxMag,
  minDrb, setMinDrb, maxDrb, setMaxDrb,
  isRock, setIsRock, isStar, setIsStar, isNearBrightstar, setIsNearBrightstar, isStationary, setIsStationary,
  isPositive, setIsPositive,
}: AlertFilterFormProps) {
  const [activePreset, setActivePreset] = useState<number | null>(24 * 3_600_000);

  return (
    <div className="space-y-3">
      <TimeCard
        timeFormat={timeFormat} onTimeFormatChange={onTimeFormatChange}
        startTime={startTime} setStartTime={setStartTime}
        endTime={endTime} setEndTime={setEndTime}
        activePreset={activePreset} setActivePreset={setActivePreset}
      />
      <PropertiesCard
        survey={survey}
        isRock={isRock} setIsRock={setIsRock}
        isStar={isStar} setIsStar={setIsStar}
        isNearBrightstar={isNearBrightstar} setIsNearBrightstar={setIsNearBrightstar}
        isStationary={isStationary} setIsStationary={setIsStationary}
        isPositive={isPositive} setIsPositive={setIsPositive}
      />
      <div className="rounded-lg border bg-muted/40 p-4 space-y-4">
        <span className="text-base font-semibold block">Cuts</span>
        <RangeRow
          label="Magnitude (AB)"
          minValue={minMag} onMinChange={setMinMag}
          maxValue={maxMag} onMaxChange={setMaxMag}
        />
        <RangeRow
          label="DRB / Reliability"
          minValue={minDrb} onMinChange={setMinDrb}
          maxValue={maxDrb} onMaxChange={setMaxDrb}
          hardMin={0} hardMax={1}
        />
      </div>
    </div>
  );
}
