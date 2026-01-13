import React, { useMemo, useRef, useState, useEffect } from 'react';
import { Card, CardContent } from './ui/card';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Maximize2 } from 'lucide-react';

// Band colors matching other plots
const BAND_COLORS: Record<string, string> = {
    g: '#38b000',
    r: '#ef233c',
    i: '#fcbf49',
    z: '#f59e0b',
    default: '#6b7280',
};
function toColor(band?: string) {
    if (!band) return BAND_COLORS.default;
    const k = String(band).toLowerCase();
    return BAND_COLORS[k] ?? BAND_COLORS.default;
}

type Detection = { jd?: number; magpsf?: number; sigmapsf?: number; diffmaglim?: number; band?: string };

type LightcurveData = {
    prv_candidates?: Detection[];
    prv_nondetections?: Detection[];
};

function jd2mjd(jd: number) {
    return jd - 2400000.5;
}

export default function Lightcurve({ data }: { data: LightcurveData }) {
    const candidates: Detection[] = data?.prv_candidates ?? [];
    const nondets: Detection[] = data?.prv_nondetections ?? [];

    // merge detections and non-detections into series grouped by band
    const detections = useMemo(() => {
        return candidates
            .map(d => ({
                t: d.jd !== undefined ? jd2mjd(Number(d.jd)) : NaN,
                mag: d.magpsf !== undefined ? Number(d.magpsf) : NaN,
                band: d.band ?? 'unknown',
                sigma: d.sigmapsf !== undefined ? Number(d.sigmapsf) : NaN,
            }))
            .filter(d => Number.isFinite(d.t) && Number.isFinite(d.mag));
    }, [candidates]);

    const nondetectionsSeries = useMemo(() => {
        return nondets
            .map(d => ({
                t: d.jd !== undefined ? jd2mjd(Number(d.jd)) : NaN,
                mag: d.diffmaglim !== undefined ? Number(d.diffmaglim) : NaN,
                band: d.band ?? 'unknown',
            }))
            .filter(d => Number.isFinite(d.t) && Number.isFinite(d.mag));
    }, [nondets]);

    const bands = useMemo(() => {
        const set = new Set<string>();
        detections.forEach(d => set.add(String(d.band).toLowerCase()));
        nondetectionsSeries.forEach(d => set.add(String(d.band).toLowerCase()));
        return Array.from(set).filter(Boolean);
    }, [detections, nondetectionsSeries]);

    // Domains
    const allTimes = [...detections.map(d => d.t), ...nondetectionsSeries.map(d => d.t)];
    const allMags = [...detections.map(d => d.mag), ...nondetectionsSeries.map(d => d.mag)];
    const tMin = Math.min(...(allTimes.length ? allTimes : [0]));
    const tMax = Math.max(...(allTimes.length ? allTimes : [1]));
    const magMin = Math.min(...(allMags.length ? allMags : [0]));
    const magMax = Math.max(...(allMags.length ? allMags : [1]));

    const padT = Math.max(1, (tMax - tMin) * 0.02);
    const padMag = Math.max(0.5, (magMax - magMin) * 0.05);

    const initialDomain = useMemo(() => ({
        x0: tMin - padT,
        x1: tMax + padT,
        y0: magMin - padMag,
        y1: magMax + padMag,
    }), [tMin, tMax, magMin, magMax, padT, padMag]);

    const [domain, setDomain] = useState(initialDomain);
    useEffect(() => setDomain(initialDomain), [initialDomain.x0, initialDomain.x1, initialDomain.y0, initialDomain.y1]);

    const [hiddenBands, setHiddenBands] = useState<Set<string>>(new Set());
    const [dialogOpen, setDialogOpen] = useState(false);

    const handleLegendClick = (band: string) => {
        setHiddenBands(prev => {
            const next = new Set(prev);
            if (next.has(band)) {
                next.delete(band);
            } else {
                next.add(band);
            }
            return next;
        });
    };

    const handleLegendDoubleClick = (band: string) => {
        const visibleBands = bands.filter(b => !hiddenBands.has(b));
        if (visibleBands.length === 1 && visibleBands[0] === band) {
            // reset to show all bands
            setHiddenBands(new Set());
        } else {
            // Hide all bands except this one
            setHiddenBands(new Set(bands.filter(b => b !== band)));
        }
    };

    // helper to get band state
    const getBandState = (band: string | undefined) => {
        const bandKey = String(band ?? 'default').toLowerCase();
        const isHidden = hiddenBands.has(bandKey);
        const color = toColor(band);
        return { bandKey, isHidden, color };
    };

    // sizing
    const containerRef = useRef<HTMLDivElement | null>(null);
    const [size, setSize] = useState({ width: 800, height: 360 });
    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const ro = new ResizeObserver(() => {
            const rect = el.getBoundingClientRect();
            setSize({ width: Math.max(320, Math.floor(rect.width)), height: Math.max(240, Math.floor(rect.height || 360)) });
        });
        ro.observe(el);
        const rect = el.getBoundingClientRect();
        setSize({ width: Math.max(320, Math.floor(rect.width)), height: Math.max(240, Math.floor(rect.height || 360)) });
        return () => ro.disconnect();
    }, []);

    // plotting geometry
    const pad = { left: 64, right: 20, top: 20, bottom: 48 };
    const plotW = Math.max(100, size.width - pad.left - pad.right);
    const plotH = Math.max(80, size.height - pad.top - pad.bottom);

    // scaling helpers
    const xToPixel = (t: number) => pad.left + ((t - domain.x0) / (domain.x1 - domain.x0)) * plotW;
    const yToPixel = (mag: number) => pad.top + ((mag - domain.y0) / (domain.y1 - domain.y0)) * plotH; // increasing mag -> downwards
    const pixelToX = (px: number) => domain.x0 + ((px - pad.left) / plotW) * (domain.x1 - domain.x0);
    const pixelToY = (py: number) => domain.y0 + ((py - pad.top) / plotH) * (domain.y1 - domain.y0);

    // ticks
    const xTicks = useMemo(() => {
        const n = Math.min(8, Math.max(3, Math.ceil(plotW / 120)));
        const arr: number[] = [];
        for (let i = 0; i < n; i++) arr.push(domain.x0 + (i / (n - 1)) * (domain.x1 - domain.x0));
        return arr;
    }, [domain, plotW]);
    const yTicks = useMemo(() => {
        const step = 0.5;
        const arr: number[] = [];
        const start = Math.ceil(domain.y0 / step) * step;
        const end = Math.floor(domain.y1 / step) * step;
        for (let val = start; val <= end; val += step) {
            arr.push(val);
        }
        return arr.length > 0 ? arr : [domain.y0, domain.y1];
    }, [domain]);

    // interaction: tooltip, drag-zoom
    const [tooltip, setTooltip] = useState<{ visible: boolean; x: number; y: number; mag?: number; t?: number; band?: string; sigma?: number; nondet?: boolean }>(() => ({ visible: false, x: 0, y: 0 }));

    const dragging = useRef(false);
    const dragStart = useRef<{ x: number; y: number } | null>(null);
    const [selection, setSelection] = useState<{ x: number; y: number; w: number; h: number } | null>(null);
    const selectionCreatedRef = useRef(false);

    const onMouseDown = (e: React.MouseEvent<SVGRectElement>) => {
        const rectEl = (e.currentTarget as SVGRectElement);
        const rect = rectEl.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        dragging.current = true;
        dragStart.current = { x, y };
        // don't create the selection rect yet; only create when movement exceeds threshold
        // disable text selection and touch-action on the chart container while dragging
        try {
            const c = containerRef.current;
            if (c) {
                (c as HTMLElement).style.userSelect = 'none';
                (c as HTMLElement).style.touchAction = 'none';
            }
        } catch (err) {
            console.debug('Lightcurve: unable to update selection styles', err);
        }
        // attach global handlers so drag continues even if cursor leaves the overlay
        selectionCreatedRef.current = false;
        const onWindowMove = (ev: MouseEvent) => {
            if (!dragging.current || !dragStart.current) return;
            const r = rectEl.getBoundingClientRect();
            const mx = ev.clientX - r.left;
            const my = ev.clientY - r.top;
            const dx = Math.abs(mx - dragStart.current!.x);
            const dy = Math.abs(my - dragStart.current!.y);
            const threshold = 6; // pixels before we treat movement as a drag
            const sx = Math.min(dragStart.current.x, mx);
            const sy = Math.min(dragStart.current.y, my);
            const w = Math.abs(mx - dragStart.current.x);
            const h = Math.abs(my - dragStart.current.y);
            if (!selectionCreatedRef.current) {
                if (dx > threshold || dy > threshold) {
                    selectionCreatedRef.current = true;
                    setSelection({ x: sx, y: sy, w, h });
                }
            } else {
                // once created, keep updating even if it shrinks below threshold
                setSelection({ x: sx, y: sy, w, h });
            }
        };

        const onWindowUp = (ev: MouseEvent) => {
            // finalize using the actual mouseup event coordinates to avoid stale selection closure
            if (dragging.current && dragStart.current) {
                const r = rectEl.getBoundingClientRect();
                const mx = ev.clientX - r.left;
                const my = ev.clientY - r.top;
                const sx = Math.min(dragStart.current.x, mx);
                const sy = Math.min(dragStart.current.y, my);
                const w = Math.abs(mx - dragStart.current.x);
                const h = Math.abs(my - dragStart.current.y);
                const threshold = 6;
                if (w > threshold || h > threshold) {
                    const absX0 = pad.left + sx + 0.5;
                    const absX1 = pad.left + sx + w - 0.5;
                    const absY0 = pad.top + sy + 0.5;
                    const absY1 = pad.top + sy + h - 0.5;
                    const x0 = pixelToX(absX0);
                    const x1 = pixelToX(absX1);
                    const y0 = pixelToY(absY0);
                    const y1 = pixelToY(absY1);
                    if (Math.abs(x1 - x0) > 1e-6 && Math.abs(y1 - y0) > 1e-3) {
                        setDomain({ x0: Math.min(x0, x1), x1: Math.max(x0, x1), y0: Math.min(y0, y1), y1: Math.max(y0, y1) });
                    }
                }
            }
            dragging.current = false;
            dragStart.current = null;
            setSelection(null);
            selectionCreatedRef.current = false;
            // restore container selection and touch behavior
            try {
                const c = containerRef.current;
                if (c) {
                    (c as HTMLElement).style.userSelect = '';
                    (c as HTMLElement).style.touchAction = '';
                }
            } catch (err) {
                console.debug('Lightcurve: unable to restore selection styles', err);
            }
            window.removeEventListener('mousemove', onWindowMove);
            window.removeEventListener('mouseup', onWindowUp);
        };

        window.addEventListener('mousemove', onWindowMove);
        window.addEventListener('mouseup', onWindowUp);
    };
    const onMouseMove = (e: React.MouseEvent<SVGRectElement>) => {
        // keep for pointer-based updates when user moves inside the overlay
        const rect = (e.currentTarget as SVGRectElement).getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        if (dragging.current && dragStart.current) {
            const dx = Math.abs(x - dragStart.current.x);
            const dy = Math.abs(y - dragStart.current.y);
            const threshold = 6;
            if (!selection && (dx > threshold || dy > threshold)) {
                const sx = Math.min(dragStart.current.x, x);
                const sy = Math.min(dragStart.current.y, y);
                const w = Math.abs(x - dragStart.current.x);
                const h = Math.abs(y - dragStart.current.y);
                setSelection({ x: sx, y: sy, w, h });
            } else if (selection) {
                const sx = Math.min(dragStart.current.x, x);
                const sy = Math.min(dragStart.current.y, y);
                const w = Math.abs(x - dragStart.current.x);
                const h = Math.abs(y - dragStart.current.y);
                setSelection({ x: sx, y: sy, w, h });
            }
        }
    };
    const onMouseUp = () => {
        // noop: finalization handled by window mouseup handler to ensure consistent behavior
    };

    const onDoubleClick = () => {
        setDomain(initialDomain);
    };

    // (no additional helpers needed right now)

    return (
        <>
        <Card className="@container/card col-span-2 lg:col-span-2">
            <CardContent>
                <div ref={containerRef} style={{ width: '100%', height: '36vh', marginBottom: 20, position: 'relative'}}>
                    <div className="flex items-center justify-between">
                        <div className="text-sm font-medium pb-2">Photometry</div>
                        <div className="flex items-center gap-3">
                            {bands.map(b =>
                                <div
                                    key={`legend-${b}`}
                                    className="flex items-center gap-2 text-xs cursor-pointer select-none"
                                    onClick={() => handleLegendClick(b)}
                                    onDoubleClick={() => handleLegendDoubleClick(b)}
                                    style={{ opacity: hiddenBands.has(b) ? 0.12 : 1, transition: 'opacity 200ms ease' }}
                                >
                                    <div className="w-3 h-3 rounded" style={{ backgroundColor: toColor(b) }} />
                                    <div className="text-xs text-gray-600 dark:text-gray-300">{b.toUpperCase()}</div>
                                </div>
                            )}
                            <button onClick={() => setDialogOpen(true)} title="Expand" className="p-1 rounded hover:bg-slate-100">
                                <Maximize2 className="w-4 h-4 text-gray-600" />
                            </button>
                        </div>
                    </div>

                    <svg width={size.width} height={size.height} onDoubleClick={onDoubleClick}>
                        {/* background */}
                        {/* <rect x={0} y={0} width={size.width} height={size.height} className="fill-transparent" rx={4} /> */}

                        {/* grid and axes */}
                        {/* horizontal grid (y ticks) */}
                        {yTicks.map((yt, i) => {
                            const py = yToPixel(yt);
                            return <line key={`gy-${i}`} x1={pad.left} x2={size.width - pad.right} y1={py} y2={py} className="stroke-[#eef2f6] dark:stroke-slate-700" />;
                        })}
                        {/* vertical grid (x ticks) */}
                        {xTicks.map((xt, i) => {
                            const px = xToPixel(xt);
                            return <line key={`gx-${i}`} x1={px} x2={px} y1={pad.top} y2={pad.top + plotH} className="stroke-[#f3f4f6] dark:stroke-slate-700" />;
                        })}

                        {/* axes labels and ticks */}
                        {/* Y ticks labels (outside left) */}
                        {yTicks.map((yt, i) => {
                            const py = yToPixel(yt);
                            return (
                                <text key={`yt-${i}`} x={pad.left - 8} y={py + 4} textAnchor="end" className="text-xs fill-gray-400 dark:fill-gray-300">{yt.toFixed(2)}</text>
                            );
                        })}
                        {/* X ticks labels */}
                        {xTicks.map((xt, i) => {
                            const px = xToPixel(xt);
                            return (
                                <text key={`xt-${i}`} x={px} y={pad.top + plotH + 20} textAnchor="middle" className="text-xs fill-gray-400 dark:fill-gray-300">{xt.toFixed(1)}</text>
                            );
                        })}

                        {/* axis titles */}
                        <text x={size.width / 2} y={size.height - 8} textAnchor="middle" className="text-sm fill-gray-600 dark:fill-gray-300">MJD</text>
                        <text x={12} y={size.height / 2} transform={`rotate(-90 12 ${size.height / 2})`} textAnchor="middle" className="text-sm fill-gray-600 dark:fill-gray-300">AB mag</text>

                        {/* plotting area clip */}
                        <defs>
                            <clipPath id="plot-area">
                                <rect x={pad.left} y={pad.top} width={plotW} height={plotH} />
                            </clipPath>
                        </defs>

                        {/* points: detections - error bars with clipping */}
                        <g clipPath="url(#plot-area)" style={{ pointerEvents: 'none' }}>
                            {detections.map((pt, i) => {
                                const { bandKey, isHidden, color } = getBandState(pt.band);
                                if (isHidden) return null;
                                const px = xToPixel(pt.t);
                                const sigma = Number(pt.sigma);
                                const hasSigma = Number.isFinite(sigma) && sigma > 0;
                                const capW = 6;
                                return hasSigma ? (
                                    <g key={`errbar-${i}-${bandKey}`}>
                                        <line x1={px} x2={px} y1={yToPixel(pt.mag - sigma)} y2={yToPixel(pt.mag + sigma)} stroke={color} strokeWidth={1.2} style={{ opacity: 0.9, transition: 'opacity 200ms ease' }} />
                                        <line x1={px - capW} x2={px + capW} y1={yToPixel(pt.mag - sigma)} y2={yToPixel(pt.mag - sigma)} stroke={color} strokeWidth={1.2} style={{ opacity: 0.9, transition: 'opacity 200ms ease' }} />
                                        <line x1={px - capW} x2={px + capW} y1={yToPixel(pt.mag + sigma)} y2={yToPixel(pt.mag + sigma)} stroke={color} strokeWidth={1.2} style={{ opacity: 0.9, transition: 'opacity 200ms ease' }} />
                                    </g>
                                ) : null;
                            })}

                            {/* non-detections as downward triangles */}
                            {nondetectionsSeries.map((pt, i) => {
                                const { bandKey, isHidden, color } = getBandState(pt.band);
                                if (isHidden) return null;
                                const px = xToPixel(pt.t);
                                const py = yToPixel(pt.mag);
                                const path = `${px - 5},${py - 1} ${px + 5},${py - 1} ${px},${py + 5}`;
                                return (
                                    <polygon
                                        key={`nd-vis-${i}-${bandKey}`}
                                        points={path}
                                        fill={color}
                                        style={{ opacity: 0.95, transition: 'opacity 200ms ease' }}
                                    />
                                );
                            })}
                        </g>

                        {/* transparent overlay to capture drag events - rendered early so interactive elements are on top */}
                        <rect
                            x={pad.left}
                            y={pad.top}
                            width={plotW}
                            height={plotH}
                            fill="transparent"
                            onMouseDown={onMouseDown}
                            onMouseMove={onMouseMove}
                            onMouseUp={onMouseUp}
                        />

                        {/* Interactive circles and polygons - rendered after overlay so they're on top */}
                        {detections.map((pt, i) => {
                            const { bandKey, isHidden, color } = getBandState(pt.band);
                            if (isHidden) return null;
                            const px = xToPixel(pt.t);
                            const py = yToPixel(pt.mag);
                            return (
                                <g key={`d-hit-${i}-${bandKey}`}>
                                    {/* invisible hit area */}
                                    <circle
                                        cx={px}
                                        cy={py}
                                        r={8}
                                        fill="transparent"
                                        style={{ pointerEvents: 'auto', cursor: 'pointer' }}
                                        onMouseEnter={(e: React.MouseEvent<SVGCircleElement>) => {
                                            const rect = containerRef.current?.getBoundingClientRect();
                                            const clientX = e.clientX;
                                            const clientY = e.clientY;
                                            const x = rect ? clientX - rect.left : clientX;
                                            const y = rect ? clientY - rect.top : clientY;
                                            setTooltip({ visible: true, x, y, mag: pt.mag, t: pt.t, band: pt.band, sigma: Number(pt.sigma), nondet: false });
                                        }}
                                        onMouseMove={(e: React.MouseEvent<SVGCircleElement>) => {
                                            const rect = containerRef.current?.getBoundingClientRect();
                                            const clientX = e.clientX;
                                            const clientY = e.clientY;
                                            const x = rect ? clientX - rect.left : clientX;
                                            const y = rect ? clientY - rect.top : clientY;
                                            setTooltip(prev => ({ ...prev, x, y }));
                                        }}
                                        onMouseLeave={() => setTooltip({ visible: false, x: 0, y: 0 })}
                                    />
                                    {/* visible circle */}
                                    <circle
                                        cx={px}
                                        cy={py}
                                        r={4}
                                        fill={color}
                                        style={{ opacity: 1, transition: 'opacity 200ms ease, r 120ms ease', pointerEvents: 'none' }}
                                    />
                                </g>
                            );
                        })}

                        {/* Interactive non-detection polygons */}
                        {nondetectionsSeries.map((pt, i) => {
                            const { bandKey, isHidden, color } = getBandState(pt.band);
                            if (isHidden) return null;
                            const px = xToPixel(pt.t);
                            const py = yToPixel(pt.mag);
                            const path = `${px - 5},${py - 1} ${px + 5},${py - 1} ${px},${py + 5}`;
                            return (
                                <g key={`nd-hit-${i}-${bandKey}`}>
                                    {/* invisible hit area */}
                                    <circle
                                        cx={px}
                                        cy={py}
                                        r={8}
                                        fill="transparent"
                                        style={{ pointerEvents: 'auto', cursor: 'pointer' }}
                                        onMouseEnter={(e: React.MouseEvent<SVGCircleElement>) => {
                                            const rect = containerRef.current?.getBoundingClientRect();
                                            const clientX = e.clientX;
                                            const clientY = e.clientY;
                                            const x = rect ? clientX - rect.left : clientX;
                                            const y = rect ? clientY - rect.top : clientY;
                                            setTooltip({ visible: true, x, y, mag: pt.mag, t: pt.t, band: pt.band, nondet: true });
                                        }}
                                        onMouseMove={(e: React.MouseEvent<SVGCircleElement>) => {
                                            const rect = containerRef.current?.getBoundingClientRect();
                                            const clientX = e.clientX;
                                            const clientY = e.clientY;
                                            const x = rect ? clientX - rect.left : clientX;
                                            const y = rect ? clientY - rect.top : clientY;
                                            setTooltip(prev => ({ ...prev, x, y }));
                                        }}
                                        onMouseLeave={() => setTooltip({ visible: false, x: 0, y: 0 })}
                                    />
                                    {/* visible polygon */}
                                    <polygon
                                        points={path}
                                        fill={color}
                                        style={{ opacity: 0.95, transition: 'opacity 200ms ease', pointerEvents: 'none' }}
                                    />
                                </g>
                            );
                        })}

                        {/* selection rect */}
                        {selection && (
                            <rect x={pad.left + selection.x} y={pad.top + selection.y} width={selection.w} height={selection.h} fill="#3b82f6" opacity={0.12} stroke="#3b82f6" strokeDasharray="4 2" />
                        )}
                    </svg>

                    {/* tooltip outside SVG */}
                    {tooltip.visible && (
                        <div style={{ position: 'absolute', left: tooltip.x + 12, top: tooltip.y + 12, zIndex: 50, pointerEvents: 'none' }}>
                            <div className="bg-white dark:bg-slate-800 text-xs border border-gray-300 dark:border-slate-600 rounded shadow-lg p-2 dark:text-gray-100" style={{ minWidth: 140 }}>
                                <div className="font-medium">Band: {String(tooltip.band).toUpperCase()}{tooltip.nondet ? ' (non-det)' : ''}</div>
                                <div>MJD: {tooltip.t?.toFixed(3)}</div>
                                {!tooltip.nondet && (
                                    <div>Mag: {tooltip.mag?.toFixed(3)} {tooltip.sigma !== undefined && Number.isFinite(tooltip.sigma) && tooltip.sigma > 0 ? `± ${tooltip.sigma?.toFixed(3)}` : ''}</div>
                                )}
                                <div>Lim mag: {tooltip.mag?.toFixed(3)}</div>
                            </div>
                        </div>
                    )}
                </div>
            </CardContent>
        </Card>

        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
            <DialogContent className="w-[min(1400px,95vw)] max-w-none sm:!max-w-none h-[90vh] flex flex-col">
                <DialogHeader>
                    <DialogTitle className="text-xl">Photometry - Expanded View</DialogTitle>
                </DialogHeader>
                <div className="flex-1 flex items-center justify-center overflow-hidden">
                    <svg width={1200} height={600} onDoubleClick={onDoubleClick}>
                        {/* Same structure as main plot but with larger dimensions */}
                        {(() => {
                            const dialogPad = { left: 80, right: 30, top: 30, bottom: 60 };
                            const dialogW = 1200;
                            const dialogH = 600;
                            const dialogPlotW = dialogW - dialogPad.left - dialogPad.right;
                            const dialogPlotH = dialogH - dialogPad.top - dialogPad.bottom;
                            const xToPixelDialog = (t: number) => dialogPad.left + ((t - domain.x0) / (domain.x1 - domain.x0)) * dialogPlotW;
                            const yToPixelDialog = (mag: number) => dialogPad.top + ((mag - domain.y0) / (domain.y1 - domain.y0)) * dialogPlotH;

                            return (
                                <>
                                    {/* grid */}
                                    {yTicks.map((yt, i) => {
                                        const py = yToPixelDialog(yt);
                                        return <line key={`gy-${i}`} x1={dialogPad.left} x2={dialogW - dialogPad.right} y1={py} y2={py} className="stroke-[#eef2f6] dark:stroke-slate-700" />;
                                    })}
                                    {xTicks.map((xt, i) => {
                                        const px = xToPixelDialog(xt);
                                        return <line key={`gx-${i}`} x1={px} x2={px} y1={dialogPad.top} y2={dialogPad.top + dialogPlotH} className="stroke-[#f3f4f6] dark:stroke-slate-700" />;
                                    })}

                                    {/* axis labels */}
                                    {yTicks.map((yt, i) => {
                                        const py = yToPixelDialog(yt);
                                        return <text key={`yt-${i}`} x={dialogPad.left - 10} y={py + 4} textAnchor="end" className="text-sm fill-gray-400 dark:fill-gray-300">{yt.toFixed(2)}</text>;
                                    })}
                                    {xTicks.map((xt, i) => {
                                        const px = xToPixelDialog(xt);
                                        return <text key={`xt-${i}`} x={px} y={dialogPad.top + dialogPlotH + 25} textAnchor="middle" className="text-sm fill-gray-400 dark:fill-gray-300">{xt.toFixed(1)}</text>;
                                    })}

                                    <text x={dialogW / 2} y={dialogH - 15} textAnchor="middle" className="text-base fill-gray-600 dark:fill-gray-300">MJD</text>
                                    <text x={20} y={dialogH / 2} transform={`rotate(-90 20 ${dialogH / 2})`} textAnchor="middle" className="text-base fill-gray-600 dark:fill-gray-300">AB mag</text>

                                    {/* clip path for dialog */}
                                    <defs>
                                        <clipPath id="plot-area-dialog">
                                            <rect x={dialogPad.left} y={dialogPad.top} width={dialogPlotW} height={dialogPlotH} />
                                        </clipPath>
                                    </defs>

                                    {/* error bars */}
                                    <g clipPath="url(#plot-area-dialog)" style={{ pointerEvents: 'none' }}>
                                        {detections.map((pt, i) => {
                                            const { bandKey, isHidden, color } = getBandState(pt.band);
                                            if (isHidden) return null;
                                            const px = xToPixelDialog(pt.t);
                                            const sigma = Number(pt.sigma);
                                            const hasSigma = Number.isFinite(sigma) && sigma > 0;
                                            const capW = 8;
                                            return hasSigma ? (
                                                <g key={`errbar-${i}-${bandKey}`}>
                                                    <line x1={px} x2={px} y1={yToPixelDialog(pt.mag - sigma)} y2={yToPixelDialog(pt.mag + sigma)} stroke={color} strokeWidth={1.5} style={{ opacity: 0.9, transition: 'opacity 200ms ease' }} />
                                                    <line x1={px - capW} x2={px + capW} y1={yToPixelDialog(pt.mag - sigma)} y2={yToPixelDialog(pt.mag - sigma)} stroke={color} strokeWidth={1.5} style={{ opacity: 0.9, transition: 'opacity 200ms ease' }} />
                                                    <line x1={px - capW} x2={px + capW} y1={yToPixelDialog(pt.mag + sigma)} y2={yToPixelDialog(pt.mag + sigma)} stroke={color} strokeWidth={1.5} style={{ opacity: 0.9, transition: 'opacity 200ms ease' }} />
                                                </g>
                                            ) : null;
                                        })}

                                        {nondetectionsSeries.map((pt, i) => {
                                            const { bandKey, isHidden, color } = getBandState(pt.band);
                                            if (isHidden) return null;
                                            const px = xToPixelDialog(pt.t);
                                            const py = yToPixelDialog(pt.mag);
                                            const path = `${px - 6},${py - 1} ${px + 6},${py - 1} ${px},${py + 6}`;
                                            return (
                                                <polygon key={`nd-vis-${i}-${bandKey}`} points={path} fill={color} style={{ opacity: 0.95, transition: 'opacity 200ms ease' }} />
                                            );
                                        })}
                                    </g>

                                    {/* detection points */}
                                    {detections.map((pt, i) => {
                                        const { bandKey, isHidden, color } = getBandState(pt.band);
                                        if (isHidden) return null;
                                        const px = xToPixelDialog(pt.t);
                                        const py = yToPixelDialog(pt.mag);
                                        return (
                                            <circle key={`d-${i}-${bandKey}`} cx={px} cy={py} r={5} fill={color} style={{ opacity: 1, transition: 'opacity 200ms ease, r 120ms ease' }} />
                                        );
                                    })}

                                    {/* non-detection points */}
                                    {nondetectionsSeries.map((pt, i) => {
                                        const { bandKey, isHidden, color } = getBandState(pt.band);
                                        if (isHidden) return null;
                                        const px = xToPixelDialog(pt.t);
                                        const py = yToPixelDialog(pt.mag);
                                        const path = `${px - 6},${py - 1} ${px + 6},${py - 1} ${px},${py + 6}`;
                                        return (
                                            <polygon key={`nd-${i}-${bandKey}`} points={path} fill={color} style={{ opacity: 0.95, transition: 'opacity 200ms ease' }} />
                                        );
                                    })}
                                </>
                            );
                        })()}
                    </svg>
                </div>
            </DialogContent>
        </Dialog>
        </>
    );
}
