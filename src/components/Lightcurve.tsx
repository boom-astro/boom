import React, { useMemo, useRef, useState, useEffect } from 'react';
import { Card, CardContent } from './ui/card';

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

    // hover state from legend (same semantics as centroid plot)
    const [hoveredBand, setHoveredBand] = useState<string | null>(null);

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
        const n = 6;
        const arr: number[] = [];
        for (let i = 0; i < n; i++) arr.push(domain.y0 + (i / (n - 1)) * (domain.y1 - domain.y0));
        return arr;
    }, [domain]);

    // interaction: tooltip, drag-zoom
    const [tooltip, setTooltip] = useState<{ visible: boolean; x: number; y: number; mag?: number; t?: number; band?: string; nondet?: boolean }>(() => ({ visible: false, x: 0, y: 0 }));

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
            // ignore (SSR or restricted env)
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
                // ignore
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
        <Card className="@container/card col-span-2 lg:col-span-2">
            <CardContent>
                <div ref={containerRef} style={{ width: '100%', height: 360, marginBottom: 20}}>
                    <div className="flex items-center justify-between">
                        <div className="text-sm font-medium pb-2">Photometry</div>
                        <div className="flex items-center gap-3">
                            {bands.map(b => {
                                const isActiveLegend = !hoveredBand || hoveredBand === b;
                                return (
                                <div
                                    key={`legend-${b}`}
                                    className="flex items-center gap-2 text-xs cursor-pointer select-none"
                                    onMouseEnter={() => setHoveredBand(b)}
                                    onMouseLeave={() => setHoveredBand(null)}
                                    style={{ opacity: isActiveLegend ? 1 : 0.12, transition: 'opacity 200ms ease' }}
                                >
                                    <div className="w-3 h-3 rounded" style={{ backgroundColor: toColor(b) }} />
                                    <div className="text-xs text-gray-600 dark:text-gray-300">{b.toUpperCase()}</div>
                                </div>
                                );
                            })}
                        </div>
                    </div>

                    <svg width={size.width} height={size.height} onDoubleClick={onDoubleClick}>
                        {/* background */}
                        <rect x={0} y={0} width={size.width} height={size.height} className="fill-white dark:fill-slate-900" rx={4} />

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

                        {/* points: detections */}
                        <g clipPath="url(#plot-area)">
                            {detections.map((pt, i) => {
                                const px = xToPixel(pt.t);
                                const py = yToPixel(pt.mag);
                                const bandKey = String(pt.band ?? 'default').toLowerCase();
                                const isActive = !hoveredBand || hoveredBand === bandKey;
                                                                const sigma = Number((pt as any).sigma);
                                                                const hasSigma = Number.isFinite(sigma) && sigma > 0;
                                                                const color = toColor(pt.band);
                                                                const capW = 6;
                                                                return (
                                                                        <g key={`d-${i}-${bandKey}`}>
                                                                            {hasSigma && (
                                                                                (() => {
                                                                                    const pyTop = yToPixel(pt.mag - sigma);
                                                                                    const pyBottom = yToPixel(pt.mag + sigma);
                                                                                    return (
                                                                                        <g>
                                                                                            <line x1={px} x2={px} y1={pyTop} y2={pyBottom} stroke={color} strokeWidth={1.2} style={{ opacity: isActive ? 0.9 : 0.12, transition: 'opacity 200ms ease' }} />
                                                                                            <line x1={px - capW} x2={px + capW} y1={pyTop} y2={pyTop} stroke={color} strokeWidth={1.2} style={{ opacity: isActive ? 0.9 : 0.12, transition: 'opacity 200ms ease' }} />
                                                                                            <line x1={px - capW} x2={px + capW} y1={pyBottom} y2={pyBottom} stroke={color} strokeWidth={1.2} style={{ opacity: isActive ? 0.9 : 0.12, transition: 'opacity 200ms ease' }} />
                                                                                        </g>
                                                                                    );
                                                                                })()
                                                                            )}
                                                                            <circle
                                                                                cx={px}
                                                                                cy={py}
                                                                                r={isActive ? 4 : 2}
                                                                                fill={color}
                                                                                style={{ opacity: isActive ? 1 : 0.12, transition: 'opacity 200ms ease, r 120ms ease' }}
                                                                                onMouseEnter={() => setTooltip({ visible: true, x: px, y: py, mag: pt.mag, t: pt.t, band: pt.band, nondet: false })}
                                                                                onMouseMove={(e) => {
                                                                                        const rect = (e.currentTarget as SVGCircleElement).ownerSVGElement?.getBoundingClientRect();
                                                                                        if (rect) setTooltip(t => ({ ...t, x: e.clientX - rect.left, y: e.clientY - rect.top }));
                                                                                }}
                                                                                onMouseLeave={() => setTooltip({ visible: false, x: 0, y: 0 })}
                                                                            />
                                                                        </g>
                                                                );
                            })}

                            {/* non-detections as downward triangles */}
                            {nondetectionsSeries.map((pt, i) => {
                                const px = xToPixel(pt.t);
                                const py = yToPixel(pt.mag);
                                const bandKey = String(pt.band ?? 'default').toLowerCase();
                                const isActive = !hoveredBand || hoveredBand === bandKey;
                                const sizeTri = isActive ? 5 : 4;
                                const path = `${px - sizeTri},${py - 1} ${px + sizeTri},${py - 1} ${px},${py + sizeTri}`; // down facing triangle
                                return (
                                    <polygon
                                        key={`nd-${i}-${bandKey}`}
                                        points={path}
                                        fill={toColor(pt.band)}
                                        style={{ opacity: isActive ? 0.95 : 0.12, transition: 'opacity 200ms ease' }}
                                        onMouseEnter={() => setTooltip({ visible: true, x: px, y: py, mag: pt.mag, t: pt.t, band: pt.band, nondet: true })}
                                        onMouseMove={(e) => {
                                            const rect = (e.currentTarget as SVGPolygonElement).ownerSVGElement?.getBoundingClientRect();
                                            if (rect) setTooltip(t => ({ ...t, x: e.clientX - rect.left, y: e.clientY - rect.top }));
                                        }}
                                        onMouseLeave={() => setTooltip({ visible: false, x: 0, y: 0 })}
                                    />
                                );
                            })}
                        </g>

                        {/* transparent overlay to capture drag events */}
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

                        {/* selection rect */}
                        {selection && (
                            <rect x={pad.left + selection.x} y={pad.top + selection.y} width={selection.w} height={selection.h} fill="#3b82f6" opacity={0.12} stroke="#3b82f6" strokeDasharray="4 2" />
                        )}

                        {/* tooltip */}
                        {tooltip.visible && (
                            <g>
                                <foreignObject x={tooltip.x + 12} y={tooltip.y + 12} width={220} height={80}>
                                    <div className="bg-white dark:bg-slate-800 text-xs border rounded shadow p-2" style={{ pointerEvents: 'none' }}>
                                        <div className="font-medium">{String(tooltip.band).toUpperCase()}{tooltip.nondet ? ' (non-detection)' : ''}</div>
                                        <div>MJD: {tooltip.t?.toFixed(3)}</div>
                                        <div>Mag: {tooltip.mag?.toFixed(3)}</div>
                                    </div>
                                </foreignObject>
                            </g>
                        )}

                    </svg>
                </div>
            </CardContent>
        </Card>
    );
}
