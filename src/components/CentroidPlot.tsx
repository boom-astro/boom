import { useMemo, useState, useRef, useEffect } from 'react';
import type React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Maximize2 } from 'lucide-react';
import useAppStore from '@/lib/store';

// band -> color map matching Lightcurve's palette
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

export default function CentroidPlot() {
  const current = useAppStore(state => state.currentSource);
  const prv = ((current && (current['data'] as Record<string, unknown> | undefined))?.['prv_candidates']) ?? [] as unknown[];

  const [hoveredBand, setHoveredBand] = useState<string | null>(null);

  type Candidate = {
    ra?: number | string;
    dec?: number | string;
    band?: string;
    jd?: number | string;
    magpsf?: number | string;
    [k: string]: unknown;
  };

  const { points, centroidRa, centroidDec, maxOffsetArcsec, bandCentroids } = useMemo(() => {
    const rows = Array.isArray(prv) ? (prv as unknown[]) : [];
    // build array preserving original row for tooltip
    const coords = rows.map((rRaw: unknown) => {
      const r = (rRaw ?? {}) as Candidate;
      return { row: r, ra: Number(r.ra), dec: Number(r.dec), band: r.band };
    });

    const valid = coords.filter(c => Number.isFinite(c.ra) && Number.isFinite(c.dec));
    if (valid.length === 0) {
      return { points: [], centroidRa: null, centroidDec: null, maxOffsetArcsec: 1 };
    }

    // Spherical (vector) centroid to avoid RA wrap issues
    // convert to unit vectors
    const vecs = valid.map(c => {
      const raRad = (c.ra * Math.PI) / 180;
      const decRad = (c.dec * Math.PI) / 180;
      return {
        x: Math.cos(decRad) * Math.cos(raRad),
        y: Math.cos(decRad) * Math.sin(raRad),
        z: Math.sin(decRad),
      };
    });
    const sum = vecs.reduce((s, v) => ({ x: s.x + v.x, y: s.y + v.y, z: s.z + v.z }), { x: 0, y: 0, z: 0 });
    const len = Math.sqrt(sum.x * sum.x + sum.y * sum.y + sum.z * sum.z);
    const avg = { x: sum.x / len, y: sum.y / len, z: sum.z / len };
    const centroidDecVal = Math.asin(avg.z) * (180 / Math.PI);
    const centroidRaVal = (Math.atan2(avg.y, avg.x) * (180 / Math.PI) + 360) % 360;

    // compute offsets in arcsec using minimal RA difference
    const points = valid.map(c => {
      // compute minimal RA difference in degrees
      let draDeg = c.ra - centroidRaVal;
      while (draDeg <= -180) draDeg += 360;
      while (draDeg > 180) draDeg -= 360;
      const dra = draDeg * Math.cos((centroidDecVal * Math.PI) / 180) * 3600;
      const ddec = (c.dec - centroidDecVal) * 3600;
      return { x: dra, y: ddec, band: c.band, row: c.row };
    });

    // determine zoom from the furthest point (radial separation)
    const seps = points.map(p => Math.sqrt(p.x * p.x + p.y * p.y));
    console.log("Centroid plot separations (arcsec):", seps);
    const finiteSeps = seps.filter(s => Number.isFinite(s) && s >= 0);
    console.log("Centroid plot finite separations (arcsec):", finiteSeps);
    const maxSep = finiteSeps.length ? Math.max(...finiteSeps) : 1;
    console.log("Centroid plot max separation (arcsec):", maxSep);
    // use furthest point with padding (×1.5) to set zoom radius
    let maxOffsetArcsec = Math.max(0.25, maxSep) * 1.5;
    const step = 0.5;
    maxOffsetArcsec = Math.ceil(maxOffsetArcsec / step) * step;
    // compute per-band centroids (mean offsets in arcsec)
    const bandMap: Record<string, { sumX: number; sumY: number; count: number }> = {};
    points.forEach(p => {
      const key = (p.band ?? 'default') as string;
      if (!bandMap[key]) bandMap[key] = { sumX: 0, sumY: 0, count: 0 };
      if (Number.isFinite(p.x) && Number.isFinite(p.y)) {
        bandMap[key].sumX += p.x;
        bandMap[key].sumY += p.y;
        bandMap[key].count += 1;
      }
    });
    const bandCentroids: Record<string, { x: number; y: number; count: number }> = {};
    Object.entries(bandMap).forEach(([k, v]) => {
      if (v.count > 0) bandCentroids[k] = { x: v.sumX / v.count, y: v.sumY / v.count, count: v.count };
    });
    return { points, centroidRa: centroidRaVal, centroidDec: centroidDecVal, maxOffsetArcsec, bandCentroids };
  }, [prv]);

  const size = 320;
  const padding = 28;
  const inner = size - padding * 2;
  const tickCount = 5;

  const containerRef = useRef<HTMLDivElement | null>(null);
  const [tooltip, setTooltip] = useState<{ visible: boolean; x: number; y: number; point?: { x: number; y: number; band?: string; row?: Candidate } }>(() => ({ visible: false, x: 0, y: 0 }));
  const [dialogOpen, setDialogOpen] = useState(false);

  // Clear hoveredBand when opening the dialog to avoid focus-driven hover
  useEffect(() => {
    if (dialogOpen) setHoveredBand(null);
  }, [dialogOpen]);

  const timeSeries = useMemo(() => {
    const rows = points
      .map(p => ({
        t: p.row && p.row.jd !== undefined ? Number(p.row.jd) : NaN,
        dra: p.x,
        ddec: p.y,
        band: p.band,
      }))
      .filter(r => Number.isFinite(r.t))
      .map(r => ({ t: r.t - 2400000.5, dra: r.dra, ddec: r.ddec, band: r.band }));
    rows.sort((a, b) => a.t - b.t);
    const series = rows.map(r => ({ t: r.t, dra: r.dra, ddec: r.ddec, sep: Math.sqrt(r.dra * r.dra + r.ddec * r.ddec), band: r.band }));
    return series;
  }, [points]);

  return (
    <>
    <Card data-slot="card" className="col-span-1">
      <CardContent>
        <div className="pt-0 pb-2 flex items-center justify-between">
          <CardTitle className="text-lg">Centroid Plot</CardTitle>
          <div>
            <button onClick={() => setDialogOpen(true)} title="Expand" className="p-1 rounded hover:bg-slate-100">
              <Maximize2 className="w-4 h-4 text-gray-600" />
            </button>
          </div>
        </div>
        {!points || points.length === 0 ? (
          <div className="text-sm text-gray-500">No previous detections available to compute centroid.</div>
        ) : (
          <div className="flex flex-col gap-1">
            <div className="flex items-center justify-between">
              <div className="text-sm text-gray-600">Centroid: {centroidRa?.toFixed(6)}, {centroidDec?.toFixed(6)}</div>
              <div className="flex items-center gap-2">
                {(() => {
                  const present = new Set(points.map(p => String(p.band ?? '').toLowerCase()).filter(Boolean));
                  return Object.entries(BAND_COLORS)
                    .filter(([k]) => k !== 'default' && present.has(k))
                    .map(([band, color]) => (
                      <div
                        key={band}
                        role="button"
                        tabIndex={0}
                        onMouseEnter={() => setHoveredBand(band)}
                        onMouseLeave={() => setHoveredBand(null)}
                        onFocus={() => setHoveredBand(band)}
                        onBlur={() => setHoveredBand(null)}
                        className="flex items-center gap-2 text-xs cursor-pointer select-none"
                      >
                        <div className="w-3 h-3 rounded" style={{ backgroundColor: color }} />
                        <div className="text-xs">{band.toUpperCase()}</div>
                      </div>
                    ));
                })()}
              </div>
            </div>

            <div ref={containerRef} className="w-full overflow-auto relative">
              <svg width={size} height={size} className="block mx-auto bg-transparent">
                {/* center lines */}
                <line x1={padding} y1={size/2} x2={size-padding} y2={size/2} stroke="#e5e7eb" strokeWidth={1} />
                <line x1={size/2} y1={padding} x2={size/2} y2={size-padding} stroke="#e5e7eb" strokeWidth={1} />

                {/* axis ticks, labels and grid */}
                {(() => {
                  // choose nice tick spacing based on maxOffsetArcsec
                  const ticks: number[] = [];
                  if (maxOffsetArcsec && isFinite(maxOffsetArcsec)) {
                    const absMax = Math.max(1, Math.ceil(maxOffsetArcsec));
                    // nice step: round to 1,2,5 * 10^n
                    const rawStep = (absMax * 2) / (tickCount - 1);
                    const pow = Math.pow(10, Math.floor(Math.log10(rawStep)));
                    const norm = rawStep / pow;
                    let stepNorm = 1;
                    if (norm <= 1) stepNorm = 1;
                    else if (norm <= 2) stepNorm = 2;
                    else if (norm <= 5) stepNorm = 5;
                    else stepNorm = 10;
                    const step = stepNorm * pow;
                    // center ticks around 0
                    const n = Math.ceil((absMax * 1.1) / step);
                    for (let i = -n; i <= n; i++) ticks.push(i * step);
                  }
                  return (
                    <g>
                      {/* axis labels */}
                      <text x={size/2} y={size - 4} fontSize={12} textAnchor="middle" fill="#9b8f8fff">ΔRA (arcsec)</text>
                      <text x={8} y={size/2} fontSize={12} textAnchor="middle" fill="#9b8f8fff" transform={`rotate(-90 8 ${size/2})`}>ΔDec (arcsec)</text>
                    </g>
                  )
                })()}

                {/* reference circles at 0.5" and 1" */}
                {/* {[0.25, 0.5, 1].map((arcsec, i) => {
                  const r = (arcsec / maxOffsetArcsec) * (inner/2);
                  return (
                    <circle key={i} cx={size/2} cy={size/2} r={r} stroke="#e5e7eb" strokeWidth={0.8} fill="none" strokeDasharray="3 3" />
                  )
                })} */}
                {/* instead make sure we have circles at 0.25, 0.5, 1, and then every n-1 * 2 increments above that, as long as n-1 instead above the max distance  */}
                {(() => {
                  const circles: number[] = [];
                  if (maxOffsetArcsec && isFinite(maxOffsetArcsec)) {
                    // always add 0.25, 0.5, 1
                    circles.push(0.25);
                    circles.push(0.5);
                    circles.push(1);
                    // then add every n * 2 above that
                    const step = 0.5 * 2;
                    let next = 1 + step;
                    while (next < maxOffsetArcsec * 1.1) {
                      circles.push(next);
                      next += step;
                    }
                  }
                  return circles.map((arcsec, i) => {
                    const r = (arcsec / maxOffsetArcsec) * (inner/2);
                    return (
                      <circle key={i} cx={size/2} cy={size/2} r={r} stroke="#e5e7eb" strokeWidth={0.8} fill="none" strokeDasharray="3 3" />
                    )
                  });
                })()}
                {/* points */}
                {points.map((p, idx) => {
                  const px = size/2 + (p.x / maxOffsetArcsec) * (inner/2);
                  const py = size/2 - (p.y / maxOffsetArcsec) * (inner/2);
                  const color = toColor(p.band);
                  const bandKey = p.band ?? 'default';
                  const isActive = !hoveredBand || hoveredBand === bandKey;
                  return (
                    <circle
                      key={idx}
                      cx={px}
                      cy={py}
                      r={isActive ? 3.5 : 2}
                      fill={color}
                      opacity={isActive ? 0.9 : 0.12}
                      stroke={color}
                      strokeWidth={isActive ? 0.9 : 0.6}
                      style={{ transition: 'opacity 200ms ease, stroke-width 200ms ease' }}
                      onMouseEnter={(e: React.MouseEvent<SVGCircleElement>) => {
                        const rect = containerRef.current?.getBoundingClientRect();
                        const clientX = e.clientX;
                        const clientY = e.clientY;
                        const x = rect ? clientX - rect.left : clientX;
                        const y = rect ? clientY - rect.top : clientY;
                        setTooltip({ visible: true, x, y, point: p });
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
                  )
                })}

                {/* per-band centroid when hovering a band in the legend */}
                {hoveredBand && bandCentroids && bandCentroids[hoveredBand] && (() => {
                  const bc = bandCentroids[hoveredBand];
                  const px = size/2 + (bc.x / maxOffsetArcsec) * (inner/2);
                  const py = size/2 - (bc.y / maxOffsetArcsec) * (inner/2);
                  const color = BAND_COLORS[hoveredBand] ?? BAND_COLORS.default;
                  return (
                    <g style={{ transition: 'opacity 200ms ease, transform 200ms ease', opacity: 0.98 }}>
                      {/* prominent cross only (no annulus/text) */}
                      <line x1={px - 8} y1={py} x2={px + 8} y2={py} stroke={color} strokeWidth={2.2} strokeOpacity={0.98} strokeLinecap="round" />
                      <line x1={px} y1={py - 8} x2={px} y2={py + 8} stroke={color} strokeWidth={2.2} strokeOpacity={0.98} strokeLinecap="round" />
                    </g>
                  )
                })()}

                {/* centroid marker */}
                <g>
                  <circle cx={size/2} cy={size/2} r={5} fill="none" stroke="#111827" strokeWidth={1.5} />
                  <circle cx={size/2} cy={size/2} r={2} fill="#111827" />
                </g>
              </svg>

              {tooltip.visible && tooltip.point && (
                <div style={{ left: tooltip.x + 12, top: tooltip.y + 12 }} className="absolute z-50 pointer-events-none">
                  <div className="bg-white text-xs border rounded shadow p-2 dark:bg-slate-800 dark:text-gray-100 dark:border-slate-700" style={{ minWidth: 180 }}>
                    <div className="font-medium">Band: {String(tooltip.point.band).toUpperCase()}</div>
                    <div>RA offset: {tooltip.point.x.toFixed(3)}″</div>
                    <div>Dec offset: {tooltip.point.y.toFixed(3)}″</div>
                    <div>Separation: {(Math.sqrt(tooltip.point.x*tooltip.point.x + tooltip.point.y*tooltip.point.y)).toFixed(3)}″</div>
                    {tooltip.point.row?.jd !== undefined && typeof tooltip.point.row?.jd === 'number' && <div>MJD: {(tooltip.point.row.jd - 2400000.5).toFixed(3)}</div>}
                    {tooltip.point.row?.magpsf !== undefined && <div>Mag: {Number(tooltip.point.row.magpsf).toFixed(3)}</div>}
                  </div>
                </div>
              )}
            </div>

            <div className="mt-2 text-xs text-gray-400 pt-2">
                <div>Points: {points.length} · maxSep: {(Math.max(0, ((() => { const s = points.map(p => Math.sqrt(p.x*p.x + p.y*p.y)).filter(n=>Number.isFinite(n)); return s.length?Math.max(...s):0 })())))?.toFixed(3)}″ · zoom: {maxOffsetArcsec.toFixed(3)}″</div>
                <div className="text-xs text-gray-600">Offsets shown in arcsec relative to centroid (0,0).</div>
            </div>
          </div>
        )}
      </CardContent>
      </Card>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="w-[min(1200px,95vw)] max-w-none sm:!max-w-none max-h-[90vh] overflow-auto">
          <DialogHeader>
            <DialogTitle className="text-xl">Centroid Time Series</DialogTitle>
          </DialogHeader>

          <div className="mt-2">
            {timeSeries.length === 0 ? (
              <div className="text-sm text-gray-500 p-4">No time-series data (missing JD) available.</div>
            ) : (
              (() => {
                const w = 1000;
                const h = 580;
                const pad = { left: 80, right: 20, top: 16, bottom: 40 };
                const panelGap = 20; // gap between stacked panels
                const plotW = w - pad.left - pad.right;
                const plotH = Math.floor((h - pad.top - pad.bottom - panelGap * 2) / 3);
                const panelInnerPad = 8; // inner padding inside each panel
                const times = timeSeries.map(d => d.t);
                const tMin = Math.min(...times);
                const tMax = Math.max(...times) || tMin + 1;

                const draValues = timeSeries.map(d => d.dra);
                const ddecValues = timeSeries.map(d => d.ddec);
                const sepValues = timeSeries.map(d => d.sep);

                const makeTicks = (min: number, max: number, n = 4) => {
                  const ticks: number[] = [];
                  for (let i = 0; i <= n; i++) ticks.push(min + (i / n) * (max - min));
                  return ticks;
                };

                const timeTicks = (() => {
                  const n = Math.min(6, Math.max(2, Math.ceil(timeSeries.length / 1)));
                  const ticks: number[] = [];
                  for (let i = 0; i < n; i++) ticks.push(tMin + (i / (n - 1)) * (tMax - tMin));
                  return ticks;
                })();

                return (
                  <div>
                    {/* Legend for dialog (controls hover filtering) */}
                    <div className="flex items-center gap-3 mb-3">
                      {(() => {
                        const present = new Set(timeSeries.map(s => String(s.band ?? '').toLowerCase()).filter(Boolean));
                        return Object.entries(BAND_COLORS)
                          .filter(([k]) => k !== 'default' && present.has(k))
                          .map(([band, color]) => (
                            <div
                              key={`dlg-legend-${band}`}
                              role="button"
                              onMouseEnter={() => setHoveredBand(band)}
                              onMouseLeave={() => setHoveredBand(null)}
                              className="flex items-center gap-2 text-xs cursor-pointer select-none"
                            >
                              <div className="w-3 h-3 rounded" style={{ backgroundColor: color }} />
                              <div className="text-xs text-gray-600 dark:text-gray-300">{band.toUpperCase()}</div>
                            </div>
                          ));
                      })()}
                    </div>

                  <svg width={w} height={h} className="mx-auto">
                    {/* Panel backgrounds & separators */}
                    {(() => {
                      const boxes = [0, 1, 2].map(i => ({
                        x: pad.left,
                        y: pad.top + i * (plotH + panelGap),
                        w: plotW,
                        h: plotH,
                      }));
                      return (
                        <g>
                          {boxes.map((b, i) => (
                            <g key={`box-${i}`}>
                              <rect x={b.x} y={b.y} width={b.w} height={b.h} className="fill-white dark:fill-slate-800 stroke-[#eef2f6] dark:stroke-slate-700" strokeWidth={1} rx={6} />
                            </g>
                          ))}
                        </g>
                      );
                    })()}

                    {/* ΔRA panel */}
                    {(() => {
                      const i = 0;
                      const panelTop = pad.top + i * (plotH + panelGap);
                      const values = draValues;
                      const minY = Math.min(...values);
                      const maxY = Math.max(...values);
                      const padY = (maxY - minY) * 0.12 || Math.max(0.5, Math.abs(maxY) * 0.1);
                      const y0 = minY - padY;
                      const y1 = maxY + padY;
                      const yTicks = makeTicks(y0, y1, 4);
                      return (
                        <g transform={`translate(0, ${panelTop})`}>
                          {yTicks.map((yt, j) => {
                            const yy = panelInnerPad + plotH - ((yt - y0) / (y1 - y0)) * plotH;
                            return <line key={j} x1={pad.left} x2={w - pad.right} y1={yy} y2={yy} className="stroke-[#eef2f6] dark:stroke-slate-700" strokeWidth={1} />;
                          })}
                          {yTicks.map((yt, j) => {
                            const yy = panelInnerPad + plotH - ((yt - y0) / (y1 - y0)) * plotH;
                            return <line key={`tick3-${j}`} x1={pad.left - 6} x2={pad.left} y1={yy} y2={yy} className="stroke-[#cbd5e1] dark:stroke-slate-600" strokeWidth={1} />;
                          })}
                          {yTicks.map((yt, j) => {
                            const yy = panelInnerPad + plotH - ((yt - y0) / (y1 - y0)) * plotH;
                            return <line key={`tick2-${j}`} x1={pad.left - 6} x2={pad.left} y1={yy} y2={yy} className="stroke-[#cbd5e1] dark:stroke-slate-600" strokeWidth={1} />;
                          })}
                          {yTicks.map((yt, j) => {
                            const yy = panelInnerPad + plotH - ((yt - y0) / (y1 - y0)) * plotH;
                            return <line key={`tick-${j}`} x1={pad.left - 6} x2={pad.left} y1={yy} y2={yy} className="stroke-[#cbd5e1] dark:stroke-slate-600" strokeWidth={1} />;
                          })}
                          {timeTicks.map((tt, j) => {
                            const xx = pad.left + ((tt - tMin) / (tMax - tMin)) * plotW;
                            return <line key={`v${j}`} x1={xx} x2={xx} y1={panelInnerPad} y2={panelInnerPad + plotH} className="stroke-[#f3f4f6] dark:stroke-slate-700" strokeWidth={1} />;
                          })}
                          {/* Y axis label (inside panel, vertical) */}
                            {(() => {
                              const yLabel = panelInnerPad + plotH / 2;
                              return (
                                <text x={pad.left - 46} y={yLabel} transform={`rotate(-90 ${pad.left - 46} ${yLabel})`} textAnchor="middle" className="text-xs fill-gray-600 dark:fill-gray-300">ΔRA</text>
                              );
                            })()}
                          {yTicks.map((yt, j) => {
                            const yy = panelInnerPad + plotH - ((yt - y0) / (y1 - y0)) * plotH;
                            return <text key={j} x={pad.left - 10} y={yy + 3} textAnchor="end" className="text-xs fill-gray-400 dark:fill-gray-300">{yt.toFixed(2)}</text>;
                          })}
                          {timeSeries.map((pt, idx) => {
                            const x = pad.left + ((pt.t - tMin) / (tMax - tMin)) * plotW;
                            const y = panelInnerPad + plotH - ((values[idx] - y0) / (y1 - y0)) * plotH;
                            const bandKey = String(pt.band ?? 'default').toLowerCase();
                            const isActive = !hoveredBand || hoveredBand === bandKey;
                            return <circle key={idx} cx={x} cy={y} r={isActive ? 3.5 : 2} fill={toColor(pt.band)} className={`stroke-white dark:stroke-slate-900`} opacity={isActive ? 1 : 0.12} strokeWidth={1} />;
                          })}
                        </g>
                      );
                    })()}

                    {/* ΔDec panel */}
                    {(() => {
                      const i = 1;
                      const panelTop = pad.top + i * (plotH + panelGap);
                      const values = ddecValues;
                      const minY = Math.min(...values);
                      const maxY = Math.max(...values);
                      const padY = (maxY - minY) * 0.12 || Math.max(0.5, Math.abs(maxY) * 0.1);
                      const y0 = minY - padY;
                      const y1 = maxY + padY;
                      const yTicks = makeTicks(y0, y1, 4);
                      return (
                        <g transform={`translate(0, ${panelTop})`}>
                          {yTicks.map((yt, j) => {
                            const yy = panelInnerPad + plotH - ((yt - y0) / (y1 - y0)) * plotH;
                            return <line key={j} x1={pad.left} x2={w - pad.right} y1={yy} y2={yy} className="stroke-[#eef2f6] dark:stroke-slate-700" strokeWidth={1} />;
                          })}
                          {timeTicks.map((tt, j) => {
                            const xx = pad.left + ((tt - tMin) / (tMax - tMin)) * plotW;
                            return <line key={`v2-${j}`} x1={xx} x2={xx} y1={panelInnerPad} y2={panelInnerPad + plotH} className="stroke-[#f3f4f6] dark:stroke-slate-700" strokeWidth={1} />;
                          })}
                          {(() => {
                            const yLabel = panelInnerPad + plotH / 2;
                            return (
                              <text x={pad.left - 46} y={yLabel} transform={`rotate(-90 ${pad.left - 46} ${yLabel})`} textAnchor="middle" className="text-xs fill-gray-600 dark:fill-gray-300">ΔDec</text>
                            );
                          })()}
                          {yTicks.map((yt, j) => {
                            const yy = panelInnerPad + plotH - ((yt - y0) / (y1 - y0)) * plotH;
                            return <text key={j} x={pad.left - 10} y={yy + 3} textAnchor="end" className="text-xs fill-gray-400 dark:fill-gray-300">{yt.toFixed(2)}</text>;
                          })}
                          {timeSeries.map((pt, idx) => {
                            const x = pad.left + ((pt.t - tMin) / (tMax - tMin)) * plotW;
                            const y = panelInnerPad + plotH - ((values[idx] - y0) / (y1 - y0)) * plotH;
                            const bandKey = String(pt.band ?? 'default').toLowerCase();
                            const isActive = !hoveredBand || hoveredBand === bandKey;
                            return <circle key={idx} cx={x} cy={y + 0} r={isActive ? 3.5 : 2} fill={toColor(pt.band)} className={`stroke-white dark:stroke-slate-900`} opacity={isActive ? 1 : 0.12} strokeWidth={1} />;
                          })}
                        </g>
                      );
                    })()}

                    {/* Separation panel */}
                    {(() => {
                      const i = 2;
                      const panelTop = pad.top + i * (plotH + panelGap);
                      const values = sepValues;
                      const minY = Math.min(...values);
                      const maxY = Math.max(...values);
                      const padY = (maxY - minY) * 0.12 || Math.max(0.5, Math.abs(maxY) * 0.1);
                      const y0 = minY - padY;
                      const y1 = maxY + padY;
                      const yTicks = makeTicks(y0, y1, 4);
                      return (
                        <g transform={`translate(0, ${panelTop})`}>
                          {yTicks.map((yt, j) => {
                            const yy = panelInnerPad + plotH - ((yt - y0) / (y1 - y0)) * plotH;
                            return <line key={j} x1={pad.left} x2={w - pad.right} y1={yy} y2={yy} className="stroke-[#eef2f6] dark:stroke-slate-700" strokeWidth={1} />;
                          })}
                          {timeTicks.map((tt, j) => {
                            const xx = pad.left + ((tt - tMin) / (tMax - tMin)) * plotW;
                            return <line key={`v3-${j}`} x1={xx} x2={xx} y1={panelInnerPad} y2={panelInnerPad + plotH} className="stroke-[#f3f4f6] dark:stroke-slate-700" strokeWidth={1} />;
                          })}
                          {(() => {
                            const yLabel = panelInnerPad + plotH / 2;
                            return (
                              <text x={pad.left - 46} y={yLabel} transform={`rotate(-90 ${pad.left - 46} ${yLabel})`} textAnchor="middle" className="text-xs fill-gray-600 dark:fill-gray-300">Separation</text>
                            );
                          })()}
                          {yTicks.map((yt, j) => {
                            const yy = panelInnerPad + plotH - ((yt - y0) / (y1 - y0)) * plotH;
                            return <text key={j} x={pad.left - 10} y={yy + 3} textAnchor="end" className="text-xs fill-gray-400 dark:fill-gray-300">{yt.toFixed(2)}</text>;
                          })}
                          {timeSeries.map((pt, idx) => {
                            const x = pad.left + ((pt.t - tMin) / (tMax - tMin)) * plotW;
                            const y = panelInnerPad + plotH - ((values[idx] - y0) / (y1 - y0)) * plotH;
                            const bandKey = String(pt.band ?? 'default').toLowerCase();
                            const isActive = !hoveredBand || hoveredBand === bandKey;
                            return <circle key={idx} cx={x} cy={y} r={isActive ? 3.5 : 2} fill={toColor(pt.band)} className={`stroke-white dark:stroke-slate-900`} opacity={isActive ? 1 : 0.12} strokeWidth={1} />;
                          })}
                        </g>
                      );
                    })()}

                    {/* X axis labels (MJD) */}
                    {timeTicks.map((pt, i) => {
                      const x = pad.left + ((pt - tMin) / (tMax - tMin)) * plotW;
                      return <text key={`xt-${i}`} x={x} y={h - 16} textAnchor="middle" className="text-xs fill-gray-400">{pt.toFixed(1)}</text>;
                    })}
                    {/* global X axis label */}
                    <text x={w / 2} y={h - 6} textAnchor="middle" className="text-sm fill-gray-600">MJD</text>
                  </svg>
                  </div>
                );
              })()
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
