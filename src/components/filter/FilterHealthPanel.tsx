/**
 * FilterHealthPanel — shows filter quality metrics after running a filter.
 * - Throughput: pass rate vs total alerts (mirrors Antoine's 5% threshold from PR #450)
 * - Classification breakdown: what fraction of results are SN-like, bogus, asteroids, etc.
 * - Magnitude stats: min/max/avg of candidate.magpsf
 */

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AlertTriangle, CheckCircle, XCircle, Activity, BarChart3, Sparkles, Clock } from "lucide-react";

interface FilterHealthPanelProps {
  matchedCount: number;
  totalCount: number | null;
  totalCountLoading: boolean;
  results: Record<string, unknown>[];
  queryTimeMs?: number | null;
}

// Antoine's threshold from PR #450
const GOOD_THRESHOLD = 5;
const WARN_THRESHOLD = 15;

function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split(".");
  let current: unknown = obj;
  for (const part of parts) {
    if (current === null || current === undefined || typeof current !== "object") return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

export function FilterHealthPanel({ matchedCount, totalCount, totalCountLoading, results, queryTimeMs }: FilterHealthPanelProps) {
  // ─── Throughput ─────────────────────────────────────────────────
  const passRate = totalCount && totalCount > 0
    ? (matchedCount / totalCount) * 100
    : null;

  const throughputStatus = totalCountLoading
    ? "loading"
    : totalCount === 0
      ? "empty"
      : passRate === null
        ? "unavailable"
        : matchedCount === 0
          ? "none"
          : passRate <= GOOD_THRESHOLD
            ? "good"
            : passRate <= WARN_THRESHOLD
              ? "warn"
              : "bad";

  // ─── Classification Breakdown ────────────────────────────────────
  const classChecks = [
    { path: "classifications.acai_h", label: "SN-like (acai_h > 0.5)", threshold: 0.5, above: true },
    { path: "classifications.btsbot", label: "BTS target (btsbot > 0.5)", threshold: 0.5, above: true },
    { path: "classifications.acai_b", label: "Bogus (acai_b > 0.5)", threshold: 0.5, above: true },
    { path: "properties.rock", label: "Asteroid (rock)", threshold: true, above: true },
    { path: "properties.star", label: "Stellar (star)", threshold: true, above: true },
  ];

  const classBreakdown = results.length > 0
    ? classChecks.map(({ path, label, threshold }) => {
        let count = 0;
        for (const r of results) {
          const val = getNestedValue(r, path);
          if (typeof threshold === "boolean") {
            if (val === true) count++;
          } else {
            if (typeof val === "number" && val > threshold) count++;
          }
        }
        return { label, count, total: results.length, pct: (count / results.length) * 100 };
      })
    : [];

  // ─── Magnitude Stats ─────────────────────────────────────────────
  const mags: number[] = [];
  for (const r of results) {
    const val = getNestedValue(r, "candidate.magpsf");
    if (typeof val === "number" && !isNaN(val)) mags.push(val);
  }
  const magStats = mags.length > 0
    ? {
        min: Math.min(...mags),
        max: Math.max(...mags),
        avg: mags.reduce((a, b) => a + b, 0) / mags.length,
      }
    : null;

  return (
    <Card className="border-border/50">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center gap-2">
          <Activity className="h-4 w-4" />
          Filter Health
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* ─── Throughput ─────────────────────────────────── */}
        <div>
          <div className="flex items-center gap-2 mb-1.5">
            {throughputStatus === "good" && <CheckCircle className="h-4 w-4 text-emerald-400" />}
            {throughputStatus === "warn" && <AlertTriangle className="h-4 w-4 text-amber-400" />}
            {throughputStatus === "bad" && <XCircle className="h-4 w-4 text-red-400" />}
            {throughputStatus === "none" && <AlertTriangle className="h-4 w-4 text-amber-400" />}
            {throughputStatus === "loading" && (
              <div className="h-4 w-4 rounded-full border-2 border-muted-foreground/30 border-t-primary animate-spin" />
            )}
            {throughputStatus === "unavailable" && (
              <div className="h-4 w-4 rounded-full border-2 border-muted-foreground/30" />
            )}
            {throughputStatus === "empty" && (
              <div className="h-4 w-4 rounded-full border-2 border-muted-foreground/30" />
            )}
            <span className="text-xs font-medium">Throughput</span>
          </div>

          {passRate !== null ? (
            <>
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <span>
                  {matchedCount.toLocaleString()} / {totalCount!.toLocaleString()} alerts
                </span>
                <span className="font-mono font-semibold">
                  → {passRate.toFixed(1)}%
                </span>
              </div>
              {throughputStatus !== "none" && (
                <div className="w-full bg-secondary rounded-full h-2 mt-1.5 overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all duration-500 ${
                      throughputStatus === "good"
                        ? "bg-emerald-500"
                        : throughputStatus === "warn"
                          ? "bg-amber-500"
                          : "bg-red-500"
                    }`}
                    style={{ width: `${Math.min(passRate, 100)}%` }}
                  />
                </div>
              )}
              <p className="text-[10px] text-muted-foreground mt-1">
                {throughputStatus === "none"
                  ? "No matches. The filter may be too restrictive, or no alerts in this window pass its conditions."
                  : throughputStatus === "good"
                    ? `Under ${GOOD_THRESHOLD}% — filter is selective enough for activation.`
                    : throughputStatus === "warn"
                      ? `Between ${GOOD_THRESHOLD}–${WARN_THRESHOLD}% — consider adding more conditions.`
                      : `Over ${WARN_THRESHOLD}% — filter is too broad. It would be rejected on activation.`}
              </p>
            </>
          ) : throughputStatus === "loading" ? (
            <p className="text-xs text-muted-foreground">Calculating total alert count...</p>
          ) : throughputStatus === "empty" ? (
            <p className="text-xs text-muted-foreground">
              No alerts present to filter from in the given JD timeframe.
            </p>
          ) : (
            <p className="text-xs text-muted-foreground">
              Total alert count unavailable.
            </p>
          )}
        </div>

        {/* ─── Classification Breakdown ──────────────────── */}
        {classBreakdown.length > 0 && (
          <div>
            <div className="flex items-center gap-2 mb-1.5">
              <BarChart3 className="h-4 w-4 text-muted-foreground" />
              <span className="text-xs font-medium">
                Classification Breakdown
                <span className="text-muted-foreground ml-1">({results.length} results)</span>
              </span>
            </div>
            <div className="space-y-1">
              {classBreakdown.map(({ label, count, pct }) => (
                <div key={label} className="flex items-center gap-2 text-xs">
                  <div className="w-28 truncate text-muted-foreground" title={label}>
                    {label}
                  </div>
                  <div className="flex-1 bg-secondary rounded-full h-1.5 overflow-hidden">
                    <div
                      className={`h-full rounded-full transition-all duration-500 ${
                        label.includes("Bogus") || label.includes("Asteroid") || label.includes("Stellar")
                          ? pct > 20 ? "bg-amber-500" : "bg-zinc-500"
                          : pct > 50 ? "bg-emerald-500" : "bg-blue-500"
                      }`}
                      style={{ width: `${Math.min(pct, 100)}%` }}
                    />
                  </div>
                  <span className="w-16 text-right font-mono text-muted-foreground">
                    {count}/{results.length} ({pct.toFixed(0)}%)
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ─── Magnitude Stats ───────────────────────────── */}
        {magStats && (
          <div>
            <div className="flex items-center gap-2 mb-1">
              <Sparkles className="h-4 w-4 text-muted-foreground" />
              <span className="text-xs font-medium">Brightness</span>
            </div>
            <div className="flex gap-4 text-xs text-muted-foreground">
              <span>
                avg: <span className="font-mono text-foreground">{magStats.avg.toFixed(1)}</span> mag
              </span>
              <span>
                range: <span className="font-mono text-foreground">{magStats.min.toFixed(1)}</span>
                {" — "}
                <span className="font-mono text-foreground">{magStats.max.toFixed(1)}</span>
              </span>
            </div>
          </div>
        )}

        {results.length === 0 && passRate === null && (
          <p className="text-xs text-muted-foreground text-center py-2">
            Run a filter to see health metrics.
          </p>
        )}

        {/* ─── Query Timing ────────────────────────────── */}
        {queryTimeMs != null && (
          <div className="flex items-center gap-2 text-xs text-muted-foreground pt-1 border-t border-border/50">
            <Clock className="h-3.5 w-3.5" />
            <span>
              Query took{" "}
              <span className={`font-mono font-semibold ${
                queryTimeMs < 2000
                  ? "text-emerald-400"
                  : queryTimeMs < 5000
                    ? "text-amber-400"
                    : "text-red-400"
              }`}>
                {(queryTimeMs / 1000).toFixed(1)}s
              </span>
            </span>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
