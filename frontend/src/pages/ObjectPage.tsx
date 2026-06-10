import { Suspense, lazy, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import api, { ApiObject } from "@/lib/api";
import useAppStore from "@/lib/store";
import { greatCircleDistance } from "@/lib/utils";

const SectionCards = lazy(async () => {
  const mod = await import("@/components/section-cards");
  return { default: mod.SectionCards };
});

export default function ObjectPage() {
  const params = useParams();
  const survey = params.survey as string | undefined;
  const objectId = params.objectId as string | undefined;

  const [data, setData] = useState<ApiObject | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!survey || !objectId) return;
    let mounted = true;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const obj = await api.fetchObject(survey as string, objectId as string);
        if (!mounted) return;

        // Format cross_matches: compute separation from source and strip coordinates
        function formatCrossMatches(data: ApiObject | null) {
          const cross = data ? (data['cross_matches'] as Record<string, unknown> | undefined) : undefined;
          if (!cross) return undefined;
          // determine source coordinates (try candidate, then top-level keys)
          // determine source coordinates (try candidate, then top-level keys)
          const candidate = data && (data['candidate'] as Record<string, unknown> | undefined);
          const srcRa = candidate?.['ra'] ?? data?.['ra'] ?? data?.['ra_deg'] ?? data?.['RA'] ?? null;
          const srcDec = candidate?.['dec'] ?? data?.['dec'] ?? data?.['dec_deg'] ?? data?.['DEC'] ?? null;

          const formatted: Record<string, Record<string, unknown>[]> = {};
          for (const [cat, matches] of Object.entries(cross)) {
            if (!Array.isArray(matches)) {
              formatted[cat] = [];
              continue;
            }
            formatted[cat] = matches.map((m) => {
              const matchObj = (m ?? {}) as Record<string, unknown>;
              const coordinates = matchObj['coordinates'];
              const rest: Record<string, unknown> = {};
              for (const [k, v] of Object.entries(matchObj)) {
                if (k === 'coordinates') continue;
                rest[k] = v;
              }

              // robustly find RA/Dec in the match
              let mRa: number | null = null;
              let mDec: number | null = null;
              const tryNum = (v: unknown) => (typeof v === 'number' ? v : (typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN));

              if (rest['ra'] !== undefined && rest['dec'] !== undefined) {
                mRa = tryNum(rest['ra']);
                mDec = tryNum(rest['dec']);
              } else if (rest['RA'] !== undefined && rest['DEC'] !== undefined) {
                mRa = tryNum(rest['RA']);
                mDec = tryNum(rest['DEC']);
              } else if (coordinates !== undefined) {
                if (Array.isArray(coordinates) && coordinates.length >= 2) {
                  mRa = tryNum(coordinates[0]);
                  mDec = tryNum(coordinates[1]);
                } else if (typeof coordinates === 'object' && coordinates !== null && 'ra' in (coordinates as Record<string, unknown>) && 'dec' in (coordinates as Record<string, unknown>)) {
                  const c = coordinates as Record<string, unknown>;
                  mRa = tryNum(c['ra']);
                  mDec = tryNum(c['dec']);
                }
              } else {
                const altRaKeys = ['ra_deg','radeg','RA','Ra','lon','lonn'];
                const altDecKeys = ['dec_deg','decdeg','DEC','Dec','lat','latt'];
                for (const k of altRaKeys) if (rest[k] !== undefined && mRa === null) mRa = tryNum(rest[k]);
                for (const k of altDecKeys) if (rest[k] !== undefined && mDec === null) mDec = tryNum(rest[k]);
              }

              let separation_arcsec: number | null = null;
              if (srcRa != null && srcDec != null && mRa != null && mDec != null && Number.isFinite(mRa) && Number.isFinite(mDec)) {
                try {
                  separation_arcsec = greatCircleDistance(Number(srcRa), Number(srcDec), Number(mRa), Number(mDec), 'arcsec');
                } catch (e) {
                  console.warn("Error computing great-circle distance:", e);
                  separation_arcsec = null;
                }
              }

              return {
                ...rest,
                separation_arcsec,
              };
            });
          }
          return formatted;
        }

        const formattedCrossMatches = formatCrossMatches(obj);
        const storedObj = { ...obj, cross_matches: formattedCrossMatches };

        setData(storedObj);
        useAppStore.getState().setCurrentSource({ survey, objectId, data: storedObj });
      } catch (err: unknown) {
        if (!mounted) return;
        const msg = err && typeof err === 'object' && 'message' in err ? String((err as { message?: unknown }).message) : String(err);
        setError(msg);
      } finally {
        if (mounted) setLoading(false);
      }
    }
    load();
    return () => { mounted = false; useAppStore.getState().clearCurrentSource(); };
  }, [survey, objectId]);

  return (
    <div className="px-2">
      {loading && <div className="mb-4">Loading object {survey}/{objectId}…</div>}
      {error && <div className="text-red-600 mb-4">{error}</div>}
      {!loading && !error && data && (
        <Suspense fallback={<div className="p-6">Loading components…</div>}>
          <SectionCards data={data} />
        </Suspense>
      )}
      {!loading && !error && !data && (
        <div className="p-6 rounded-lg border bg-card/50">No data available for {survey}/{objectId}.</div>
      )}
    </div>
  );
}
