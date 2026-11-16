import { useEffect, useState } from "react";
import api, { type LiveIngest } from "@/lib/api";

const REFRESH_MS = 60_000;
const MINUTE = 60_000;

export function formatAgo(ms: number): string {
  const minutes = Math.round(ms / MINUTE);
  if (minutes < 1) return "just now";
  if (minutes < 90) return `${minutes} min ago`;
  return `${(minutes / 60).toFixed(1)} h ago`;
}

export function formatNight(date: string): string {
  const evening = new Date(`${date}T00:00:00`);
  const morning = new Date(evening.getFullYear(), evening.getMonth(), evening.getDate() + 1);
  const day = (d: Date) => d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  return `${day(evening)} → ${morning.getMonth() === evening.getMonth() ? morning.getDate() : day(morning)}`;
}

export function useLiveIngest(enabled: boolean): { data: LiveIngest | null; error: string | null } {
  const [data, setData] = useState<LiveIngest | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!enabled) return;
    let cancelled = false;
    const load = () => {
      if (document.visibilityState !== "visible") return;
      api.fetchLiveIngest()
        .then((live) => {
          if (cancelled) return;
          setData(live);
          setError(null);
        })
        .catch((e) => {
          if (!cancelled) setError(e instanceof Error ? e.message : "Failed to load tonight's alerts");
        });
    };
    load();
    const id = setInterval(load, REFRESH_MS);
    document.addEventListener("visibilitychange", load);
    return () => {
      cancelled = true;
      clearInterval(id);
      document.removeEventListener("visibilitychange", load);
    };
  }, [enabled]);

  return { data, error };
}
