import { Badge } from "@/components/ui/badge";
import type { AppleCiderFusion } from "@/lib/api";
import { formatLabel, formatPercent, rankedClasses } from "@/lib/applecider";

/** Classes shown before the rest are folded away. */
const TOP_CLASSES = 3;

function ProbabilityBar({ name, p }: { name: string; p: number }) {
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-36 truncate">{name}</span>
      <div aria-hidden className="bg-muted h-1.5 w-20 overflow-hidden rounded-full">
        <div className="bg-primary h-full" style={{ width: `${Math.min(Math.max(p, 0), 1) * 100}%` }} />
      </div>
      <span className="w-9 text-right tabular-nums text-muted-foreground">{formatPercent(p)}</span>
    </div>
  );
}

/** AppleCiDEr's decision: the label, its probability, and how coarse it is. */
export function Decision({ fusion }: { fusion: AppleCiderFusion | null | undefined }) {
  if (!fusion) return <span className="text-sm text-muted-foreground">Not classified</span>;
  if (fusion.abstain_completely || !fusion.decision_label) {
    return <Badge variant="outline">Abstained</Badge>;
  }
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <Badge>{formatLabel(fusion.decision_label)}</Badge>
      {fusion.decision_probability !== undefined && (
        <span className="text-sm tabular-nums">{formatPercent(fusion.decision_probability)}</span>
      )}
      {/* A family- or domain-level label is coarser than a class; say so. */}
      {fusion.decision_level && fusion.decision_level !== "class" && (
        <span className="text-xs text-muted-foreground">({fusion.decision_level})</span>
      )}
    </div>
  );
}

/** The most likely classes, with the rest behind a disclosure. */
export function ClassProbabilities({ fusion }: { fusion: AppleCiderFusion | null | undefined }) {
  const ranked = fusion ? rankedClasses(fusion.class_probabilities) : [];
  if (ranked.length === 0) return <span className="text-sm text-muted-foreground">-</span>;

  const rest = ranked.slice(TOP_CLASSES);
  return (
    <div className="flex flex-col gap-1">
      {ranked.slice(0, TOP_CLASSES).map(([name, p]) => (
        <ProbabilityBar key={name} name={name} p={p} />
      ))}
      {rest.length > 0 && (
        <details>
          <summary className="cursor-pointer text-xs text-muted-foreground">{rest.length} more</summary>
          <div className="mt-1 flex flex-col gap-1">
            {rest.map(([name, p]) => (
              <ProbabilityBar key={name} name={name} p={p} />
            ))}
          </div>
        </details>
      )}
    </div>
  );
}
