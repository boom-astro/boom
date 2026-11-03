/** Class probabilities as `[name, probability]`, most likely first. */
export function rankedClasses(probs: Record<string, number>): [string, number][] {
  return Object.entries(probs)
    .filter(([, p]) => Number.isFinite(p))
    .sort(([, a], [, b]) => b - a);
}

/**
 * A decision label for display. Family and domain names are stored in
 * PascalCase ("StellarVariable"); leaf class names already have spaces.
 */
export function formatLabel(label: string): string {
  return label.replace(/([a-z])([A-Z])/g, "$1 $2");
}

/** A probability as a whole percent; "<1%" so a nonzero value never reads as 0. */
export function formatPercent(p: number): string {
  if (p > 0 && p < 0.005) return "<1%";
  return `${Math.round(p * 100)}%`;
}
