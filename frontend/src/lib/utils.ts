import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

/**
 * shadcn/ui's `cn` helper, and nothing else.
 *
 * DON'T ADD TO THIS FILE. "Utils" names no context, so anything dropped here
 * ends up coupled to everything and described by nothing — which is how this
 * module previously accumulated spherical astronomy and Kafka topic names. New
 * code belongs in a module named for what it is about: `coordinates.ts`,
 * `constants.ts`, `imageProcessing.ts`.
 *
 * This one file stays because shadcn/ui's CLI hard-codes the path: it is the
 * `aliases.utils` default in `components.json`, and every component it
 * generates imports `cn` from `@/lib/utils`. Keeping the canonical location
 * means `npx shadcn add ...` just works.
 */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}
