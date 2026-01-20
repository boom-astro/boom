/**
 * Minimal type declarations for Aladin Lite used via global `window.A`.
 * We use `unknown` for external API shapes to avoid `any` and still satisfy
 * TypeScript + ESLint rules. These can be refined later if desired.
 */
interface AladinInstance {
  addCatalog(catalog: AladinCatalog | unknown): void;
  addOverlay(overlay: unknown): void;
  gotoRaDec(ra: number, dec: number): void;
  setFov(fov: number): void;
}

interface AladinCatalog {
  addSources(sources: unknown[]): void;
}

interface AladinAPI {
  init: Promise<void>;
  aladin(selector: string, opts: Record<string, unknown>): AladinInstance;
  catalogHiPS(url: string, opts?: Record<string, unknown>): AladinCatalog;
  catalog(opts?: Record<string, unknown>): AladinCatalog;
  source(ra: number, dec: number, data?: Record<string, unknown>): unknown;
  graphicOverlay(opts?: Record<string, unknown>): { add: (item: unknown) => void; clear?: () => void };
  circle(ra: number, dec: number, radius: number): unknown;
}

declare global {
  interface Window {
    A: AladinAPI;
  }
}

export {};
