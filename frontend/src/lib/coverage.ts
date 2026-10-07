import {
  DESI_DR1_FOOTPRINT,
  GALEX_FOOTPRINT,
  LEGACY_SURVEYS_FOOTPRINT,
  LSDR10_FOOTPRINT,
  MILLIQUAS_FOOTPRINT,
  NED_FOOTPRINT,
} from "@/lib/footprints";

const EDGE_SOFTNESS = 1.5;

export const CATALOG_COLORS = ["oklch(0.78 0.13 215)", "oklch(0.84 0.16 85)"];

export type Footprint = {
  decMin?: number;
  decMax?: number;
  raster?: string;
};

export type Coverage = {
  id: string;
  color: string;
  origin: [number, number] | null;
  footprint: Footprint;
};

export type Catalog = {
  id: string;
  name: string;
  description: string;
  extent: string;
  footprint: Footprint;
};

export const CATALOGS: Catalog[] = [
  { id: "Gaia_DR3", name: "Gaia DR3", description: "Astrometry and photometry", extent: "All sky", footprint: {} },
  {
    id: "PS1_DR2",
    name: "PS1 DR2",
    description: "Pan-STARRS1 3π optical survey",
    extent: "δ > −30°",
    footprint: { decMin: -30 },
  },
  {
    id: "LSDR10",
    name: "Legacy Surveys DR10",
    description: "DESI Legacy Imaging Surveys",
    extent: "DECam area only, up to δ ≈ +35°",
    footprint: { raster: LSDR10_FOOTPRINT },
  },
  {
    id: "LSPSC",
    name: "LS PSC",
    description: "Legacy Surveys point-source catalog",
    extent: "Legacy Surveys footprint",
    footprint: { raster: LEGACY_SURVEYS_FOOTPRINT },
  },
  {
    id: "DESI_DR1",
    name: "DESI DR1",
    description: "DESI spectroscopic redshifts",
    extent: "About 15,500 deg², none south of δ −30°",
    footprint: { raster: DESI_DR1_FOOTPRINT },
  },
  { id: "2MASS_PSC", name: "2MASS PSC", description: "Near-infrared point sources", extent: "All sky", footprint: {} },
  {
    id: "CatWISE2020",
    name: "CatWISE2020",
    description: "Mid-infrared sources and proper motions",
    extent: "All sky",
    footprint: {},
  },
  {
    id: "GALEX",
    name: "GALEX",
    description: "Ultraviolet sources",
    extent: "Mostly away from the Galactic plane",
    footprint: { raster: GALEX_FOOTPRINT },
  },
  {
    id: "NED",
    name: "NED-LVS",
    description: "Galaxies with distances from NED",
    extent: "All sky except parts of the Galactic plane",
    footprint: { raster: NED_FOOTPRINT },
  },
  {
    id: "milliquas_v8",
    name: "Milliquas v8",
    description: "Quasars and AGN",
    extent: "Patchy, sparse near the Galactic plane",
    footprint: { raster: MILLIQUAS_FOOTPRINT },
  },
  { id: "VSX", name: "VSX", description: "Known variable stars (AAVSO)", extent: "All sky", footprint: {} },
  { id: "TNS", name: "TNS", description: "Reported transients", extent: "All sky", footprint: {} },
];

const rasters = new Map<string, Uint8Array>();

function rasterCoverage(raster: string, ra: number, dec: number): number {
  let grid = rasters.get(raster);
  if (!grid) {
    const cells = new Uint8Array(360 * 180);
    raster.split(";").forEach((row, j) => {
      for (const run of row ? row.split(",") : []) {
        const [start, end] = run.split("-").map(Number);
        cells.fill(1, j * 360 + start, j * 360 + end);
      }
    });
    rasters.set(raster, cells);
    grid = cells;
  }
  const x = ((ra % 360) + 360) % 360 - 0.5;
  const y = Math.min(179, Math.max(0, dec + 89.5));
  const i = Math.floor(x);
  const j = Math.min(178, Math.floor(y));
  const fx = x - i;
  const fy = y - j;
  const at = (column: number, row: number) => grid[row * 360 + ((column + 360) % 360)];
  const value =
    (at(i, j) * (1 - fx) + at(i + 1, j) * fx) * (1 - fy) +
    (at(i, j + 1) * (1 - fx) + at(i + 1, j + 1) * fx) * fy;
  return Math.min(1, Math.max(0, (value - 0.25) * 2));
}

function above(value: number, limit: number): number {
  return Math.min(1, Math.max(0, (value - limit) / EDGE_SOFTNESS + 0.5));
}

export function followsSiderealTime(footprint: Footprint): boolean {
  return footprint.raster !== undefined;
}

export function footprintCoverage(footprint: Footprint, ra: number, dec: number): number {
  let coverage = 1;
  if (footprint.decMin !== undefined) coverage *= above(dec, footprint.decMin);
  if (footprint.decMax !== undefined) coverage *= above(footprint.decMax, dec);
  if (footprint.raster !== undefined && coverage > 0) coverage *= rasterCoverage(footprint.raster, ra, dec);
  return coverage;
}
