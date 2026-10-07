import { LEGACY_SURVEYS_FOOTPRINT } from "@/lib/legacySurveysFootprint";

const DEG = Math.PI / 180;
const NGP_RA = 192.85948;
const NGP_DEC = 27.12825;
const EDGE_SOFTNESS = 1.5;

export const CATALOG_COLOR = "oklch(0.78 0.13 215)";

export type Footprint = {
  decMin?: number;
  decMax?: number;
  minGalacticLatitude?: number;
  legacySurveys?: boolean;
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
    extent: "Legacy Surveys footprint",
    footprint: { legacySurveys: true },
  },
  {
    id: "LSPSC",
    name: "LS PSC",
    description: "Legacy Surveys point-source catalog",
    extent: "Legacy Surveys footprint",
    footprint: { legacySurveys: true },
  },
  {
    id: "DESI_DR1",
    name: "DESI DR1",
    description: "DESI spectroscopic redshifts",
    extent: "Legacy Surveys above δ −20° (approx.)",
    footprint: { legacySurveys: true, decMin: -20 },
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
    extent: "|b| > 15° (approx.)",
    footprint: { minGalacticLatitude: 15 },
  },
  { id: "NED", name: "NED-LVS", description: "Galaxies with distances from NED", extent: "All sky", footprint: {} },
  { id: "milliquas_v8", name: "Milliquas v8", description: "Quasars and AGN", extent: "All sky", footprint: {} },
  { id: "VSX", name: "VSX", description: "Known variable stars (AAVSO)", extent: "All sky", footprint: {} },
  { id: "TNS", name: "TNS", description: "Reported transients", extent: "All sky", footprint: {} },
];

let legacySurveysGrid: Uint8Array | null = null;

function legacySurveys(ra: number, dec: number): number {
  if (!legacySurveysGrid) {
    const grid = new Uint8Array(360 * 180);
    LEGACY_SURVEYS_FOOTPRINT.split(";").forEach((row, j) => {
      for (const run of row ? row.split(",") : []) {
        const [start, end] = run.split("-").map(Number);
        grid.fill(1, j * 360 + start, j * 360 + end);
      }
    });
    legacySurveysGrid = grid;
  }
  const grid = legacySurveysGrid;
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

function galacticLatitude(ra: number, dec: number): number {
  const sinB =
    Math.sin(dec * DEG) * Math.sin(NGP_DEC * DEG) +
    Math.cos(dec * DEG) * Math.cos(NGP_DEC * DEG) * Math.cos((ra - NGP_RA) * DEG);
  return Math.asin(Math.max(-1, Math.min(1, sinB))) / DEG;
}

export function followsSiderealTime(footprint: Footprint): boolean {
  return footprint.minGalacticLatitude !== undefined || !!footprint.legacySurveys;
}

export function footprintCoverage(footprint: Footprint, ra: number, dec: number): number {
  let coverage = 1;
  if (footprint.decMin !== undefined) coverage *= above(dec, footprint.decMin);
  if (footprint.decMax !== undefined) coverage *= above(footprint.decMax, dec);
  if (footprint.minGalacticLatitude !== undefined && coverage > 0) {
    coverage *= above(Math.abs(galacticLatitude(ra, dec)), footprint.minGalacticLatitude);
  }
  if (footprint.legacySurveys && coverage > 0) coverage *= legacySurveys(ra, dec);
  return coverage;
}
