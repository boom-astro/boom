import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

// Constants and types
const { PI } = Math;
const rad = PI / 180;

export const ZTF = "ztf";
export const LSST = "lsst";

export const SURVEYS = [LSST, ZTF] as const;
export type Survey = (typeof SURVEYS)[number];

export const KAFKA_TOPICS = [
  "babamul.ztf.no-lsst-match.stellar",
  "babamul.ztf.lsst-match.stellar",
  "babamul.ztf.no-lsst-match.hosted",
  "babamul.ztf.lsst-match.hosted",
  "babamul.ztf.no-lsst-match.hostless",
  "babamul.ztf.lsst-match.hostless",
  "babamul.lsst.no-ztf-match.stellar",
  "babamul.lsst.ztf-match.stellar",
  "babamul.lsst.no-ztf-match.hosted",
  "babamul.lsst.ztf-match.hosted",
  "babamul.lsst.no-ztf-match.hostless",
  "babamul.lsst.ztf-match.hostless",
  "babamul.lsst.no-ztf-match.unknown",
  "babamul.lsst.ztf-match.unknown",
] as string[];

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function greatCircleDistance(
  ra1_deg: number,
  dec1_deg: number,
  ra2_deg: number,
  dec2_deg: number,
  unit: "arcsec" | "deg" | "arcmin" | "rad" = "arcsec",
): number {
  const ra1 = ra1_deg * rad;
  const dec1 = dec1_deg * rad;
  const ra2 = ra2_deg * rad;
  const dec2 = dec2_deg * rad;

  const delta_ra = Math.abs(ra2 - ra1);
  const distance = Math.atan2(
    Math.sqrt(
      (Math.cos(dec2) * Math.sin(delta_ra)) ** 2 +
        (Math.cos(dec1) * Math.sin(dec2) -
          Math.sin(dec1) * Math.cos(dec2) * Math.cos(delta_ra)) **
          2,
    ),
    Math.sin(dec1) * Math.sin(dec2) +
      Math.cos(dec1) * Math.cos(dec2) * Math.cos(delta_ra),
  );

  switch (unit) {
    case "arcsec":
      return (distance * 180.0 * 3600) / Math.PI;
    case "deg":
      return (distance * 180.0) / Math.PI;
    case "arcmin":
      return (distance * 180.0 * 60) / Math.PI;
    case "rad":
      return distance;
    default:
      return (distance * 180.0) / Math.PI;
  }
}

// pub const DEGRA: f64 = std::f64::consts::PI / 180.0;

const RGE = [
    [-0.054875539, -0.873437105, -0.483834992],
    [0.494109454, -0.444829594, 0.746982249],
    [-0.867666136, -0.198076390, 0.455983795],
];

// based on the rust code above, implement the function in typescript, to convert ra/dec to galactic l/b
export function radec2lb(ra_deg: number, dec_deg: number): [number, number] {
  const ra_rad = ra_deg * rad;
  const dec_rad = dec_deg * rad;

  const u = [
    Math.cos(ra_rad) * Math.cos(dec_rad),
    Math.sin(ra_rad) * Math.cos(dec_rad),
    Math.sin(dec_rad),
  ];

  const ug = [
    RGE[0][0] * u[0] + RGE[0][1] * u[1] + RGE[0][2] * u[2],
    RGE[1][0] * u[0] + RGE[1][1] * u[1] + RGE[1][2] * u[2],
    RGE[2][0] * u[0] + RGE[2][1] * u[1] + RGE[2][2] * u[2],
  ];

  const x = ug[0];
  const y = ug[1];
  const z = ug[2];

  const galactic_l = Math.atan2(y, x);
  const galactic_b = Math.atan2(z, Math.sqrt(x * x + y * y));

  return [galactic_l * (180.0 / PI), galactic_b * (180.0 / PI)];
}
