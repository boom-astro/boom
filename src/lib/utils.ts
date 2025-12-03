import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

const { PI } = Math;
const rad = PI / 180;

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