"use client"

import pako from 'pako';

import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
    CardFooter,
} from "@/components/ui/card"

import { toast } from "sonner"

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"

import dayjs from "dayjs";

import utc from "dayjs/plugin/utc";
import relativeTime from "dayjs/plugin/relativeTime";
import { useState, useMemo } from "react";
import { Badge } from "@/components/ui/badge"
import { Button } from './ui/button';
import { IconGalaxy, IconMeteor, IconSparkles, IconStar, IconStars } from '@tabler/icons-react';
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from "@/components/ui/dialog"


dayjs.extend(utc);
dayjs.extend(relativeTime);


const NAXIS1_BYTES = new TextEncoder().encode("NAXIS1  =");
const NAXIS2_BYTES = new TextEncoder().encode("NAXIS2  =");
const NAXIS1_BYTES_LEN = NAXIS1_BYTES.length;
const NAXIS2_BYTES_LEN = NAXIS2_BYTES.length;
const FITS_HEADER_LEN = 2880;
const NAXIS_STANDARD = 63;
const NB_PIXELS = NAXIS_STANDARD * NAXIS_STANDARD;
const SPACE_BYTES = new TextEncoder().encode(" ");
const SPACE_BYTE = SPACE_BYTES[0];

type Detection = {
  jd?: number;
  magpsf?: number;
  sigmapsf?: number;
  band?: string;
  diffmaglim?: number;
};

type CandidateData = {
  object_id?: string;
  candidate?: { ra?: number; dec?: number; drb?: number; ndethist?: number };
  classifications?: Record<string, number>;
  properties?: { star?: boolean };
  cutout_science?: Uint8Array | string | ArrayBuffer | undefined;
  cutout_template?: Uint8Array | string | ArrayBuffer | undefined;
  cutout_difference?: Uint8Array | string | ArrayBuffer | undefined;
  prv_candidates?: Detection[];
  prv_nondetections?: Detection[];
};

function bone(n: number) {
  // here is, according to plotly, the ranges for the bone color map:
  // bone:[{index:0,rgb:[0,0,0]},{index:.376,rgb:[84,84,116]},{index:.753,rgb:[169,200,200]},{index:1,rgb:[255,255,255]}]
  // we need to convert that to a lookup table of 256 colors
  // we do that by interpolating between the 4 points
  // we use the same method as plotly, which is a cubic spline interpolation

  // first we create the 4 points
  const points = [
    {index:0,rgb:[0,0,0]},
    {index:.376,rgb:[84,84,116]},
    {index:.753,rgb:[169,200,200]},
    {index:1,rgb:[255,255,255]}
  ]

  // then we create the lookup table
  const lookup = []
  for (let i = 0; i < n; i++) {
    const x = i / (n - 1)
    let j = 0
    while (points[j + 1].index < x) {
      j++
    }
    const x0 = points[j].index
    const x1 = points[j + 1].index
    const y0 = points[j].rgb
    const y1 = points[j + 1].rgb
    const t = (x - x0) / (x1 - x0)
    const y = [
      Math.round(y0[0] + t * (y1[0] - y0[0])),
      Math.round(y0[1] + t * (y1[1] - y0[1])),
      Math.round(y0[2] + t * (y1[2] - y0[2])),
      255
    ]
    lookup.push(y)
  }
  return lookup
}

const bone_cm = bone(256)

function isEqualArray(a: Uint8Array | number[], b: Uint8Array | number[]) {
  if (a.length != b.length) {
    return false
  };
  for (let i = 0; i < a.length; i++) {
    if (a[i] != b[i]) {
      return false;
    }
  }
  return true;
}

function bytesToFloats(data: Uint8Array): Float32Array {
  const floats = new Float32Array(data.length / 4);
  for (let i = 0; i < data.length; i += 4) {
    floats[i / 4] = new DataView(data.buffer, data.byteOffset + i, 4).getFloat32(0, false);
  }
  return floats;
}

function bytes2imgdata(bytes: Uint8Array | ArrayBuffer): Float32Array {
  // decompress the data
  const compressedCutoutArray = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const decompressedCutout: Uint8Array = pako.inflate(compressedCutoutArray);

  const subset = decompressedCutout.slice(0, FITS_HEADER_LEN);

  let naxis1_key_start = 0;
  let naxis1_val_start = 0;
  let naxis1_val_end = 0;

  for (let i = 0; i < FITS_HEADER_LEN - NAXIS1_BYTES_LEN; i++) {
    if (isEqualArray(subset.slice(i, i + NAXIS1_BYTES_LEN), NAXIS1_BYTES)) {
      naxis1_key_start = i;
      break;
    }
  }

  if (naxis1_key_start === 0) {
    console.error("NAXIS1 key not found in FITS header");
  }

  for (let i = naxis1_key_start + NAXIS1_BYTES_LEN; i < FITS_HEADER_LEN; i++) {
    if (subset[i] !== SPACE_BYTE) {
      naxis1_val_start = i;
      break;
    }
  }

  if (naxis1_val_start === 0) {
    console.error("NAXIS1 value start not found in FITS header");
  }

  for (let i = naxis1_val_start; i < FITS_HEADER_LEN; i++) {
    if (subset[i] === SPACE_BYTE) {
      naxis1_val_end = i;
      break;
    }
  }

  const naxis1_val = subset.slice(naxis1_val_start, naxis1_val_end);
  // convert that value to a number
  const naxis1_val_str = new TextDecoder().decode(naxis1_val);
  const naxis1 = parseInt(naxis1_val_str, 10);

  // same for NAXIS2, but we start at the naxis1_val_end
  let naxis2_key_start = naxis1_val_end;
  let naxis2_val_start = 0;
  let naxis2_val_end = 0;
  for (let i = naxis2_key_start; i < FITS_HEADER_LEN - NAXIS2_BYTES_LEN; i++) {
    if (isEqualArray(subset.slice(i, i + NAXIS2_BYTES_LEN), NAXIS2_BYTES)) {
      naxis2_key_start = i;
      break;
    }
  }
  if (naxis2_key_start === 0) {
    console.error("NAXIS2 key not found in FITS header");
  }
  for (let i = naxis2_key_start + NAXIS2_BYTES_LEN; i < FITS_HEADER_LEN; i++) {
    if (subset[i] !== SPACE_BYTE) {
      naxis2_val_start = i;
      break;
    }
  }
  if (naxis2_val_start === 0) {
    console.error("NAXIS2 value start not found in FITS header");
  }
  for (let i = naxis2_val_start; i < FITS_HEADER_LEN; i++) {
    if (subset[i] === SPACE_BYTE) {
      naxis2_val_end = i;
      break;
    }
  }

  const naxis2_val = subset.slice(naxis2_val_start, naxis2_val_end);
  const naxis2_val_str = new TextDecoder().decode(naxis2_val);
  const naxis2 = parseInt(naxis2_val_str, 10);


  // now we can get the subset of the array that contains the data
  // it starts at the end of the header, which is 2880 bytes
  // and ends at naxis1 * naxis2 * 4 bytes
  let data: Uint8Array | Float32Array = decompressedCutout.slice(FITS_HEADER_LEN, naxis1 * naxis2 * 4 + FITS_HEADER_LEN);
  // convert to a Float32Array
  if (data instanceof Uint8Array) {
    data = bytesToFloats(data);
  }

  // if NAXIS1 and NAXIS2 are not NAXIS_STANDARD, we need to pad the image with zeros
  // we can't just add zeros to the end of the image_data vector, because it's a 2D array we flattened into a 1D vector
  // so if NAXIS1 is not NAXIS_STANDARD, we need to add NAXIS_STANDARD - NAXIS1 zeros to the start and end of each row
  // and if NAXIS2 is not NAXIS_STANDARD, we need to add NAXIS_STANDARD - NAXIS2 zeros to the start and end of the vector

  const offset1 = Math.ceil((NAXIS_STANDARD - naxis1) / 2.0);
  const offset2 = Math.ceil((NAXIS_STANDARD - naxis2) / 2.0);

  if (offset1 !== 0 || offset2 !== 0) {
    const new_image_data = new Float32Array(NB_PIXELS);
    // data is expected to be Float32Array here
    const fdata = data as Float32Array;
    for (let i = 0; i < naxis2; i++) {
      for (let j = 0; j < naxis1; j++) {
        const k = i * naxis1 + j;
        const k_new = (i + offset2) * NAXIS_STANDARD + (j + offset1);
        new_image_data[k_new] = fdata[k];
      }
    }
    data = new_image_data;
  }

  return data as Float32Array;
}

function cleanupImage(image: Float32Array | number[]): number[] {
  // first, replace all dubiously large values (absolute) with NaN (>1e20)
  const new_img = Array.from(image).map((value) => (Math.abs(value) > 1e20 ? NaN : value));
  // then find the median of the non-NaN values
  const filtered = new_img.filter((val) => !isNaN(val));
  const sorted = filtered.sort((a, b) => a - b);
  const median = sorted[Math.floor(filtered.length / 2)];
  // then replace all NaN values with the median
  return new_img.map((value) => (isNaN(value) ? median : value));
}

function normalizeImage(
  image: number[],
  method: "minmax" | "asymmetric_percentile" = "minmax",
  lower_percentile = 0.01,
  upper_percentile = 1
): number[] {
  if (method == "minmax") {
    const max = Math.max(...image);
    const min = Math.min(...image);
    return image.map((value) => (value - min) / (max - min));
  } else if (method == "asymmetric_percentile") {
    const sorted = [...image].sort((a, b) => a - b);
    const lower = lower_percentile > 0 ? sorted[Math.floor(lower_percentile * sorted.length)] : 0;
    const upper = upper_percentile < 1 ? sorted[Math.floor(upper_percentile * sorted.length)] : 1;
    const clipped = image.map((value) => Math.max(lower, Math.min(upper, value)));
    return clipped.map((value) => (value - lower) / (upper - lower));
  }
  throw new Error("Unknown normalization method");
}

function stretchImage(
  image: number[],
  cutoutType: string,
  method: "log" | "linear" | "asinh" | "sqrt" | null = null,
  alpha = 1000.0
): number[] {
  if (method === null) {
    method = cutoutType === "difference" ? "linear" : "log";
  }

  if (method === "linear") {
    return image;
  }
  if (method === "log") {
    return image.map((value) => Math.log(alpha * value + 1) / Math.log(alpha + 1));
  }
  if (method === "asinh") {
    return image.map((value) => Math.asinh(value));
  }
  if (method === "sqrt") {
    return image.map((value) => Math.sqrt(value));
  }
  throw new Error("Unknown stretch method");
}

function applyColorMap(image: number[], colorMap: "gray" | "bone" = "gray") {
  // we implement 2 color maps here
  // gray: gray scale
  // bone: bone color map

  const rgba_image: Array<[number, number, number, number]> = new Array(image.length);

  if (colorMap === "gray") {
    for (let i = 0; i < image.length; i++) {
      rgba_image[i] = [image[i], image[i], image[i], 255];
    }
  } else if (colorMap === "bone") {     
    // return image.map((value) => bone_cm[Math.floor(value)]);
    for (let i = 0; i < image.length; i++) {
      rgba_image[i] = bone_cm[image[i]] as [number, number, number, number];
    }
  } else {
    throw new Error("Invalid color map")
  }
  return rgba_image;
}

function bytes2image(bytes: Uint8Array | string | ArrayBuffer | undefined, type: string = "science"): string | null {

  // let data = bytes2imgdata(bytes);
  // bytes coulds be an array or a base64 encoded string
  let data: number[];
  if (typeof bytes === 'string') {
    // base64 string
    const binaryString = atob(bytes);
    const len = binaryString.length;
    const byteArray = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      byteArray[i] = binaryString.charCodeAt(i);
    }
    data = Array.from(bytes2imgdata(byteArray));
  } else {
    if (!bytes) return null;
    let buf: Uint8Array;
    if (bytes instanceof Uint8Array) buf = bytes;
    else if (bytes instanceof ArrayBuffer) buf = new Uint8Array(bytes);
    else return null;
    data = Array.from(bytes2imgdata(buf));
  }
  data = cleanupImage(data);
  
  // first we normalize with a minmax
  data = normalizeImage(data, "minmax");

  // then we stretch with a log
  data = stretchImage(data, type, "log");

  // then we normalize again with a asymmetric_percentile
  data = normalizeImage(data, "asymmetric_percentile");

  // now that we have a value between 0 and 1, convert to 0 to 255
  data = data.map(value => Math.round(value * 255));

  // then we apply the color map
  const colored = applyColorMap(data, "bone");

  if (typeof document !== 'undefined') {
    const canvas = document.createElement('canvas');
    canvas.width = NAXIS_STANDARD;
    canvas.height = NAXIS_STANDARD;
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    const imageData = ctx.createImageData(NAXIS_STANDARD, NAXIS_STANDARD);
    for (let i = 0; i < 63; i++) {
      for (let j = 0; j < 63; j++) {
        const pixelValue = colored[i * 63 + j];
        const pixelIndex = (i * 63 + j) * 4;
        imageData.data[pixelIndex] = pixelValue[0]; // R
        imageData.data[pixelIndex + 1] = pixelValue[1]; // G
        imageData.data[pixelIndex + 2] = pixelValue[2]; // B
        imageData.data[pixelIndex + 3] = pixelValue[3]; // A
      }
    }
    ctx.putImageData(imageData, 0, 0);
    return canvas.toDataURL();
  } else {
    return null;
  }
}

function mjd_to_utc(mjd: number) {
  // Take a MJD string and return UTC time
  return dayjs
    .unix((mjd - 40587) * 86400.0)
    .utc()
    .format();
}

function jd_to_mjd(jd: number) {
  return jd - 2400000.5;
}

export function ClassificationBadges({
  data,
}: {
  data: CandidateData;
}) {
  const classifications = data.classifications ?? {};
  const properties = data.properties ?? {};
  return (
    <div className="flex flex-row flex-wrap gap-2">
      {(data.candidate?.drb ?? 1) < 0.2 && (
        <Badge variant="outline" className="text-sm font-semibold">
          Bogus?
        </Badge>
      )}
      {(classifications['acai_h'] ?? 0) > 0.8 && (
        <Badge variant="outline" className="text-sm font-semibold">
          Hosted
        </Badge>
      )}
      {(classifications['acai_n'] ?? 0) > 0.8 && (
        <Badge variant="outline" className="text-sm font-semibold">
          Nuclear
        </Badge>
      )}
      {((classifications['btsbot'] ?? 0) > 0.8) || ((classifications['acai_h'] ?? 0) > 0.8 && !properties?.star && ((data.candidate?.ndethist ?? 0) < 200)) && (
        <Badge variant="outline" className="text-sm font-semibold">
          SN?
        </Badge>
      )}
      {properties?.star && (
        <Badge variant="outline" className="text-sm font-semibold">
          Stellar
        </Badge>
      )}
    </div>
  )
}

function formatDetection(det: Detection | null): string {
  if (!det) return "-";
  const mag = det.magpsf;
  const sig = det.sigmapsf;
  if (mag == null || sig == null) return "-";
  return `${mag.toFixed(2)}±${sig.toFixed(2)}`;
}

export default function Header({
    data,
  }: {
    data: CandidateData;
  }) {
    const [lightboxOpen, setLightboxOpen] = useState(false);
    const [band, setBand] = useState<string>("all");

    const objectId = data.object_id ?? "";
    const ra = data.candidate?.ra?.toFixed(6) ?? "-";
    const dec = data.candidate?.dec?.toFixed(6) ?? "-";

    const prvCandidates: Detection[] = data.prv_candidates ?? [];
    const prvNonDetections: Detection[] = data.prv_nondetections ?? [];

    const filteredCandidates = useMemo(() => {
      if (!band || band === "all") return prvCandidates;
      return prvCandidates.filter((c: Detection) => ((c.band ?? "")).toLowerCase() === band.toLowerCase());
    }, [band, prvCandidates]);

    const filteredNonDetections = useMemo(() => {
      if (!band || band === "all") return prvNonDetections;
      return prvNonDetections.filter((c: Detection) => ((c.band ?? "")).toLowerCase() === band.toLowerCase());
    }, [band, prvNonDetections]);

    const nb_detections = filteredCandidates.length;
    const nb_nondetections = filteredNonDetections.length;

    // let first_det = filteredCandidates.length ? filteredCandidates.reduce((a: any, b: any) => a.jd < b.jd ? a : b) : null;
    // let last_det = filteredCandidates.length ? filteredCandidates.reduce((a: any, b: any) => a.jd > b.jd ? a : b) : null;
    const first_det = useMemo(() => {
      if (filteredCandidates.length === 0) return null;
      return filteredCandidates.reduce((a: Detection, b: Detection) => ( (a.jd ?? Infinity) < (b.jd ?? Infinity) ? a : b));
    }, [filteredCandidates]);
    const last_det = useMemo(() => {
      if (filteredCandidates.length === 0) return null;
      return filteredCandidates.reduce((a: Detection, b: Detection) => ( (a.jd ?? -Infinity) > (b.jd ?? -Infinity) ? a : b));
    }, [filteredCandidates]);
    const peak_det = useMemo(() => {
      if (filteredCandidates.length === 0) return null;
      return filteredCandidates.reduce((a: Detection, b: Detection) => ((a.magpsf ?? Infinity) < (b.magpsf ?? Infinity) ? a : b));
    }, [filteredCandidates]);
    const age = (first_det && last_det && first_det.jd != null && last_det.jd != null) ? Math.round((last_det.jd - first_det.jd) * 100) / 100 : "-";

    const scienceImage = bytes2image(data.cutout_science, "science");
    const templateImage = bytes2image(data.cutout_template, "template");
    const differenceImage = bytes2image(data.cutout_difference, "difference");

    function openLightbox() {
      setLightboxOpen(true);
    }

    return (
      <Card className="@container/card col-span-2 row-span-2">
        <CardHeader>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">{objectId}</CardTitle>
          <CardDescription>
            <ClassificationBadges data={data} />
          </CardDescription>
        </CardHeader>
        <CardContent className="pb-0 flex flex-col gap-4">
            <div className="grid grid-cols-3 gap-4">
              <div key="science" className="w-full relative group">
                <button onClick={openLightbox} className="w-full h-full text-left">
                  <img src={scienceImage ?? undefined} alt="Science" className="w-full h-auto object-cover rounded" style={{ imageRendering: 'pixelated' }}/>
                </button>
                <span className="absolute top-2 left-2 bg-black/60 text-white text-sm px-3 py-1 rounded-md backdrop-blur-sm opacity-100 group-hover:opacity-0 transition-opacity duration-150 pointer-events-none">Science</span>
              </div>
              <div key="template" className="w-full relative group">
                <button onClick={openLightbox} className="w-full h-full text-left">
                  <img src={templateImage ?? undefined} alt="Reference" className="w-full h-auto object-cover rounded" style={{ imageRendering: 'pixelated' }}/>
                </button>
                <span className="absolute top-2 left-2 bg-black/60 text-white text-sm px-3 py-1 rounded-md backdrop-blur-sm opacity-100 group-hover:opacity-0 transition-opacity duration-150 pointer-events-none">Reference</span>
              </div>
              <div key="difference" className="w-full relative group">
                <button onClick={openLightbox} className="w-full h-full text-left">
                  <img src={differenceImage ?? undefined} alt="Difference" className="w-full h-auto object-cover rounded" style={{ imageRendering: 'pixelated' }}/>
                </button>
                <span className="absolute top-2 left-2 bg-black/60 text-white text-sm px-3 py-1 rounded-md backdrop-blur-sm opacity-100 group-hover:opacity-0 transition-opacity duration-150 pointer-events-none">Difference</span>
              </div>
            </div>
            <div className="rounded-lg shadow-sm w-full border">
            <table className="min-w-full rounded-lg">
              <thead>
                <tr>
                  <th className="py-3 px-4 text-left font-medium">
                    <Select value={band} onValueChange={(v) => setBand(v)}>
                      <SelectTrigger className="w-[180px]">
                        <SelectValue placeholder="Band(s)" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All bands</SelectItem>
                        <SelectItem value="r">R-band</SelectItem>
                        <SelectItem value="g">G-band</SelectItem>
                      </SelectContent>
                    </Select>
                  </th>
                  <th className="py-3 px-4 text-left font-medium">Age</th>
                  <th className="py-3 px-4 text-left font-medium">Nb Detections</th>
                  <th className="py-3 px-4 text-left font-medium">Nb Non Detections</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                <tr>
                  <td className="py-4 px-4 font-medium">{band === "all" ? "Showing all bands" : <span className="text-orange-500">Showing {band}-band only</span>}</td>
                  <td className="py-4 px-4 font-medium">{age} days</td>
                  <td className="py-4 px-4 font-medium">{nb_detections}</td>
                  <td className="py-4 px-4 font-medium">{nb_nondetections}</td>
                </tr>
              </tbody>
            </table>
          </div>

          <div className="rounded-lg shadow-sm w-full border">
            <table className="min-w-full rounded-lg">
              <thead>
                <tr>
                  <th className="py-3 px-4 text-left font-medium">Measurement</th>
                  <th className="py-3 px-4 text-left font-medium">Time (UTC)</th>
                  <th className="py-3 px-4 text-left font-medium">Magnitude</th>
                  {(!band || band === "all") && (
                    <th className="py-3 px-4 text-left font-medium">Band</th>
                  )}
                </tr>
              </thead>
              <tbody className="divide-y">
                <tr>
                  <td className="py-4 px-4 font-medium">First Detection</td>
                  <td className="py-4 px-4">{first_det?.jd ? mjd_to_utc(jd_to_mjd(first_det.jd)).replace("T", ' ').replace("Z", "") : "-"}</td>
                  <td className="py-4 px-4">{formatDetection(first_det)}</td>
                  {(!band || band === "all") && (
                    <td className="py-4 px-4">{first_det?.band || "-"}</td>
                  )}
                </tr>
                <tr>
                  <td className="py-4 px-4 font-medium">Peak Detection</td>
                  <td className="py-4 px-4">{peak_det?.jd ? mjd_to_utc(jd_to_mjd(peak_det.jd)).replace("T", ' ').replace("Z", "") : "-"}</td>
                  <td className="py-4 px-4">{formatDetection(peak_det)}</td>
                  {(!band || band === "all") && (
                    <td className="py-4 px-4">{peak_det?.band || "-"}</td>
                  )}
                </tr>
                <tr>
                  <td className="py-4 px-4 font-medium">Last Detection</td>
                  <td className="py-4 px-4">{last_det?.jd ? mjd_to_utc(jd_to_mjd(last_det.jd)).replace("T", ' ').replace("Z", "") : "-"}</td>
                  <td className="py-4 px-4">{formatDetection(last_det)}</td>
                  {(!band || band === "all") && (
                    <td className="py-4 px-4">{last_det?.band || "-"}</td>
                  )}
                </tr>
              </tbody>
            </table>
          </div>
        </CardContent>
        <CardFooter className="flex flex-row justify-between">
          {/* next have a grid of icon buttons that link to other websites*/}
          <div className="grid grid-cols-5 gap-4 w-full">
          <Button variant="outline" className="w-full" onClick={() => window.open(`http://simbad.u-strasbg.fr/simbad/sim-coo?Coord=${ra}%20${dec}&Radius=0.08`, "_blank")}>
              <IconSparkles /> Simbad
            </Button>
            <Button variant="outline" className="w-full" onClick={() => window.open(`https://www.wis-tns.org/search?ra=${ra}&decl=${dec}&radius=5&coords_unit=arcsec`, "_blank")}>
              <IconStar /> TNS
            </Button>
            <Button variant="outline" className="w-full" onClick={() => window.open(`https://www.legacysurvey.org/viewer?ra=${ra}&dec=${dec}&layer=ls-dr10&photoz-dr9&zoom=16&mark=${ra},${dec}`, "_blank")}>
              <IconStars /> Legacy Survey
            </Button>
            <Button variant="outline" className="w-full" onClick={() => window.open(`https://ned.ipac.caltech.edu/cgi-bin/objsearch?search_type=Near+Position+Search&in_csys=Equatorial&in_equinox=J2000.0&ra=${ra}&dec=${dec}&radius=1.0&obj_sort=Distance+to+search+center&img_stamp=Yes`, "_blank")}>
              <IconGalaxy /> NED
            </Button>
            <Button variant="outline" className="w-full"
              onClick={() =>
                toast("Not implemented yet", {
                  description: "Crossmatching against the MPC is not implemented yet",
                  action: {
                    label: "Undo",
                    onClick: () => console.log("Undo"),
                  },
                })
              }
            >
              <IconMeteor /> MPC
            </Button>
          </div>
        </CardFooter>
        <Dialog open={lightboxOpen} onOpenChange={(v) => setLightboxOpen(v)}>
          <DialogContent className="!fixed inset-0 m-0 p-0 bg-background !max-w-none w-full h-full rounded-none overflow-hidden !top-0 !left-0 !translate-x-0 !translate-y-0 !border-none !shadow-none">
            <DialogTitle className="sr-only">Cutouts</DialogTitle>
            <div className="w-full h-full flex items-center justify-center">
              <div className="w-[95%] h-[95%] bg-background rounded-md p-6 overflow-auto">
                <div className="flex flex-col md:flex-row gap-4 h-full">
                  <div className="flex-1 flex items-center justify-center min-h-0">
                    {scienceImage ? (
                      <img src={scienceImage} alt="Science large" className="w-full h-full object-contain" style={{ imageRendering: 'pixelated' }} />
                    ) : (
                      <div className="text-muted-foreground">No science cutout</div>
                    )}
                  </div>
                  <div className="flex-1 flex items-center justify-center min-h-0">
                    {templateImage ? (
                      <img src={templateImage} alt="Reference large" className="w-full h-full object-contain" style={{ imageRendering: 'pixelated' }} />
                    ) : (
                      <div className="text-muted-foreground">No reference cutout</div>
                    )}
                  </div>
                  <div className="flex-1 flex items-center justify-center min-h-0">
                    {differenceImage ? (
                      <img src={differenceImage} alt="Difference large" className="w-full h-full object-contain" style={{ imageRendering: 'pixelated' }} />
                    ) : (
                      <div className="text-muted-foreground">No difference cutout</div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </DialogContent>
        </Dialog>
      </Card>
    )
}