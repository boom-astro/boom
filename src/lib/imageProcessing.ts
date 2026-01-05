import pako from 'pako';

const NAXIS1_BYTES = new TextEncoder().encode("NAXIS1  =");
const NAXIS2_BYTES = new TextEncoder().encode("NAXIS2  =");
const NAXIS1_BYTES_LEN = NAXIS1_BYTES.length;
const NAXIS2_BYTES_LEN = NAXIS2_BYTES.length;
const FITS_HEADER_LEN = 2880;
const SPACE_BYTES = new TextEncoder().encode(" ");
const SPACE_BYTE = SPACE_BYTES[0];

function bone(n: number) {
  const points = [
    {index:0,rgb:[0,0,0]},
    {index:.376,rgb:[84,84,116]},
    {index:.753,rgb:[169,200,200]},
    {index:1,rgb:[255,255,255]}
  ]

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

function bytes2imgdata(bytes: Uint8Array | ArrayBuffer): { data: Float32Array, naxis1: number, naxis2: number } {
  let decompressedCutout: Uint8Array;
  try {
    const compressedCutoutArray = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    decompressedCutout = pako.inflate(compressedCutoutArray);
  } catch (e) {
    console.warn("Failed to decompress cutout, assuming uncompressed data", e);
    decompressedCutout = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  }

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
    return { data: new Float32Array(), naxis1: 0, naxis2: 0 };
  }

  for (let i = naxis1_key_start + NAXIS1_BYTES_LEN; i < FITS_HEADER_LEN; i++) {
    if (subset[i] !== SPACE_BYTE) {
      naxis1_val_start = i;
      break;
    }
  }

  if (naxis1_val_start === 0) {
    console.error("NAXIS1 value start not found in FITS header");
    return { data: new Float32Array(), naxis1: 0, naxis2: 0 };
  }

  for (let i = naxis1_val_start; i < FITS_HEADER_LEN; i++) {
    if (subset[i] === SPACE_BYTE) {
      naxis1_val_end = i;
      break;
    }
  }

  const naxis1_val = subset.slice(naxis1_val_start, naxis1_val_end);
  const naxis1_val_str = new TextDecoder().decode(naxis1_val);
  const naxis1 = parseInt(naxis1_val_str, 10);

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
    return { data: new Float32Array(), naxis1: 0, naxis2: 0 };
  }
  for (let i = naxis2_key_start + NAXIS2_BYTES_LEN; i < FITS_HEADER_LEN; i++) {
    if (subset[i] !== SPACE_BYTE) {
      naxis2_val_start = i;
      break;
    }
  }
  if (naxis2_val_start === 0) {
    console.error("NAXIS2 value start not found in FITS header");
    return { data: new Float32Array(), naxis1: 0, naxis2: 0 };
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

  let data: Uint8Array | Float32Array = decompressedCutout.slice(FITS_HEADER_LEN, naxis1 * naxis2 * 4 + FITS_HEADER_LEN);
  if (data instanceof Uint8Array) {
    data = bytesToFloats(data);
  }

  const NAXIS_STANDARD = Math.max(naxis1, naxis2);
  const NB_PIXELS = NAXIS_STANDARD * NAXIS_STANDARD;

  const offset1 = Math.ceil((NAXIS_STANDARD - naxis1) / 2.0);
  const offset2 = Math.ceil((NAXIS_STANDARD - naxis2) / 2.0);

  if (offset1 !== 0 || offset2 !== 0) {
    const new_image_data = new Float32Array(NB_PIXELS);
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

  console.log(`bytes2imgdata: naxis1=${naxis1}, naxis2=${naxis2}, NAXIS_STANDARD=${NAXIS_STANDARD}, data length=${data.length}`);
  return { data: data as Float32Array, naxis1: NAXIS_STANDARD, naxis2: NAXIS_STANDARD };
}

function cleanupImage(image: Float32Array | number[]): number[] {
  const new_img = Array.from(image).map((value) => (Math.abs(value) > 1e20 ? NaN : value));
  const filtered = new_img.filter((val) => !isNaN(val));
  const sorted = filtered.sort((a, b) => a - b);
  const median = sorted[Math.floor(filtered.length / 2)] ?? 0;
  return new_img.map((value) => (isNaN(value) ? median : value));
}

function ensureFinite(image: number[]): number[] {
  return image.map((value) => {
    if (!isFinite(value)) {
      return 0;
    }
    return value;
  });
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
    const range = max - min;
    if (range === 0) {
      // If all values are the same, return 0.5
      return image.map(() => 0.5);
    }
    return image.map((value) => (value - min) / range);
  } else if (method == "asymmetric_percentile") {
    const sorted = [...image].sort((a, b) => a - b);
    const lower = lower_percentile > 0 ? sorted[Math.floor(lower_percentile * sorted.length)] : 0;
    const upper = upper_percentile < 1 ? sorted[Math.floor(upper_percentile * sorted.length)] : 1;
    const range = upper - lower;
    if (range === 0) {
      return image.map(() => 0.5);
    }
    const clipped = image.map((value) => Math.max(lower, Math.min(upper, value)));
    return clipped.map((value) => (value - lower) / range);
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
  const rgba_image: Array<[number, number, number, number]> = new Array(image.length);

  if (colorMap === "gray") {
    for (let i = 0; i < image.length; i++) {
      rgba_image[i] = [image[i], image[i], image[i], 255];
    }
  } else if (colorMap === "bone") {
    for (let i = 0; i < image.length; i++) {
      rgba_image[i] = bone_cm[image[i]] as [number, number, number, number];
    }
  } else {
    throw new Error("Invalid color map")
  }
  return rgba_image;
}

export function bytes2image(bytes: Uint8Array | string | ArrayBuffer | undefined, type: string = "science"): string | null {
  let naxis1: number;
  let naxis2: number;
  let data: number[];
  
  if (typeof bytes === 'string') {
    const binaryString = atob(bytes);
    const len = binaryString.length;
    const byteArray = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      byteArray[i] = binaryString.charCodeAt(i);
    }
    const result = bytes2imgdata(byteArray);
    console.log("bytes2image: result from bytes2imgdata:", result);
    data = Array.from(result.data);
    naxis1 = result.naxis1;
    naxis2 = result.naxis2;
  } else {
    if (!bytes) return null;
    let buf: Uint8Array;
    if (bytes instanceof Uint8Array) buf = bytes;
    else if (bytes instanceof ArrayBuffer) buf = new Uint8Array(bytes);
    else return null;
    const result = bytes2imgdata(buf);
    console.log("bytes2image: result from bytes2imgdata:", result);
    data = Array.from(result.data);
    naxis1 = result.naxis1;
    naxis2 = result.naxis2;
  }
  
  const NAXIS_STANDARD = Math.max(naxis1, naxis2);
  console.log(`bytes2image: initial naxis1=${naxis1}, naxis2=${naxis2}, NAXIS_STANDARD=${NAXIS_STANDARD}, data length=${data.length}`);
  data = cleanupImage(data);
  
  data = normalizeImage(data, "minmax");
  data = stretchImage(data, type, "log");
  data = normalizeImage(data, "asymmetric_percentile");
  data = ensureFinite(data);

  data = data.map(value => Math.round(Math.max(0, Math.min(255, value * 255))));

  const colored = applyColorMap(data, "bone");

  if (typeof document !== 'undefined') {
    const canvas = document.createElement('canvas');
    canvas.width = NAXIS_STANDARD;
    canvas.height = NAXIS_STANDARD;
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    const imageData = ctx.createImageData(NAXIS_STANDARD, NAXIS_STANDARD);
    for (let i = 0; i < NAXIS_STANDARD; i++) {
      for (let j = 0; j < NAXIS_STANDARD; j++) {
        const pixelValue = colored[i * NAXIS_STANDARD + j];
        const pixelIndex = (i * NAXIS_STANDARD + j) * 4;
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
