const pako = require("pako");
const response = require("./src/assets/ZTF25aagbkaj.json");

const NAXIS1_BYTES = new TextEncoder().encode("NAXIS1  =");
const NAXIS2_BYTES = new TextEncoder().encode("NAXIS2  =");
const NAXIS1_BYTES_LEN = NAXIS1_BYTES.length;
const NAXIS2_BYTES_LEN = NAXIS2_BYTES.length;
const FITS_HEADER_LEN = 2880;
const NAXIS_STANDARD = 63;
const NB_PIXELS = NAXIS_STANDARD * NAXIS_STANDARD;
const SPACE_BYTES = new TextEncoder().encode(" ");
const SPACE_BYTE = SPACE_BYTES[0];

function bone(n) {
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
      Math.round(y0[2] + t * (y1[2] - y0[2]))
    ]
    lookup.push(y)
  }
  return lookup
}

const bone_cm = bone(256)

function isEqualArray(a, b) {
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

function bytesToFloats(data) {
  let floats = new Float32Array(data.length / 4);
  for (let i = 0; i < data.length; i += 4) {
    floats[i / 4] = new DataView(data.buffer, data.byteOffset + i, 4).getFloat32(0, false);
  }
  return floats;
}

function bytes2imgdata(bytes) {
  // decompress the data
  const compressedCutoutArray = new Uint8Array(bytes);
  const decompressedCutout = pako.inflate(compressedCutoutArray);

  let subset = decompressedCutout.slice(0, FITS_HEADER_LEN);

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

  let naxis1_val = subset.slice(naxis1_val_start, naxis1_val_end);
  // convert that value to a number
  let naxis1_val_str = new TextDecoder().decode(naxis1_val);
  let naxis1 = parseInt(naxis1_val_str, 10);

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

  let naxis2_val = subset.slice(naxis2_val_start, naxis2_val_end);
  let naxis2_val_str = new TextDecoder().decode(naxis2_val);
  let naxis2 = parseInt(naxis2_val_str, 10);


  // now we can get the subset of the array that contains the data
  // it starts at the end of the header, which is 2880 bytes
  // and ends at naxis1 * naxis2 * 4 bytes
  let data = decompressedCutout.slice(FITS_HEADER_LEN, naxis1 * naxis2 * 4 + FITS_HEADER_LEN);
  // convert to a Float32Array
  data = bytesToFloats(data);

  // if NAXIS1 and NAXIS2 are not NAXIS_STANDARD, we need to pad the image with zeros
  // we can't just add zeros to the end of the image_data vector, because it's a 2D array we flattened into a 1D vector
  // so if NAXIS1 is not NAXIS_STANDARD, we need to add NAXIS_STANDARD - NAXIS1 zeros to the start and end of each row
  // and if NAXIS2 is not NAXIS_STANDARD, we need to add NAXIS_STANDARD - NAXIS2 zeros to the start and end of the vector

  let offset1 = Math.ceil((NAXIS_STANDARD - naxis1) / 2.0);
  let offset2 = Math.ceil((NAXIS_STANDARD - naxis2) / 2.0);

  if (offset1 !== 0 || offset2 !== 0) {
    let new_image_data = new Float32Array(NB_PIXELS);
    for (let i = 0; i < naxis2; i++) {
      for (let j = 0; j < naxis1; j++) {
        let k = i * naxis1 + j;
        let k_new = (i + offset2) * NAXIS_STANDARD + (j + offset1);
        new_image_data[k_new] = data[k];
      }
    }
    data = new_image_data;
  }

  return data;
}

function cleanupImage(image) {
  // first, replace all dubiously large values (absolute) with NaN (>1e20)
  const new_img = image.map(value => Math.abs(value) > 1e20 ? NaN : value)
  // then find the median of the non-NaN values
  const filtered = [...new_img].filter(val => !isNaN(val))
  const sorted = filtered.sort((a, b) => a - b)
  const median = sorted[Math.floor(filtered.length / 2)]
  // then replace all NaN values with the median
  return new_img.map(value => isNaN(value) ? median : value);
}

function normalizeImage(image, method="minmax", lower_percentile=0.01, upper_percentile=1) {
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

function stretchImage(image, cutoutType, method=null, alpha=1000.0) {
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

function applyColorMap(image, colorMap="gray") {
  // we implement 2 color maps here
  // gray: gray scale
  // bone: bone color map

  if (colorMap == "gray") {
    return image.map(val => [val, val, val])
  } else if (colorMap == "bone") {     
    return image.map(val => bone_cm[Math.floor(val)])
  } else {
    throw new Error("Invalid color map")
  }
}

function bytes2image(bytes) {

  let data = bytes2imgdata(bytes);
  data = cleanupImage(data);
  
  // first we normalize with a minmax
  data = normalizeImage(data, "minmax");
  // then we stretch with a log
  data = stretchImage(data, "science", "log");
  // then we normalize again with a asymmetric_percentile
  data = normalizeImage(data, "asymmetric_percentile");

  // now that we have a value between 0 and 1, convert to 0 to 255
  data = data.map(value => Math.round(value * 255));
  // then we apply the color map
  data = applyColorMap(data, "bone");

  if (typeof document !== 'undefined') {
    const canvas = document.createElement('canvas')
    canvas.width = NAXIS_STANDARD;
    canvas.height = NAXIS_STANDARD;
    const ctx = canvas.getContext('2d');
    const imageData = ctx.createImageData(NAXIS_STANDARD, NAXIS_STANDARD);
    for (let i = 0; i < NB_PIXELS; i++) {
      for (let j = 0; j < 3; j++) {
        const pixelValue = data[i][j];
        const pixelIndex = (i * NB_PIXELS + j) * 4;
        imageData.data[pixelIndex] = [pixelValue[0], pixelValue[1], pixelValue[2], 255];
      }
    }
    ctx.putImageData(imageData, 0, 0);
    return canvas.toDataURL();
  } else {
    return null;
  }
}

const science_thumbnail = bytes2image(response.data.cutoutScience);

