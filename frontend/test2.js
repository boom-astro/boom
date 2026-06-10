import zlib from 'zlib'

// this should be the content of a .fits.gz file
// which is a gzipped fits file
// we need to unzip it and then parse it

const HEADER_LENGTH = 2880;

// first get the first 80 bytes of the file
function validate(fits_buffer) {
    const fits_signature = fits_buffer.slice(0, 80).toString()
    if (fits_signature.indexOf("SIMPLE  =") == -1) {
      return false
    }
    return true
}

function get_header(fits_buffer) {
    // find header length
    const header_start = fits_buffer.indexOf("SIMPLE  =")
    const header_end = fits_buffer.indexOf(" END")
    const header_text = fits_buffer.slice(header_start, header_end).toString()

    const header = {}
    
    // find all the keywords
    const keywords = []
    const keyword_regex = /[A-Z]+[0-9]*\s*=/g
    let match = keyword_regex.exec(header_text)
    while (match != null) {
        keywords.push(match[0].trim().slice(0, -1).trim())
        match = keyword_regex.exec(header_text)
    }

    // look at the strings between the keywords
    const values = []
    const comments = []
    const value_regex = /=[\s\S]+?(?=[A-Z]+[0-9]*\s*=|$)/g
    match = value_regex.exec(header_text)
    while (match != null) {
        const text = match[0].trim().slice(1).trim()
        // split them in case there is a comment
        const comment_start = text.indexOf("/")
        if (comment_start == -1) {
            values.push(text)
            comments.push("")
        } else {
            values.push(text.slice(0, comment_start).trim())
            comments.push(text.slice(comment_start + 1).trim())
        }
        match = value_regex.exec(header_text)
    }

    // verify that the lengths are the same
    if (keywords.length != values.length || keywords.length != comments.length) {
        throw new Error("Error parsing fits header")
    }

    // create the header object
    for (let i = 0; i < keywords.length; i++) {
        header[keywords[i]] = {
            value: values[i],
            comment: comments[i]
        }
    }
    return header
}

function get_image_data(fits_buffer) {
    // A FITS file consists of one or more Header + Data Units (HDUs), where the first HDU is called the primary HDU, or primary array.
    // The primary HDU is always present and contains the main data of the FITS file.
    // The primary HDU is followed by zero or more additional HDUs, which are optional.

    const main_header = get_header(fits_buffer)
    // the main header gives us information like the BITPIX, NAXIS, NAXIS1, NAXIS2
    // BITPIX is the number of bits per pixel
    // NAXIS is the number of axes
    // NAXIS1 is the number of pixels along the first axis
    // NAXIS2 is the number of pixels along the second axis

    if (parseInt(main_header.NAXIS.value, 10) != 2) {
        throw new Error("Only 2D images are supported for now")
    }

    // based on that information we can get the data units
    const bitpix = main_header.BITPIX.value
    const naxis = main_header.NAXIS.value
    const naxis1 = main_header.NAXIS1.value
    const naxis2 = main_header.NAXIS2.value
    // the data units are the data after the header, and all headers are 2880 bytes long
    // so we skip the first 2880 bytes
    const data_start = HEADER_LENGTH
    // data end is the end of the file
    const img_length = Math.abs(parseInt(main_header.NAXIS1.value, 10) * parseInt(main_header.NAXIS2.value, 10) * (parseInt(main_header.BITPIX.value, 10) / 8))
    const data_end = data_start + img_length
    const data = fits_buffer.slice(data_start, data_end)
    const data_length = data.length

    // convert the data to a 1d array of numbers, by reading the bytes in the right BITPIX format
    const data_array = []
    if (bitpix == 8) {
        for (let i = 0; i < data_length; i++) {
            data_array.push(data[i])
        }
    } else if (bitpix == 16) {
        for (let i = 0; i < data_length; i += 2) {
            data_array.push(data.readInt16BE(i))
        }
    } else if (bitpix == 32) {
        for (let i = 0; i < data_length; i += 4) {
            data_array.push(data.readInt32BE(i))
        }
    } else if (bitpix == -32) {
        for (let i = 0; i < data_length; i += 4) {
            data_array.push(data.readFloatBE(i))
        }
    } else if (bitpix == -64) {
        for (let i = 0; i < data_length; i += 8) {
            data_array.push(data.readDoubleBE(i))
        }
    } else {
        throw new Error("Error parsing data units")
    }
    
    // convert the 1d array to a 2d array
    const image = []
    for (let i = 0; i < naxis2; i++) {
        image.push(data_array.slice(i * naxis1, (i + 1) * naxis1))
    }
    // verify the dimensions
    if (image.length != naxis2 || image[0].length != naxis1) {
        throw new Error("Error parsing data units")
    }
    return image
}

function normalizeImage(image, method="minmax", lower_percentile=0.01, upper_percentile=1) {
  // we implement 3 methods here to normalize the image between 0 and 1
  // minmax: interval based on min and max values
  // zscale: interval based on IRAF ZSCALE algorithm
  // asymmetric_percentile: interval based on asymmetric percentiles, in which case we need to specify the lower and upper percentiles
  if (method == "minmax") {
    const max = Math.max(...image.flat())
    const min = Math.min(...image.flat())
    return image.map(row => row.map(val => (val - min) / (max - min)))
  } else if (method == "asymmetric_percentile") {
    const sorted = image.flat().sort((a, b) => a - b)
    const lower = lower_percentile > 0 ? sorted[Math.floor(lower_percentile * sorted.length)] : sorted[0]
    const upper = upper_percentile < 1 ? sorted[Math.floor(upper_percentile * sorted.length)] : sorted[sorted.length - 1]
    const clipped = image.map(row => row.map(val => Math.min(Math.max(val, lower), upper)))
    return clipped.map(row => row.map(val => (val - lower) / (upper - lower)))
  } else if (method == "zscale") {
    throw new Error("Not implemented yet")
  } else {
    throw new Error("Invalid normalization method")
  }
}

function stretchImage(image, cutoutType, method=null, alpha=1000.0) {
  if (method === null) {
    if (cutoutType !== "Difference") {
      method = "log"
    } else {
      method = "linear"
    }
  }

  if (method == "linear") {
    return image
  } else if (method == "log") {
    // a log stretch is given by:  y = \frac{\log{(a x + 1)}}{\log{(a + 1)}}
    // where a is a parameter that controls the contrast, that we set to 1000
    const stretched = image.map(row => row.map(val => Math.log(alpha * val + 1) / Math.log(alpha + 1)))
    return stretched
  } else if (method == "asinh") {
    const stretched = image.map(row => row.map(val => Math.asinh(val)))
    return stretched
  } else if (method == "sqrt") {
    const stretched = image.map(row => row.map(val => Math.sqrt(val)))
    return stretched
  } else {
    throw new Error("Invalid stretch method")
  }
}

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

function applyColorMap(image, colorMap="gray") {
  // we implement 2 color maps here
  // gray: gray scale
  // bone: bone color map

  if (colorMap == "gray") {
    return image.map(row => row.map(val => [val, val, val]))
  } else if (colorMap == "bone") {     
    return image.map(row => row.map(val => bone_cm[Math.floor(val)]))
  } else {
    throw new Error("Invalid color map")
  }
}

function cleanupImage(image) {
  // first, replace all dubiously large values (absolute) with NaN (>1e20)
  const new_img = image.map(row => row.map(val => Math.abs(val) > 1e20 ? NaN : val))
  // then find the median of the non-NaN values
  const filtered = new_img.flat().filter(val => !isNaN(val))
  const sorted = filtered.sort((a, b) => a - b)
  const median = sorted[Math.floor(filtered.length / 2)]
  // then replace all NaN values with the median
  return new_img.map(row => row.map(val => isNaN(val) ? median : val))
}

function cutoutDataToImage(cutout, cutoutType, normalization="asymmetric_percentile", stretch=null, colorMap="bone") {
  const { stampData } = cutout
  const base64 = stampData?.$binary ? stampData.$binary.base64 : stampData
  const buffer = Buffer.from(base64, 'base64')
  const unzipped = zlib.unzipSync(buffer).toString('base64')
  const fits_buffer = Buffer.from(unzipped, 'base64')
  const img = get_image_data(fits_buffer)
  const filtered = cleanupImage(img)
  const normalized1 = normalizeImage(filtered, "minmax") // we normalize between 0 and 1
  const stretched = stretchImage(normalized1, cutoutType, stretch) // we stretch the image
  const normalized2 = normalizeImage(stretched, normalization) // we normalize again between 0, with the normalization specified
  const image = normalized2.map(row => row.map(val => val * 255))
  const colored = applyColorMap(image, colorMap)
  
  const width = image[0].length
  const height = image.length
  if (typeof document !== 'undefined') {
    const canvas = document.createElement('canvas')
    canvas.width = width
    canvas.height = height
    const ctx = canvas.getContext('2d')
    const imageData = ctx.createImageData(width, height)
    for (let i = 0; i < height; i++) {
        for (let j = 0; j < width; j++) {            
            const pixel = colored[i][j]
            const index = (i * width + j) * 4
            imageData.data[index] = pixel[0]
            imageData.data[index + 1] = pixel[1]
            imageData.data[index + 2] = pixel[2]
            imageData.data[index + 3] = 255
        }
    }
    ctx.putImageData(imageData, 0, 0)
    return canvas.toDataURL()
  } else {
    return null
  }
}

export { validate, get_header, get_image_data, normalizeImage, stretchImage, applyColorMap, cutoutDataToImage }

export default cutoutDataToImage
