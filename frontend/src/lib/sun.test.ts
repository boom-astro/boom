import { describe, expect, it } from "vitest"
import {
  darkIntervals,
  nextCrossing,
  NIGHT_SUN_ALTITUDE,
  skyState,
  subsolarPoint,
  sunAltitudeAt,
} from "@/lib/sun"

const PALOMAR = { lat: 33.3563, lon: -116.865 }

describe("subsolarPoint", () => {
  it("sits on the Tropic of Cancer at the June solstice", () => {
    expect(subsolarPoint(Date.UTC(2026, 5, 21, 8, 24)).lat).toBeCloseTo(23.44, 1)
  })

  it("crosses the equator at the March equinox", () => {
    expect(Math.abs(subsolarPoint(Date.UTC(2026, 2, 20, 14, 46)).lat)).toBeLessThan(0.05)
  })

  it("follows the equation of time at noon UTC", () => {
    expect(subsolarPoint(Date.UTC(2026, 10, 3, 12)).lon).toBeCloseTo(-4.1, 0)
  })
})

describe("sky state at Palomar", () => {
  it("is night after local midnight and day after local noon", () => {
    expect(skyState(sunAltitudeAt(Date.UTC(2026, 9, 1, 8), PALOMAR.lat, PALOMAR.lon))).toBe("night")
    expect(skyState(sunAltitudeAt(Date.UTC(2026, 9, 1, 20), PALOMAR.lat, PALOMAR.lon))).toBe("day")
  })

  it("finds the end of the night at nautical dawn", () => {
    const dawn = nextCrossing(Date.UTC(2026, 9, 1, 8), PALOMAR.lat, PALOMAR.lon)
    expect(dawn).not.toBeNull()
    expect(dawn!).toBeGreaterThan(Date.UTC(2026, 9, 1, 12))
    expect(dawn!).toBeLessThan(Date.UTC(2026, 9, 1, 13, 30))
    expect(sunAltitudeAt(dawn!, PALOMAR.lat, PALOMAR.lon)).toBeCloseTo(NIGHT_SUN_ALTITUDE, 1)
  })

  it("splits a day and a half into the dark windows it contains", () => {
    const from = Date.UTC(2026, 9, 1, 8)
    const intervals = darkIntervals(from, from + 36 * 3_600_000, PALOMAR.lat, PALOMAR.lon)
    expect(intervals).toHaveLength(2)
    expect(intervals[0].start).toBe(from)
    expect(intervals[1].end - intervals[1].start).toBeGreaterThan(9 * 3_600_000)
  })
})
