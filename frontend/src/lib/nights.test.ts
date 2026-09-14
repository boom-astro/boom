import { describe, expect, it } from "vitest"
import { describeNight, formatNightRange, formatNightRangeLong } from "@/lib/nights"

/**
 * A night labeled `2025-08-16` holds the alerts observed between the evening
 * of Aug 16 and the morning of Aug 17, so every label the dashboard prints has
 * to name both dates rather than the one the API sends.
 */

describe("formatNightRange", () => {
  it("names the evening date and the morning after", () => {
    expect(formatNightRange("2025-08-16")).toBe("Aug 16 → 17")
  })

  it("repeats the month when the night crosses one", () => {
    expect(formatNightRange("2025-08-31")).toBe("Aug 31 → Sep 1")
  })

  it("repeats the month when the night crosses a year", () => {
    expect(formatNightRange("2025-12-31")).toBe("Dec 31 → Jan 1")
  })

  it("handles the leap day", () => {
    expect(formatNightRange("2024-02-28")).toBe("Feb 28 → 29")
  })
})

describe("formatNightRangeLong", () => {
  it("adds the year of the evening the night started", () => {
    expect(formatNightRangeLong("2025-12-31")).toBe("Dec 31 → Jan 1, 2025")
  })
})

describe("describeNight", () => {
  it("names the half of the night each date refers to", () => {
    expect(describeNight("2025-08-16")).toBe("Sat evening → Sun morning")
  })
})
