import { describe, expect, it } from "vitest"
import type { Profile } from "@/lib/api"
import { ztfAccess, ztfProgramIds } from "@/lib/ztfAccess"

function profile(fields: Partial<NonNullable<Profile>>): Profile {
  return { username: "u", email: "u@example.com", created_at: 0, ...fields }
}

describe("ztfAccess", () => {
  it("gives admins every program, whatever their ACLs", () => {
    const access = ztfAccess(profile({ is_admin: true, acls: [] }))
    expect(access).toBe("admin")
    expect(ztfProgramIds(access)).toEqual([1, 2, 3])
  })

  it("gives ztf_caltech private access to every program", () => {
    const access = ztfAccess(profile({ is_admin: false, acls: ["ztf_partnership", "ztf_caltech"] }))
    expect(access).toBe("private")
    expect(ztfProgramIds(access)).toEqual([1, 2, 3])
  })

  it("gives ztf_partnership the public and partnership programs", () => {
    const access = ztfAccess(profile({ is_admin: false, acls: ["ztf_partnership"] }))
    expect(access).toBe("partnership")
    expect(ztfProgramIds(access)).toEqual([1, 2])
  })

  it("gives everyone else the public program only", () => {
    expect(ztfAccess(profile({ acls: ["winter"] }))).toBe("public")
    expect(ztfAccess(profile({}))).toBe("public")
    expect(ztfAccess(null)).toBe("public")
    expect(ztfProgramIds("public")).toEqual([1])
  })
})
