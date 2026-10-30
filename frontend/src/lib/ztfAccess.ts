import type { Profile } from "@/lib/api";

export type ZtfAccess = "admin" | "private" | "partnership" | "public";

const ACCESS_PROGRAMIDS: Record<ZtfAccess, number[]> = {
  admin: [1, 2, 3],
  private: [1, 2, 3],
  partnership: [1, 2],
  public: [1],
};

export const ZTF_ACCESS_LABELS: Record<ZtfAccess, string> = {
  admin: "Admin",
  private: "Private",
  partnership: "Partnership",
  public: "Public",
};

/** The widest ZTF access the profile grants: `is_admin` first, then the ACLs. */
export function ztfAccess(profile: Profile): ZtfAccess {
  if (profile?.is_admin) return "admin";
  const acls = profile?.acls ?? [];
  if (acls.includes("ztf_caltech")) return "private";
  if (acls.includes("ztf_partnership")) return "partnership";
  return "public";
}

/**
 * ZTF program IDs the access level may search (1 public, 2 partnership,
 * 3 Caltech). The profile is a cached copy, so the server must derive the same
 * set from the caller's ACLs rather than trust this one.
 */
export function ztfProgramIds(access: ZtfAccess): number[] {
  return ACCESS_PROGRAMIDS[access];
}
