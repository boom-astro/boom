import { useEffect, useState } from "react";
import { toast } from "sonner";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Checkbox } from "@/components/ui/checkbox";
import { Spinner } from "@/components/ui/spinner";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { fetchAdminUsers, updateAdminUser, type AdminUser, type AdminUserPatch } from "@/lib/api";
import { useAppStore } from "@/lib/store";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";

const PAGE_SIZE = 50;

const ACL_LABELS: Record<string, string> = {
  winter: "WINTER",
  ztf_partnership: "ZTF partnership",
  ztf_caltech: "ZTF Caltech",
};

const aclLabel = (acl: string) => ACL_LABELS[acl] ?? acl;

type PendingChange = {
  user: AdminUser;
  patch: AdminUserPatch;
  title: string;
  description: string;
  grants: boolean;
};

// User management: one card on the admin page, beside the catalog and task
// surfaces. The route guard checks `is_admin` and re-reads the profile to do
// it; every route called from here is enforced server-side as well.
export function AdminUsers() {
  const profile = useAppStore((s) => s.profile);
  const [search, setSearch] = useState("");
  const [query, setQuery] = useState("");
  const [offset, setOffset] = useState(0);
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [total, setTotal] = useState(0);
  const [availableAcls, setAvailableAcls] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState<Set<string>>(new Set());
  const [failed, setFailed] = useState(false);
  const [pending, setPending] = useState<PendingChange | null>(null);

  useEffect(() => {
    const timer = setTimeout(() => {
      setQuery(search);
      // Only when the search actually changed. Typing a character and deleting
      // it fires this effect twice with the same text, and resetting the offset
      // on the second one threw the reader back to page 1.
      if (search !== query) setOffset(0);
    }, 300);
    return () => clearTimeout(timer);
  }, [search, query]);

  useEffect(() => {
    if (!profile?.is_admin) return;
    let active = true;
    setLoading(true);
    fetchAdminUsers(query, PAGE_SIZE, offset)
      .then((page) => {
        if (!active) return;
        setUsers(page.users);
        setTotal(page.total);
        setAvailableAcls(page.available_acls);
        setFailed(false);
      })
      .catch((err) => {
        if (!active) return;
        // The rows on screen belong to the page that was asked for before this
        // one, and `offset` has already moved: left in place they sat under a
        // footer reading "51 to 100 of 120" while showing rows 1 to 50.
        setUsers([]);
        setTotal(0);
        setFailed(true);
        toast.error(`Failed to load users: ${err.message ?? err}`);
      })
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [profile?.is_admin, query, offset]);

  // The route guard has already decided this, having re-read the profile to do
  // it. Kept because `profile.id` is read below and the rows mean nothing
  // without it.
  if (!profile?.is_admin) return null;

  async function update(user: AdminUser, patch: AdminUserPatch) {
    setSaving((s) => new Set(s).add(user.id));
    try {
      const updated = await updateAdminUser(user.id, patch);
      setUsers((list) => list.map((u) => (u.id === updated.id ? updated : u)));
      toast.success(`Updated ${updated.email}`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving((s) => {
        const next = new Set(s);
        next.delete(user.id);
        return next;
      });
    }
  }

  function toggleAdmin(user: AdminUser, isAdmin: boolean) {
    setPending({
      user,
      patch: { is_admin: isAdmin },
      grants: isAdmin,
      title: isAdmin ? `Make ${user.email} an admin?` : `Remove admin status from ${user.email}?`,
      description: isAdmin
        ? "Admins can see every restricted dataset and change any user's admin status and ACLs."
        : "This user will keep only the ACLs listed on their row.",
    });
  }

  function toggleAcl(user: AdminUser, acl: string, granted: boolean) {
    const linked =
      granted && acl === "ztf_caltech" && !user.acls.includes("ztf_partnership")
        ? "ztf_partnership"
        : !granted && acl === "ztf_partnership" && user.acls.includes("ztf_caltech")
          ? "ztf_caltech"
          : null;
    const changed = linked ? [acl, linked] : [acl];
    const labels = changed.map(aclLabel).join(" and ");
    setPending({
      user,
      patch: granted ? { grant_acls: [acl] } : { revoke_acls: [acl] },
      grants: granted,
      title: granted
        ? `Grant ${labels} access to ${user.email}?`
        : `Revoke ${labels} access from ${user.email}?`,
      description:
        (granted
          ? `This user will be able to see ${labels} data.`
          : `This user will no longer see ${labels} data.`) +
        (linked ? " ZTF Caltech access requires ZTF partnership access." : ""),
    });
  }

  function confirmPending() {
    if (!pending) return;
    update(pending.user, pending.patch);
    setPending(null);
  }

  const lastIndex = Math.min(offset + users.length, total);

  return (
    <>
      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Users</CardTitle>
          <CardDescription>
            Admins can manage users and see every restricted dataset. ACLs grant access to one restricted dataset.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <Input
            placeholder="Search by email, username or name"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="max-w-sm"
          />

          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>User</TableHead>
                  <TableHead>Joined</TableHead>
                  <TableHead className="text-center">Admin</TableHead>
                  {availableAcls.map((acl) => (
                    <TableHead key={acl} className="text-center">{aclLabel(acl)}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading ? (
                  <TableRow>
                    <TableCell colSpan={3 + availableAcls.length} className="h-24 text-center">
                      <Spinner className="mx-auto" />
                    </TableCell>
                  </TableRow>
                ) : users.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={3 + availableAcls.length} className="h-24 text-center text-muted-foreground">
                      {failed ? "Users could not be read" : "No users found"}
                    </TableCell>
                  </TableRow>
                ) : (
                  users.map((user) => {
                    const busy = saving.has(user.id);
                    const isSelf = user.id === profile.id;
                    return (
                      <TableRow key={user.id}>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <span className="font-medium">{user.name || user.username}</span>
                            {isSelf && <Badge variant="secondary">You</Badge>}
                            {!user.is_activated && <Badge variant="outline">Not activated</Badge>}
                          </div>
                          <div className="text-xs text-muted-foreground">{user.email}</div>
                        </TableCell>
                        <TableCell className="text-sm text-muted-foreground">
                          {new Date(user.created_at * 1000).toLocaleDateString()}
                        </TableCell>
                        <TableCell className="text-center">
                          <Switch
                            checked={user.is_admin}
                            disabled={busy || isSelf}
                            onCheckedChange={(checked) => toggleAdmin(user, checked)}
                            aria-label={`Admin status of ${user.email}`}
                          />
                        </TableCell>
                        {availableAcls.map((acl) => (
                          <TableCell key={acl} className="text-center">
                            <Checkbox
                              checked={user.is_admin || user.acls.includes(acl)}
                              disabled={busy || user.is_admin}
                              onCheckedChange={(checked) => toggleAcl(user, acl, checked === true)}
                              aria-label={`${aclLabel(acl)} access of ${user.email}`}
                            />
                          </TableCell>
                        ))}
                      </TableRow>
                    );
                  })
                )}
              </TableBody>
            </Table>
          </div>

          <div className="flex items-center justify-between text-sm text-muted-foreground">
            <span>{total === 0 ? "0 users" : `${offset + 1} to ${lastIndex} of ${total} users`}</span>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={loading || offset === 0}
                onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
              >
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={loading || lastIndex >= total}
                onClick={() => setOffset(offset + PAGE_SIZE)}
              >
                Next
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <Dialog open={pending !== null} onOpenChange={(open) => !open && setPending(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{pending?.title}</DialogTitle>
            <DialogDescription>{pending?.description}</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setPending(null)}>
              Cancel
            </Button>
            <Button variant={pending?.grants ? "default" : "destructive"} onClick={confirmPending}>
              Confirm
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
