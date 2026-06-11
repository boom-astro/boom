import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { toast } from "sonner";
import { cn } from "@/lib/utils.ts";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { fetchProfile, fetchKafkaCredentials, createKafkaCredential, deleteKafkaCredential, fetchTokens, createToken, deleteToken, type Profile as ProfileType, type KafkaCredential, type TokenPublic, type TokenResponse } from "@/lib/api";
import { Copy, Key, Plus, Trash } from "lucide-react";
import { IconEye, IconEyeOff } from "@tabler/icons-react";
import { Spinner } from "@/components/ui/spinner.tsx";
import * as analytics from "@/lib/analytics";

export default function Profile() {
  const [profile, setProfile] = useState<ProfileType>(null);
  const [loading, setLoading] = useState(true);

  // Kafka credential state
  const [credentials, setCredentials] = useState<KafkaCredential[]>([]);
  const [creating, setCreating] = useState<Set<string>>(new Set());
  const [deleting, setDeleting] = useState<Set<string>>(new Set());
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [newCredentialName, setNewCredentialName] = useState("");
  const [credentialToDelete, setCredentialToDelete] = useState<string | null>(null);
  const [revealedSecrets, setRevealedSecrets] = useState<Set<string>>(new Set());

  // Api token state
  const [tokens, setTokens] = useState<TokenPublic[]>([]);
  const [creatingToken, setCreatingToken] = useState<boolean>(false);
  const [deletingToken, setDeletingToken] = useState<boolean>(false);
  const [showCreateTokenDialog, setShowCreateTokenDialog] = useState(false);
  const [newTokenName, setNewTokenName] = useState("");
  const [newTokenExpiry, setNewTokenExpiry] = useState("365");
  const [customExpiry, setCustomExpiry] = useState("");
  const [tokenToDelete, setTokenToDelete] = useState<string | null>(null);
  const [newlyCreatedToken, setNewlyCreatedToken] = useState<TokenResponse | null>(null);

  useEffect(() => {
    loadData();
  }, []);

  async function loadData() {
    setLoading(true);
    try {
      const [prof, creds, toks] = await Promise.all([
        fetchProfile(),
        fetchKafkaCredentials(),
        fetchTokens()
      ]);
      setProfile(prof);
      // Ensure credentials is always an array
      setCredentials(Array.isArray(creds) ? creds : []);
      setTokens(Array.isArray(toks) ? toks : []);
    } catch (error) {
      console.error('Profile load error:', error);
      toast.error(`Failed to load profile: ${error}`);
    } finally {
      setLoading(false);
    }
  }

  async function handleCreateCredential() {
    if (!newCredentialName.trim()) {
      toast.error("Please enter a name for the credential");
      return;
    }

    analytics.trackKafkaCredentialCreateInitiated({
      credential_name: newCredentialName,
    });

    // Loading state
    const tempId = "loadingCred_" + Date.now();
    setCreating((ids) => new Set([...ids, tempId]));
    const loadingCred = {
      id: tempId,
      name: newCredentialName.trim(),
      kafka_username: "...",
      kafka_password: "...",
    };
    setShowCreateDialog(false);
    setNewCredentialName("");
    setCredentials((creds) => [...creds, loadingCred]);

    try {
      const newCred = await createKafkaCredential(loadingCred.name);
      setCredentials((creds) => creds.map((c) => c.id === tempId ? newCred : c));
      // Auto-reveal the newly created credential's secret
      setRevealedSecrets(new Set([...revealedSecrets, newCred.id]));
      toast.success("Kafka credential created successfully");
      analytics.trackKafkaCredentialCreated({
        credential_id: newCred.id,
        credential_name: newCred.name,
      });
    } catch (error) {
      setCredentials((creds) => creds.filter((c) => c.id !== tempId));
      toast.error(`Failed to create credential: ${error}`);
      analytics.trackError('kafka_credential_creation', error, { credential_name: loadingCred.name });
      await loadData();
    } finally {
      setCreating((ids) => new Set([...ids].filter(id => id !== tempId)));
    }
  }

  async function handleDeleteCredential() {
    setShowDeleteDialog(false);
    const idToDelete = credentialToDelete;
    if (!idToDelete) {
      toast.error("No credential selected for deletion");
      return;
    }
    try {
      setDeleting((ids) => new Set([...ids, idToDelete]));
      await deleteKafkaCredential(idToDelete);
      setCredentials(creds => creds.filter(c => c.id !== idToDelete));
      setRevealedSecrets((ids) => new Set([...ids].filter(id => id !== idToDelete)));
      toast.success("Kafka credential deleted successfully");
      analytics.trackKafkaCredentialDeleted({
        credential_id: idToDelete,
      });
    } catch (error) {
      toast.error(`Failed to delete credential: ${error}`);
      analytics.trackError('kafka_credential_deletion', error, { credential_id: idToDelete });
      await loadData();
    } finally {
      setDeleting((ids) => new Set([...ids].filter(id => id !== idToDelete)));
      setCredentialToDelete(null);
    }
  }

  function copyToClipboard(text: string, label: string) {
    navigator.clipboard.writeText(text).then(() => {
      toast.success(`${label} copied to clipboard`);
      analytics.trackCredentialCopied({
        label,
      });
    }).catch(() => {
      toast.error("Failed to copy to clipboard");
    });
  }

  function toggleSecretVisibility(credentialsId: string) {
    const newSet = new Set(revealedSecrets);
    const isRevealing = !newSet.has(credentialsId);
    if (newSet.has(credentialsId)) {
      newSet.delete(credentialsId);
    } else {
      newSet.add(credentialsId);
    }
    setRevealedSecrets(newSet);
    analytics.trackCredentialSecretToggled({
      credential_id: credentialsId,
      revealed: isRevealing,
    });
  }

  async function handleCreateApiToken() {
    if (!newTokenName.trim()) {
      toast.error("Please enter a name for the token");
      return;
    }

    const rawDays = newTokenExpiry === "custom" ? customExpiry : newTokenExpiry;
    const expiryDays = rawDays.trim() === "" ? 365 : parseInt(rawDays);
    if (isNaN(expiryDays) || expiryDays < 1 || expiryDays > 1095) {
      toast.error("Expiration must be between 1 and 1095 days");
      return;
    }

    analytics.trackApiTokenCreateInitiated({
      token_name: newTokenName,
      expiry_days: expiryDays,
    });

    setCreatingToken(true);
    try {
      const created = await createToken(newTokenName.trim(), expiryDays === 365 ? undefined : expiryDays);
      setTokens((toks) => [...toks, {
        id: created.id,
        name: created.name,
        created_at: created.created_at,
        expires_at: created.expires_at,
        last_used_at: null,
      }]);
      setShowCreateTokenDialog(false);
      setNewlyCreatedToken(created);
      toast.success("Token created successfully");
      analytics.trackApiTokenCreated({
        token_id: created.id,
        token_name: created.name,
        expiry_days: expiryDays,
      });
    } catch (error) {
      toast.error(`Failed to create token: ${error}`);
      analytics.trackError('api_token_creation', error, { token_name: newTokenName, expiry_days: expiryDays });
      await loadData();
    } finally {
      setCreatingToken(false);
      setNewTokenName("");
      setNewTokenExpiry("365");
      setCustomExpiry("");
    }
  }

  async function handleDeleteApiToken() {
    if (!tokenToDelete) {
      toast.error("No token selected for deletion");
      return;
    }
    setDeletingToken(true);
    try {
      await deleteToken(tokenToDelete);
      setTokens((toks) => toks.filter((t) => t.id !== tokenToDelete));
      toast.success("Token deleted successfully");
      analytics.trackApiTokenDeleted({
        token_id: tokenToDelete,
      });
    } catch (error) {
      toast.error(`Failed to delete token: ${error}`);
      analytics.trackError('api_token_deletion', error, { token_id: tokenToDelete });
      await loadData();
    } finally {
      setDeletingToken(false);
      setTokenToDelete(null);
    }
  }

  function formatDate(timestamp: number): string {
    return new Date(timestamp * 1000).toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  }

  function isTokenExpired(expiresAt: number): boolean {
    return expiresAt * 1000 < Date.now();
  }

  if (loading) {
    return <div className="px-4 py-6">Loading profile...</div>;
  }

  return (
    <div className="px-4 lg:px-6 w-full max-w-3xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold">Profile</h1>
        <p className="text-sm text-muted-foreground">Manage your account, Kafka credentials, and API tokens</p>
      </div>

      {/* User Info Section */}
      <Card className="mb-6">
        <CardHeader>
          <CardTitle>User Information</CardTitle>
          <CardDescription>Your account details</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {profile?.username && (
              <div>
                <Label className="text-muted-foreground">Username</Label>
                <p className="font-medium">{profile.username}</p>
              </div>
            )}
            {profile?.email && (
              <div>
                <Label className="text-muted-foreground">Email</Label>
                <p className="font-medium">{profile.email}</p>
              </div>
            )}
            {profile?.name && (
              <div>
                <Label className="text-muted-foreground">Name</Label>
                <p className="font-medium">{profile.name}</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Kafka Credentials Section */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Kafka Credentials</CardTitle>
              <CardDescription>Manage your Kafka authentication credentials</CardDescription>
            </div>
            <Button onClick={() => setShowCreateDialog(true)} size="sm">
              <Plus className="h-4 w-4 mr-2" />
              Create New
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {!Array.isArray(credentials) || credentials.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              <p>No Kafka credentials yet</p>
              <p className="text-sm mt-2">Create your first credential to start streaming alerts</p>
            </div>
          ) : (
            <div className="space-y-4">
              {credentials.map((cred) => {
                const isRevealed = revealedSecrets.has(cred.id);
                return (
                  <div key={cred.id} className={cn("border rounded-lg p-4",
                    deleting.has(cred.id) && "shimmer-destructive",
                    creating.has(cred.id) && "shimmer"
                  )}>
                    <div className="flex items-start justify-between mb-3 flex-row">
                        <h3 className="font-semibold">{cred.name}</h3>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => {
                            setCredentialToDelete(cred.id);
                            setShowDeleteDialog(true);
                          }}
                          disabled={deleting.has(cred.id) || creating.has(cred.id)}
                        >
                          <Trash className="h-4 w-4 text-destructive" />
                        </Button>
                    </div>
                    <div className="space-y-3">
                      <div>
                        <Label className="text-xs text-muted-foreground">Kafka Username</Label>
                        <div className="flex items-center gap-2 mt-1">
                          <code className="flex-1 bg-muted px-3 py-2 rounded text-sm font-mono">
                            {cred.kafka_username}
                          </code>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => copyToClipboard(cred.kafka_username, "Kafka Username")}
                          >
                            <Copy className="h-4 w-4" />
                          </Button>
                        </div>
                      </div>
                      <div>
                        <Label className="text-xs text-muted-foreground">Kafka Password</Label>
                        <div className="flex items-center gap-2 mt-1">
                          <code className="flex-1 bg-muted px-3 py-2 rounded text-sm font-mono">
                            {creating.has(cred.id) || isRevealed ? cred.kafka_password : "••••••••••••••••"}
                          </code>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => toggleSecretVisibility(cred.id)}
                          >
                            {isRevealed ? <IconEyeOff className="h-4 w-4" /> : <IconEye className="h-4 w-4" />}
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => copyToClipboard(cred.kafka_password, "Kafka Password")}
                          >
                            <Copy className="h-4 w-4" />
                          </Button>
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>

      {/* API Tokens Section */}
      <Card className="mt-6">
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>API Tokens</CardTitle>
              <CardDescription>Manage your personal access tokens for the API</CardDescription>
            </div>
            <Button onClick={() => setShowCreateTokenDialog(true)} size="sm">
              <Plus className="h-4 w-4 mr-2" />
              Create New
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {!Array.isArray(tokens) || tokens.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              <p>No API tokens yet</p>
              <p className="text-sm mt-2">Create your first token to authenticate with the API</p>
            </div>
          ) : (
            <div className="space-y-4">
              {tokens.map((token) => (
                <div key={token.id} className="border rounded-lg p-4">
                  <div className="flex items-start justify-between mb-3 flex-row">
                    <div className="flex items-center gap-2">
                      <Key className="h-4 w-4 text-muted-foreground" />
                      <h3 className="font-semibold">{token.name}</h3>
                      {isTokenExpired(token.expires_at) && (
                        <span className="text-xs bg-destructive/10 text-destructive px-2 py-0.5 rounded-full">Expired</span>
                      )}
                    </div>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => setTokenToDelete(token.id)}
                    >
                      <Trash className="h-4 w-4 text-destructive" />
                    </Button>
                  </div>
                  <div className="space-y-1 text-sm text-muted-foreground">
                    <div className="flex gap-4">
                      <span>Created: {formatDate(token.created_at)}</span>
                      <span>Expires: {formatDate(token.expires_at)}</span>
                    </div>
                    {token.last_used_at && (
                      <div>Last used: {formatDate(token.last_used_at)}</div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Create Credential Dialog */}
      <Dialog open={showCreateDialog} onOpenChange={setShowCreateDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Kafka Credential</DialogTitle>
            <DialogDescription>
              Create a new set of Kafka credentials for streaming alerts. Give it a descriptive name to identify its purpose.
            </DialogDescription>
          </DialogHeader>
          <div className="py-4">
            <Label htmlFor="credentialName">Credential Name</Label>
            <Input
              id="credentialName"
              placeholder="e.g., production-consumer"
              value={newCredentialName}
              onChange={(e) => setNewCredentialName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") handleCreateCredential()
              }}
              className="mt-2"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowCreateDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreateCredential} disabled={!newCredentialName.trim()}>
              Create
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Credential Dialog */}
      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Kafka Credential</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this Kafka credential? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowDeleteDialog(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleDeleteCredential}
            >
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Create Token Dialog */}
      <Dialog open={showCreateTokenDialog} onOpenChange={setShowCreateTokenDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create API Token</DialogTitle>
            <DialogDescription>
              Create a new personal access token for API authentication. The token will only be shown once after creation.
            </DialogDescription>
          </DialogHeader>
          <div className="py-4 space-y-4">
            <div>
              <Label htmlFor="name">Name</Label>
              <Input
                id="name"
                placeholder="e.g., my-script"
                value={newTokenName}
                onChange={(e) => setNewTokenName(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") handleCreateApiToken()
                }}
                className="mt-2"
              />
            </div>
            <div>
              <Label htmlFor="tokenExpiry">Expires In</Label>
              <Select value={newTokenExpiry} onValueChange={(v) => { setNewTokenExpiry(v); if (v !== "custom") setCustomExpiry(""); }}>
                <SelectTrigger className="mt-2">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="30">30 days</SelectItem>
                  <SelectItem value="90">90 days</SelectItem>
                  <SelectItem value="365">1 year</SelectItem>
                  <SelectItem value="730">2 years</SelectItem>
                  <SelectItem value="1095">3 years</SelectItem>
                  <SelectItem value="custom">Custom...</SelectItem>
                </SelectContent>
              </Select>
              {newTokenExpiry === "custom" && (
                <div className="mt-2">
                  <Input
                    id="customExpiry"
                    type="number"
                    min={1}
                    max={1095}
                    placeholder="Number of days (1-1095)"
                    value={customExpiry}
                    onChange={(e) => setCustomExpiry(e.target.value)}
                  />
                </div>
              )}
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setShowCreateTokenDialog(false)}
              disabled={creatingToken}
            >
              Cancel
            </Button>
            <Button
              onClick={handleCreateApiToken}
              disabled={!newTokenName.trim() || creatingToken}
            >
              <Spinner data-icon="inline-start" className={!creatingToken ? "hidden" : ""} />
              Create
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Token value Dialog, shown once after creating a token */}
      <Dialog open={!!newlyCreatedToken} onOpenChange={() => setNewlyCreatedToken(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Token Created</DialogTitle>
            <DialogDescription>
              Copy your token now. You won't be able to see it again.
            </DialogDescription>
          </DialogHeader>
          {newlyCreatedToken && (
            <div className="py-4">
              <Label className="text-xs text-muted-foreground">Access Token</Label>
              <div className="flex items-center gap-2 mt-1">
                <code className="flex-1 bg-muted px-3 py-2 rounded text-sm font-mono break-all">
                  {newlyCreatedToken.access_token}
                </code>
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={() => copyToClipboard(newlyCreatedToken.access_token, "Access Token")}
                >
                  <Copy className="h-4 w-4" />
                </Button>
              </div>
            </div>
          )}
          <DialogFooter>
            <Button onClick={() => setNewlyCreatedToken(null)}>
              Done
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Token Dialog */}
      <Dialog open={!!tokenToDelete} onOpenChange={() => setTokenToDelete(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete API Token</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this API token? Any applications using this token will lose access. This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setTokenToDelete(null)} disabled={deletingToken}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleDeleteApiToken}
              disabled={deletingToken}
            >
              <Spinner data-icon="inline-start" className={!deletingToken ? "hidden" : ""} />
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
