import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";
import { fetchProfile, fetchKafkaCredentials, createKafkaCredential, type Profile as ProfileType, type KafkaCredential } from "@/lib/api";
import { Copy, Plus } from "lucide-react";
import { IconEye, IconEyeOff } from "@tabler/icons-react";

export default function Profile() {
  const [profile, setProfile] = useState<ProfileType>(null);
  const [credentials, setCredentials] = useState<KafkaCredential[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [newCredentialName, setNewCredentialName] = useState("");
  const [revealedSecrets, setRevealedSecrets] = useState<Set<string>>(new Set());

  useEffect(() => {
    loadData();
  }, []);

  async function loadData() {
    setLoading(true);
    try {
      const [prof, creds] = await Promise.all([
        fetchProfile(),
        fetchKafkaCredentials()
      ]);
      setProfile(prof);
      // Ensure credentials is always an array
      setCredentials(Array.isArray(creds) ? creds : []);
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

    setCreating(true);
    try {
      const newCred = await createKafkaCredential(newCredentialName.trim());
      setCredentials([...credentials, newCred]);
      setNewCredentialName("");
      setShowCreateDialog(false);
      // Auto-reveal the newly created credential's secret
      setRevealedSecrets(new Set([...revealedSecrets, newCred.client_id]));
      toast.success("Kafka credential created successfully");
    } catch (error) {
      toast.error(`Failed to create credential: ${error}`);
    } finally {
      setCreating(false);
    }
  }

  function copyToClipboard(text: string, label: string) {
    navigator.clipboard.writeText(text).then(() => {
      toast.success(`${label} copied to clipboard`);
    }).catch(() => {
      toast.error("Failed to copy to clipboard");
    });
  }

  function toggleSecretVisibility(clientId: string) {
    const newSet = new Set(revealedSecrets);
    if (newSet.has(clientId)) {
      newSet.delete(clientId);
    } else {
      newSet.add(clientId);
    }
    setRevealedSecrets(newSet);
  }

  if (loading) {
    return <div className="px-4 py-6">Loading profile...</div>;
  }

  return (
    <div className="px-4 lg:px-6 max-w-6xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold">Profile</h1>
        <p className="text-sm text-muted-foreground">Manage your account and Kafka credentials</p>
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
                const isRevealed = revealedSecrets.has(cred.client_id);
                return (
                  <div key={cred.client_id} className="border rounded-lg p-4">
                    <div className="flex items-start justify-between mb-3">
                      <div>
                        <h3 className="font-semibold">{cred.name}</h3>
                        <Badge variant="outline" className="mt-1">Kafka Credential</Badge>
                      </div>
                    </div>
                    
                    <div className="space-y-3">
                      <div>
                        <Label className="text-xs text-muted-foreground">Client ID</Label>
                        <div className="flex items-center gap-2 mt-1">
                          <code className="flex-1 bg-muted px-3 py-2 rounded text-sm font-mono">
                            {cred.client_id}
                          </code>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => copyToClipboard(cred.client_id, "Client ID")}
                          >
                            <Copy className="h-4 w-4" />
                          </Button>
                        </div>
                      </div>
                      
                      <div>
                        <Label className="text-xs text-muted-foreground">Client Secret</Label>
                        <div className="flex items-center gap-2 mt-1">
                          <code className="flex-1 bg-muted px-3 py-2 rounded text-sm font-mono">
                            {isRevealed ? cred.client_secret : "••••••••••••••••"}
                          </code>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => toggleSecretVisibility(cred.client_id)}
                          >
                            {isRevealed ? <IconEyeOff className="h-4 w-4" /> : <IconEye className="h-4 w-4" />}
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => copyToClipboard(cred.client_secret, "Client Secret")}
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
                if (e.key === "Enter") {
                  handleCreateCredential();
                }
              }}
              className="mt-2"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowCreateDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreateCredential} disabled={creating || !newCredentialName.trim()}>
              {creating ? "Creating..." : "Create"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
