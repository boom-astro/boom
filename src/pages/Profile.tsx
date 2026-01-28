import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { toast } from "sonner";
import { fetchProfile, fetchKafkaCredentials, createKafkaCredential, deleteKafkaCredential, type Profile as ProfileType, type KafkaCredential } from "@/lib/api";
import { Copy, Plus, Trash } from "lucide-react";
import { IconEye, IconEyeOff } from "@tabler/icons-react";

export default function Profile() {
  const [profile, setProfile] = useState<ProfileType>(null);
  const [credentials, setCredentials] = useState<KafkaCredential[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [deleting, setDeleting] = useState<string | null>(null);
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
      setRevealedSecrets(new Set([...revealedSecrets, newCred.kafka_username]));
      toast.success("Kafka credential created successfully");
      // let's reload profile to reflect any changes
      await loadData();
    } catch (error) {
      toast.error(`Failed to create credential: ${error}`);
    } finally {
      setCreating(false);
    }
  }

  async function handleDeleteCredential(credentialId: string) {
    setDeleting(credentialId);
    try {
      await deleteKafkaCredential(credentialId);
      setCredentials(credentials.filter(cred => cred.id !== credentialId));
      toast.success("Kafka credential deleted successfully");
      // let's reload profile to reflect any changes
      await loadData();
    } catch (error) {
      toast.error(`Failed to delete credential: ${error}`);
    } finally {
      setDeleting(null);
    }
  }

  function copyToClipboard(text: string, label: string) {
    navigator.clipboard.writeText(text).then(() => {
      toast.success(`${label} copied to clipboard`);
    }).catch(() => {
      toast.error("Failed to copy to clipboard");
    });
  }

  function toggleSecretVisibility(credentialsId: string) {
    const newSet = new Set(revealedSecrets);
    if (newSet.has(credentialsId)) {
      newSet.delete(credentialsId);
    } else {
      newSet.add(credentialsId);
    }
    setRevealedSecrets(newSet);
  }

  if (loading) {
    return <div className="px-4 py-6">Loading profile...</div>;
  }

  return (
    <div className="px-4 lg:px-6 w-full max-w-3xl mx-auto">
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
                const isRevealed = revealedSecrets.has(cred.id);
                return (
                  <div key={cred.id} className="border rounded-lg p-4">
                    <div className="flex items-start justify-between mb-3 flex-row">
                        <h3 className="font-semibold">{cred.name}</h3>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => setDeleting(cred.id)}
                        >
                          <Trash className="h-4 w-4" />
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
                            {isRevealed ? cred.kafka_password : "••••••••••••••••"}
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

      {/* Delete Credential Dialog */}
      <Dialog open={deleting !== null} onOpenChange={() => setDeleting(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Kafka Credential</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this Kafka credential? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleting(null)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={() => {
              if (deleting) {
                handleDeleteCredential(deleting);
              }
            }} disabled={deleting === null}>
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
