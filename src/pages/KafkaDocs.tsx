import { useMemo, useState, useEffect, lazy, Suspense } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { ChevronDown, ChevronRight, Copy } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { Link } from "react-router-dom";
import { toast } from "sonner";
import { fetchKafkaCredentials, type KafkaCredential } from "@/lib/api";
const KafkaMarkdownDocs = lazy(() => import('@/components/KafkaMarkdownDocs'));

type TopicNode = {
  key: string;
  label?: string;
  desc?: string;
  children?: TopicNode[];
};

const TOPIC_TREE: TopicNode[] = [
  {
    key: "babamul.ztf",
    label: "ZTF",
    children: [
      {
        key: "babamul.ztf.stellar",
        label: "Stellar",
        children: [
          { key: "babamul.ztf.no-lsst-match.stellar", desc: "ZTF-only, classified as stellar" },
          { key: "babamul.ztf.lsst-match.stellar", desc: "With LSST match, classified as stellar" },
        ],
      },
      {
        key: "babamul.ztf.hosted",
        label: "Hosted",
        children: [
          { key: "babamul.ztf.no-lsst-match.hosted", desc: "ZTF-only, with a host galaxy" },
          { key: "babamul.ztf.lsst-match.hosted", desc: "With LSST match, with a host galaxy" },
        ],
      },
      {
        key: "babamul.ztf.hostless",
        label: "Hostless",
        children: [
          { key: "babamul.ztf.no-lsst-match.hostless", desc: "ZTF-only, without a host galaxy" },
          { key: "babamul.ztf.lsst-match.hostless", desc: "With LSST match, without a host galaxy" },
        ],
      },
    ],
  },
  {
    key: "babamul.lsst",
    label: "LSST",
    children: [
      {
        key: "babamul.lsst.stellar",
        label: "Stellar",
        children: [
          { key: "babamul.lsst.no-ztf-match.stellar", desc: "LSST-only, classified as stellar" },
          { key: "babamul.lsst.ztf-match.stellar", desc: "With ZTF match, classified as stellar" },
        ],
      },
      {
        key: "babamul.lsst.hosted",
        label: "Hosted",
        children: [
          { key: "babamul.lsst.no-ztf-match.hosted", desc: "LSST-only, with a host galaxy" },
          { key: "babamul.lsst.ztf-match.hosted", desc: "With ZTF match, with a host galaxy" },
        ],
      },
      {
        key: "babamul.lsst.hostless",
        label: "Hostless",
        children: [
          { key: "babamul.lsst.no-ztf-match.hostless", desc: "LSST-only, without a host galaxy" },
          { key: "babamul.lsst.ztf-match.hostless", desc: "With ZTF match, without a host galaxy" },
        ],
      },
      {
        key: "babamul.lsst.unknown",
        label: "Unknown",
        children: [
          { key: "babamul.lsst.no-ztf-match.unknown", desc: "LSST-only, unclassified alerts" },
          { key: "babamul.lsst.ztf-match.unknown", desc: "With ZTF match, unclassified alerts" },
        ],
      },
    ],
  },
];

const KAFKA_BOOTSTRAP = import.meta.env.VITE_KAFKA_DOMAIN ?? "kafka.boom.example.com:9092";

function generatePython(topics: string[], groupId: string, offset: string, autoCommit: boolean, usernameVar = "<KAFKA_USERNAME>", passwordVar = "<KAFKA_PASSWORD>") {
  const topicsList = topics.map(t => `    "${t}"`).join(',\n');
  return `import os
import babamul

# Set your Kafka credentials (or even better, export these as env vars)
os.environ["BABAMUL_KAFKA_USERNAME"] = "${usernameVar}"
os.environ["BABAMUL_KAFKA_PASSWORD"] = "${passwordVar}"

# The API token is technically optional, but highly recommended for API access 
# (e.g. fetch and display cutouts). You can create API tokens in your profile page.
os.environ["BABAMUL_API_TOKEN"] = "<API_TOKEN>"

# Define parameters
limit = 1000
topics = [
${topicsList},
]

alerts: list[babamul.LsstAlert | babamul.ZtfAlert] = []

with babamul.AlertConsumer(
    topics=topics,
    group_id="${groupId}",
    offset="${offset}",
    auto_commit=${autoCommit ? 'True' : 'False'},
    timeout=10,
) as consumer:
    for alert in consumer:
        alerts.append(alert)
        if len(alerts) >= limit:
            break

print(f"Fetched {len(alerts)} alerts.")
`;
}

function generateRust(topics: string[], groupId: string, offset: string, autoCommit: boolean, usernameVar = "<KAFKA_USERNAME>", passwordVar = "<KAFKA_PASSWORD>") {
  const topicsList = topics.map(t => `"${t}"`).join(', ');
  const autoCommitCfg = autoCommit ? 'true' : 'false';
  return `use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;

fn main() {
    let consumer: BaseConsumer = ClientConfig::new()
    .set("bootstrap.servers", "${KAFKA_BOOTSTRAP}")
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanisms", "SCRAM-SHA-512")
        .set("sasl.username", "${usernameVar}")
        .set("sasl.password", "${passwordVar}")
        .set("group.id", "${groupId}")
        .set("auto.offset.reset", "${offset}")
        .set("enable.auto.commit", "${autoCommitCfg}")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[${topicsList}]).expect("Failed to subscribe");

    loop {
        match consumer.poll(None) {
            None => continue,
            Some(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    println!("{:?}", payload);
                }
            }
            Some(Err(e)) => eprintln!("Kafka error: {:?}", e),
        }
    }
}
`;
}

function CopyablePre({ code, label = "Copy snippet" }: { code: string; label?: string }) {
  const handleCopy = () => {
    navigator.clipboard.writeText(code).then(
      () => toast.success('Code copied to clipboard'),
      () => toast.error('Failed to copy code')
    );
  };

  return (
    <button
      type="button"
      onClick={handleCopy}
      className="group relative w-full text-left cursor-pointer"
      aria-label={label}
    >
      <pre className="p-2 rounded bg-muted text-sm overflow-x-auto pr-10">{code}</pre>
      <Copy className="h-4 w-4 absolute right-2 top-2 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none" />
    </button>
  );
}

export default function KafkaDocs() {
  const [step, setStep] = useState<number>(0);
  const [selected, setSelected] = useState<string[]>([]);

  // compute default collapsed nodes: groups whose immediate children are leaves
  function findLeafGroupKeys(nodes: TopicNode[]): string[] {
    const out: string[] = [];
    for (const n of nodes) {
      if (n.children && n.children.length > 0) {
        const childrenAreLeaves = n.children.every(c => !c.children || c.children.length === 0);
        if (childrenAreLeaves) out.push(n.key);
        else out.push(...findLeafGroupKeys(n.children));
      }
    }
    return out;
  }

  const defaultCollapsed = findLeafGroupKeys(TOPIC_TREE);
  const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set(defaultCollapsed));

  // Kafka credentials state
  const [kafkaCredentials, setKafkaCredentials] = useState<KafkaCredential[]>([]);
  const [selectedCredentialId, setSelectedCredentialId] = useState<string>("");
  const [loadingCredentials, setLoadingCredentials] = useState(false);

  // Other settings
  const [groupId, setGroupId] = useState<string>("");
  const [offset, setOffset] = useState<string>("earliest");
  const [autoCommit, setAutoCommit] = useState<boolean>(false);
  const [lang, setLang] = useState<'python'|'rust'>('python');
  const [showObjectAppearanceModal, setShowObjectAppearanceModal] = useState<boolean>(false);

  // Load kafka credentials on mount
  useEffect(() => {
    async function loadCredentials() {
      setLoadingCredentials(true);
      try {
        const creds = await fetchKafkaCredentials();
        setKafkaCredentials(creds);
        if (creds.length > 0) {
          setSelectedCredentialId(creds[0].kafka_username);
        }
      } catch (err) {
        console.error('Failed to load kafka credentials:', err);
      } finally {
        setLoadingCredentials(false);
      }
    }
    loadCredentials();
  }, []);

  // Get selected credential details for code generation
  const selectedCredential = kafkaCredentials.find(c => c.kafka_username === selectedCredentialId);
  const clientIdForGeneration = selectedCredential?.kafka_username || "<KAFKA_USERNAME>";
  const clientSecretForGeneration = selectedCredential?.kafka_password || "<KAFKA_PASSWORD>";

  function toggleCollapsed(key: string) {
    setCollapsed(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  // no default groupId; user must enter one before generating code

  const effectiveSelected = useMemo(() => {
    function findNodeByKey(nodes: TopicNode[], key: string): TopicNode | null {
      for (const n of nodes) {
        if (n.key === key) return n;
        if (n.children) {
          const found = findNodeByKey(n.children, key);
          if (found) return found;
        }
      }
      return null;
    }

    const effectiveSet = new Set<string>();
    for (const k of selected) {
      const node = findNodeByKey(TOPIC_TREE, k);
      if (node) {
        if (node.children && node.children.length > 0) collectDescendantKeys(node).forEach(x => effectiveSet.add(x));
        else effectiveSet.add(k);
      } else {
        effectiveSet.add(k);
      }
    }
    return Array.from(effectiveSet);
  }, [selected]);

  const generated = useMemo(() => {
    if (effectiveSelected.length === 0) return '';
    return lang === 'python'
      ? generatePython(effectiveSelected, groupId, offset, autoCommit, clientIdForGeneration, clientSecretForGeneration)
      : generateRust(effectiveSelected, groupId, offset, autoCommit, clientIdForGeneration, clientSecretForGeneration);
  }, [effectiveSelected, groupId, offset, autoCommit, lang, clientIdForGeneration, clientSecretForGeneration]);

  function collectDescendantKeys(node: TopicNode): string[] {
    if (!node.children || node.children.length === 0) return [node.key];
    return node.children.flatMap(n => collectDescendantKeys(n));
  }

  function allDescendantsSelected(node: TopicNode) {
    const keys = collectDescendantKeys(node);
    return keys.length > 0 && keys.every(k => selected.includes(k));
  }

  function someDescendantsSelected(node: TopicNode) {
    const keys = collectDescendantKeys(node);
    const some = keys.some(k => selected.includes(k));
    return some && !allDescendantsSelected(node);
  }

  function copyGenerated() {
    if (!generated) return;
    navigator.clipboard.writeText(generated).then(() => {
      toast.success('Code copied to clipboard');
    }).catch(() => {
      toast.error('Failed to copy code');
    });
  }

  const renderNode = (node: TopicNode, depth = 0) => {
    const childIndentPx = (depth + 1) * 36;
    if (node.children && node.children.length > 0) {
      const childrenAreLeaves = node.children.every(c => !c.children || c.children.length === 0);
      const isCollapsed = childrenAreLeaves && collapsed.has(node.key);
      return (
        <div key={node.key}>
          <div className="flex items-center gap-3">
            {childrenAreLeaves ? (
              <button
                type="button"
                aria-label={isCollapsed ? 'Expand topics' : 'Collapse topics'}
                onClick={(e) => { e.stopPropagation(); toggleCollapsed(node.key); }}
                className="inline-flex items-center justify-center w-8 h-8 text-muted-foreground hover:text-foreground"
              >
                {isCollapsed ? <ChevronRight className="size-4" /> : <ChevronDown className="size-4" />}
              </button>
            ) : (
              <span className="w-8" />
            )}

            <div>
              <Checkbox
                checked={allDescendantsSelected(node)}
                onCheckedChange={(v) => {
                  const keys = collectDescendantKeys(node);
                  if (v) setSelected(s => Array.from(new Set([...s, ...keys])));
                  else setSelected(s => s.filter(k => !keys.includes(k)));
                }}
              />
            </div>

            <div className="font-medium">{node.label ?? node.key} {someDescendantsSelected(node) && !allDescendantsSelected(node) ? <span className="ml-2 text-xs text-muted-foreground">•</span> : null}</div>
          </div>

          {!isCollapsed && (
            <div style={{ paddingLeft: childIndentPx }} className="mt-2 grid gap-2">
              {node.children.map(child => renderNode(child, depth + 1))}
            </div>
          )}
        </div>
      );
    }

    // leaf
    return (
      <div key={node.key} className="flex items-start gap-3">
        <div>
          <Checkbox checked={selected.includes(node.key)} onCheckedChange={(v) => { if (v) setSelected(s => Array.from(new Set([...s, node.key]))); else setSelected(s => s.filter(x => x !== node.key)) }} />
        </div>
        <div>
          <div className="font-medium">{node.label ?? node.key}</div>
          <div className="text-sm text-muted-foreground">{node.desc}</div>
        </div>
      </div>
    );
  }

  return (
    <div className="px-4 lg:px-6">
      <div className="max-w-4xl mx-auto">
        {/* Modal for object appearance documentation */}
        <Dialog open={showObjectAppearanceModal} onOpenChange={setShowObjectAppearanceModal}>
          <DialogContent className="!max-w-6xl max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle>Object appearance in output topics</DialogTitle>
            </DialogHeader>
            <Suspense fallback={<div className="text-center py-10">Loading documentation...</div>}>
              <KafkaMarkdownDocs />
            </Suspense>
          </DialogContent>
        </Dialog>

        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-lg font-medium">Babamul — Kafka access guide</h2>
            <p className="text-sm text-muted-foreground">Use your Kafka credentials from your profile page for SCRAM authentication. Client ID is the username and client secret is the password.</p>
          </div>
          <div className="flex items-center gap-3">
            <div className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-primary text-white dark:text-black font-medium select-none">{step+1}/3</div>
          </div>
        </div>

        <div className="h-2 rounded bg-muted mb-6">
          <div className="h-2 rounded bg-primary" style={{ width: `${((step+1)/3)*100}%` }} />
        </div>

        {/* Step 1 - Topics */}
        {step === 0 && (
          <Card className="min-h-[50vh]">
            <CardHeader>
              <CardTitle>1 — Pick topics</CardTitle>
              <CardDescription>Choose one or more Babamul topics to read from. <button onClick={() => setShowObjectAppearanceModal(true)} className="text-primary hover:underline cursor-pointer">Learn more about how objects will appear in topics</button>.</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid gap-3">
                <div className="flex items-center justify-between">
                  <div className="text-sm text-muted-foreground">{selected.length} topic{selected.length === 1 ? '' : 's'} selected</div>
                  {selected.length === 0 && (
                    <div className="text-sm text-red-600 dark:text-red-400">Select at least one topic to continue.</div>
                  )}
                </div>

                {TOPIC_TREE.map(node => renderNode(node, 0))}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Step 2 - Consumer settings */}
        {step === 1 && (
          <Card className="min-h-[50vh]">
            <CardHeader>
              <CardTitle>2 — Consumer settings & credentials</CardTitle>
              <CardDescription>Configure your Kafka consumer behavior and select your credentials.</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4">
                <div>
                  <label className="text-sm font-medium">Kafka Credential</label>
                  <div className="flex gap-2 mt-1">
                    {loadingCredentials ? (
                      <div className="text-sm text-muted-foreground">Loading credentials...</div>
                    ) : kafkaCredentials.length === 0 ? (
                      <div className="text-sm text-red-600 dark:text-red-400">
                        No Kafka credentials found. <Link to="/profile" className="underline hover:text-red-500">Create one in your profile</Link>.
                      </div>
                    ) : (
                      <Select value={selectedCredentialId} onValueChange={setSelectedCredentialId}>
                        <SelectTrigger className="w-full">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {kafkaCredentials.map(cred => (
                            <SelectItem key={cred.kafka_username} value={cred.kafka_username}>
                              {cred.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    )}
                  </div>
                  <p className="text-xs text-muted-foreground mt-2">Don't have any credentials or want to create new ones? <Link to="/profile" className="underline hover:text-muted-foreground">Create one</Link> in your profile.</p>
                </div>

                <div>
                  <label className="text-sm font-medium">Group ID</label>
                  <Input value={groupId} onChange={e => setGroupId(e.target.value)} placeholder="e.g. babamul-demo" className="mt-1" />
                  {groupId.trim() === '' && (
                    <div className="text-sm text-red-600 dark:text-red-400 mt-2">Group ID is required to continue.</div>
                  )}
                </div>

                <div>
                  <label className="text-sm font-medium">Auto offset reset</label>
                  <div className="flex gap-3 mt-2">
                    <Button variant={offset === 'earliest' ? 'default' : 'outline'} onClick={() => setOffset('earliest')}>earliest</Button>
                    <Button variant={offset === 'latest' ? 'default' : 'outline'} onClick={() => setOffset('latest')}>latest</Button>
                  </div>
                  <p className="text-sm text-muted-foreground mt-2">If you set <strong>earliest</strong>, your consumer will read from the beginning every time. Use <strong>latest</strong> to resume from the last committed offset (requires a stable <code>group.id</code>).</p>
                </div>

                <div className="flex items-center gap-3">
                  <label className="text-sm font-medium">Enable auto commit</label>
                  <input type="checkbox" checked={autoCommit} onChange={e => setAutoCommit(e.target.checked)} />
                </div>
                <p className="text-sm text-muted-foreground -mt-1">
                  Default is off to avoid unexpected offset commits. Turn it on if you're okay with Kafka committing reads automatically; leave it off if you prefer to manage commits yourself (you'll reread messages on restart unless you commit manually! Useful while testing for example, so you can re-read the same messages on restart).
                </p>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Step 3 - Generated code */}
        {step === 2 && (
          <Card className="min-h-[50vh]">
            <CardHeader>
              <CardTitle>3 — Generated code</CardTitle>
              <CardDescription>Choose a language and copy the example to your project.</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex items-center gap-3 mb-4">
                <Button variant={lang === 'python' ? 'default' : 'outline'} onClick={() => setLang('python')}>Python</Button>
                <Button variant={lang === 'rust' ? 'default' : 'outline'} onClick={() => setLang('rust')}>Rust</Button>
                <Separator orientation="vertical" className="mx-2" />
                {selectedCredential && (
                  <div className="text-sm text-muted-foreground">Using credential: <span className="font-medium">{selectedCredential.name}</span></div>
                )}
              </div>
              <div className="text-sm text-muted-foreground mb-2">Selected topics ({effectiveSelected.length}): <span className="font-mono text-xs">{effectiveSelected.join(', ')}</span></div>
              <div className="relative">
                <pre className="p-4 rounded bg-muted text-sm overflow-auto"><code>{generated}</code></pre>
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <button
                        onClick={copyGenerated}
                        className="absolute top-2 right-2 p-2 rounded bg-muted-foreground/10 hover:bg-muted-foreground/20 transition-colors"
                        aria-label="Copy code to clipboard"
                      >
                        <Copy className="h-4 w-4" />
                      </button>
                    </TooltipTrigger>
                    <TooltipContent>Copy code to clipboard</TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </div>

              <div className="mt-6">
                <h4 className="font-semibold">Setup</h4>
                {lang === 'python' ? (
                  <div className="mt-2 space-y-3">
                    <p className="text-sm">Install the <a href="https://pypi.org/project/babamul/" target="_blank" rel="noreferrer" className="underline hover:text-foreground font-medium">babamul</a> Python client — it handles Kafka connection, authentication, and Avro deserialization for you. It also comes with all you need to interact with the API. Check out the <a href="https://github.com/boom-astro/babamul/tree/main/examples" target="_blank" rel="noreferrer" className="underline hover:text-foreground font-medium">examples</a> for clear usage patterns covering both Kafka streams and the API.</p>
                    <Tabs defaultValue="uv">
                      <TabsList className="w-full sm:w-auto">
                        <TabsTrigger value="uv">uv</TabsTrigger>
                        <TabsTrigger value="conda">conda</TabsTrigger>
                        <TabsTrigger value="pip">pip + venv</TabsTrigger>
                      </TabsList>

                      <TabsContent value="uv" className="space-y-2">
                        <p className="text-sm text-muted-foreground">Fast as lightning, we can't recommend it enough. See installation steps here: <a href="https://docs.astral.sh/uv/getting-started/" target="_blank" rel="noreferrer" className="underline hover:text-foreground">docs.astral.sh/uv/getting-started/</a></p>
                        <ol className="list-decimal pl-5 text-sm space-y-3 marker:font-medium marker:text-muted-foreground">
                          <li>
                            <div>Create and activate an isolated env:</div>
                            <CopyablePre code={`uv venv\nsource .venv/bin/activate`} label="Copy uv venv commands" />
                          </li>
                          <li>
                            <div>Install babamul:</div>
                            <CopyablePre code={`uv pip install babamul`} label="Copy uv install command" />
                          </li>
                        </ol>
                      </TabsContent>

                      <TabsContent value="conda" className="space-y-2">
                        <p className="text-sm text-muted-foreground">Use this if you already manage Python with Anaconda/Miniconda.</p>
                        <ol className="list-decimal pl-5 text-sm space-y-3 marker:font-medium marker:text-muted-foreground">
                          <li>
                            <div>Create and activate an env (adjust the Python version if needed):</div>
                            <CopyablePre code={`conda create -n boom-kafka python=3.11 -y\nconda activate boom-kafka`} label="Copy conda env commands" />
                          </li>
                          <li>
                            <div>Install babamul with pip inside the env:</div>
                            <CopyablePre code={`pip install babamul`} label="Copy conda pip install command" />
                          </li>
                        </ol>
                      </TabsContent>

                      <TabsContent value="pip" className="space-y-2">
                        <p className="text-sm text-muted-foreground">Works anywhere Python 3 is available.</p>
                        <ol className="list-decimal pl-5 text-sm space-y-3 marker:font-medium marker:text-muted-foreground">
                          <li>
                            <div>Create and activate a virtual environment:</div>
                            <CopyablePre code={`python -m venv .venv\nsource .venv/bin/activate`} label="Copy venv commands" />
                          </li>
                          <li>
                            <div>Install babamul:</div>
                            <CopyablePre code={`python -m pip install --upgrade pip\npip install babamul`} label="Copy pip install commands" />
                          </li>
                        </ol>
                      </TabsContent>
                    </Tabs>
                  </div>
                ) : (
                  <div className="mt-2">
                    <p className="text-sm">Add the Kafka client dependency to your project:</p>
                    <CopyablePre code={`cargo add rdkafka --features="gssapi sasl ssl tracing"`} label="Copy cargo add command" />
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        )}

        <div className="flex items-center justify-between gap-4 mt-6">
          <div>
            <Button variant="outline" onClick={() => setStep(s => Math.max(0, s-1))} disabled={step === 0}>Previous</Button>
          </div>
          <div className="flex items-center gap-2">
            {step < 2 ? (
              <Button
                onClick={() => setStep(s => Math.min(2, s+1))}
                disabled={(step === 0 && selected.length === 0) || (step === 1 && groupId.trim() === '')}
              >
                Next
              </Button>
            ) : (
              <Button onClick={() => { setStep(0); }}>Start over</Button>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
