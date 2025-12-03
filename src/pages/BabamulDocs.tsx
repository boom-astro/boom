import { useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { ChevronDown, ChevronRight } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import useAppStore from "@/lib/store";

type TopicNode = {
  key: string;
  label?: string;
  desc?: string;
  children?: TopicNode[];
};

const TOPIC_TREE: TopicNode[] = [
  {
    key: "babamul.lsst",
    label: "LSST",
    children: [
      {
        key: "babamul.lsst.only",
        label: "LSST — LSST-only",
        children: [
          { key: "babamul.lsst.only.stellar", desc: "Alerts classified as stellar" },
          { key: "babamul.lsst.only.hosted", desc: "Alerts with a host galaxy" },
          { key: "babamul.lsst.only.hostless", desc: "Alerts without a host galaxy" },
          { key: "babamul.lsst.only.unknown", desc: "Unclassified alerts" },
        ],
      },
      {
        key: "babamul.lsst.ztfmatch",
        label: "LSST — with ZTF match",
        children: [
          { key: "babamul.lsst.ztfmatch.stellar", desc: "Alerts classified as stellar" },
          { key: "babamul.lsst.ztfmatch.hosted", desc: "Alerts with a host galaxy" },
          { key: "babamul.lsst.ztfmatch.hostless", desc: "Alerts without a host galaxy" },
          { key: "babamul.lsst.ztfmatch.unknown", desc: "Unclassified alerts" },
        ],
      },
    ],
  },
  {
    key: "babamul.ztf",
    label: "ZTF",
    children: [
      {
        key: "babamul.ztf.only",
        label: "ZTF — ZTF-only",
        children: [
          { key: "babamul.ztf.only.stellar", desc: "Alerts classified as stellar" },
          { key: "babamul.ztf.only.hosted", desc: "Alerts with a host galaxy" },
          { key: "babamul.ztf.only.hostless", desc: "Alerts without a host galaxy" },
          { key: "babamul.ztf.only.unknown", desc: "Unclassified alerts" },
        ],
      },
      {
        key: "babamul.ztf.ztfmatch",
        label: "ZTF — with LSST match",
        children: [
          { key: "babamul.ztf.lsstmatch.stellar", desc: "Alerts classified as stellar" },
          { key: "babamul.ztf.lsstmatch.hosted", desc: "Alerts with a host galaxy" },
          { key: "babamul.ztf.lsstmatch.hostless", desc: "Alerts without a host galaxy" },
          { key: "babamul.ztf.lsstmatch.unknown", desc: "Unclassified alerts" },
        ],
      },
    ],
  },
];

function generatePython(topics: string[], groupId: string, offset: string, autoCommit: boolean, usernameVar = "<EMAIL>", passwordVar = "<PASSWORD>") {
  const topicsList = topics.map(t => `"${t}"`).join(', ');
  return `from confluent_kafka import Consumer

# Configuration
conf = {
    "bootstrap.servers": "kafka.boom.example.com:9092",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "${usernameVar}",
    "sasl.password": "${passwordVar}",
    "group.id": "${usernameVar}-${groupId}",
    "auto.offset.reset": "${offset}",
    "enable.auto.commit": ${autoCommit}
}

consumer = Consumer(conf)
consumer.subscribe([${topicsList}])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        print(msg.value())
finally:
    consumer.close()
`;
}

function generateRust(topics: string[], groupId: string, offset: string, autoCommit: boolean, usernameVar = "<EMAIL>", passwordVar = "<PASSWORD>") {
  const topicsList = topics.map(t => `"${t}"`).join(', ');
  const autoCommitCfg = autoCommit ? 'true' : 'false';
  return `use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;

fn main() {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "kafka.boom.example.com:9092")
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

export default function BabamulDocs() {
  const profile = useAppStore(s => s.profile);
  const usernamePrefill = profile?.username ?? "<EMAIL>";

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

  function toggleCollapsed(key: string) {
    setCollapsed(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }
  const [groupId, setGroupId] = useState<string>("");
  const [offset, setOffset] = useState<string>("earliest");
  const [autoCommit, setAutoCommit] = useState<boolean>(true);
  const [lang, setLang] = useState<'python'|'rust'>('python');

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
    const user = usernamePrefill;
    return lang === 'python'
      ? generatePython(effectiveSelected, groupId, offset, autoCommit, user, '<PASSWORD>')
      : generateRust(effectiveSelected, groupId, offset, autoCommit, user, '<PASSWORD>');
  }, [effectiveSelected, groupId, offset, autoCommit, lang, usernamePrefill]);

  // topic toggling handled inline via Checkbox `onCheckedChange`

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
    navigator.clipboard.writeText(generated).then(() => alert('Copied to clipboard'));
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
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-lg font-medium">Babamul — Kafka access guide</h2>
            <p className="text-sm text-muted-foreground">Your Babamul Kafka username is the same email you use to sign in to this web application. Use the activation password given at account activation for SCRAM authentication.</p>
          </div>
          <div className="flex items-center gap-3">
            <div className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-primary text-white font-medium select-none">{step+1}/3</div>
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
              <CardDescription>Choose one or more Babamul topics to read from.</CardDescription>
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
              <CardTitle>2 — Consumer settings</CardTitle>
              <CardDescription>Configure your Kafka consumer behavior and group id.</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4">
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
                <div className="text-sm text-muted-foreground">Username will be your account's username: <span className="font-medium">{usernamePrefill}</span></div>
              </div>
              <div className="text-sm text-muted-foreground mb-2">Selected topics ({effectiveSelected.length}): <span className="font-mono text-xs">{effectiveSelected.join(', ')}</span></div>
              <pre className="p-4 rounded bg-muted text-sm overflow-auto"><code>{generated}</code></pre>
              <div className="flex gap-2 mt-3">
                <Button onClick={copyGenerated}>Copy code</Button>
              </div>

              <div className="mt-6">
                <h4 className="font-semibold">Setup</h4>
                {lang === 'python' ? (
                  <div className="mt-2">
                    <p className="text-sm">Create a virtualenv and install the dependency:</p>
                    <pre className="p-2 rounded bg-muted text-sm">python -m venv .venv
source .venv/bin/activate
pip install confluent-kafka</pre>
                  </div>
                ) : (
                  <div className="mt-2">
                    <p className="text-sm">Add the Kafka client dependency to your project:</p>
                    <pre className="p-2 rounded bg-muted text-sm">cargo add rdkafka --features="gssapi sasl ssl tracing"</pre>
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
