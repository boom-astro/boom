import { useState, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { ChevronRight, ChevronDown, Copy, Check, Search } from "lucide-react";
import type { AvroSchema } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card.tsx";
import { Input } from "@/components/ui/input.tsx";

function avroTypeLabel(type: unknown): string {
  if (typeof type === "string") return type;
  if (Array.isArray(type)) return type.map(avroTypeLabel).join(" | ");
  if (typeof type === "object" && type !== null) {
    const t = type as AvroSchema;
    if (t.type === "array") return `array<${avroTypeLabel(t.items)}>`;
    if (t.type === "map") return `map<${avroTypeLabel(t.values)}>`;
    if (t.type === "record") return t.name || "record";
    if (t.type === "enum") return t.name || "enum";
    if (t.type === "fixed") return `fixed(${t.size})`;
    return t.type || "unknown";
  }
  return "unknown";
}

function getNestedRecords(type: unknown): AvroSchema[] {
  if (Array.isArray(type)) return type.flatMap(getNestedRecords);
  if (typeof type === "object" && type !== null) {
    const t = type as AvroSchema;
    if (t.type === "record" && t.fields) return [t];
    if (t.type === "array") return getNestedRecords(t.items);
    if (t.type === "map") return getNestedRecords(t.values);
  }
  return [];
}

function fieldMatches(field: AvroSchema, query: string): boolean {
  if (field.name?.toLowerCase().includes(query)) return true;
  for (const rec of getNestedRecords(field.type)) {
    if (rec.fields?.some((f: AvroSchema) => fieldMatches(f, query))) return true;
  }
  return false;
}

export function SchemaFields({ schema, filter, field_descriptions }: {
  schema: AvroSchema;
  filter?: string;
  field_descriptions?: Record<string, ReactNode>;
}) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const query = filter?.toLowerCase().trim() || "";

  if (!schema.fields || !Array.isArray(schema.fields)) return null;

  return (
    <div className="space-y-0.5">
      {schema.fields.map((field: AvroSchema) => {
        const nested = getNestedRecords(field.type);
        const hasChildren = nested.length > 0;
        const matches = !query || fieldMatches(field, query);
        if (query && !matches) return null;
        const nameMatch = query && field.name?.toLowerCase().includes(query);
        const childMatch = query && !nameMatch;
        const isOpen = childMatch || (expanded[field.name] ?? false);
        const description = field_descriptions?.[field.name];
        return (
          <div key={field.name}>
            <div
              className={`flex items-center gap-2 py-1 px-2 rounded text-sm ${hasChildren ? "cursor-pointer hover:bg-muted/50" : ""} ${nameMatch ? "bg-accent" : ""}`}
              onClick={hasChildren ? () => setExpanded((p) => ({ ...p, [field.name]: !p[field.name] })) : undefined}
            >
              {hasChildren ? (
                isOpen ? <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" /> : <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
              ) : (
                <span className="w-3.5 shrink-0" />
              )}
              <code className="text-xs font-semibold">{field.name}</code>
              <span className="text-xs text-muted-foreground">{avroTypeLabel(field.type)}</span>
            </div>
            {description && (
              <p className="text-[11px] text-muted-foreground ml-6.5 px-2 pb-1">{description}</p>
            )}
            {hasChildren && isOpen && nested.map((rec) => (
              <div key={rec.name} className="ml-6 pl-3 border-l border-border">
                <SchemaFields schema={rec} filter={filter} field_descriptions={field_descriptions} />
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}

export function SchemaViewer({ survey, schema, field_descriptions, copy_button=true }: {
  survey: string;
  schema: AvroSchema;
  field_descriptions?: Record<string, ReactNode>;
  copy_button?: boolean;
}) {
  const [search, setSearch] = useState("");
  const [copied, setCopied] = useState(false);
  const text = JSON.stringify(schema, null, 2);

  return (
    <Card>
      <CardHeader className="flex justify-between">
        <div>
          <CardTitle>{survey.toUpperCase()}</CardTitle>
          <CardDescription>Avro schema for babamul.{survey} topics</CardDescription>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
            <Input
              placeholder="Search fields..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="h-8 w-48 pl-7 text-xs"
            />
          </div>
          {copy_button &&
            <Button
              variant="ghost" size="icon" className="h-7 w-7"
              onClick={() => {
                navigator.clipboard.writeText(text);
                setCopied(true);
                setTimeout(() => setCopied(false), 1500);
              }}
            >
              {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
            </Button>
          }
        </div>
      </CardHeader>
      <CardContent>
        <SchemaFields schema={schema} filter={search} field_descriptions={field_descriptions} />
      </CardContent>
    </Card>
  );
}
