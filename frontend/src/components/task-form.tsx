// A submission form rendered from a task type's JSON Schema.
//
// The schema comes from the params struct's `ToSchema` derive, so the fields
// offered here are exactly the fields the API accepts, and the help text is the
// doc comment written on each one. Adding a parameter to a task puts it on this
// form with no frontend change.
import { useMemo, useState } from "react";
import type { SchemaField, TaskType } from "@/lib/adminApi";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";

/** The base type of a field, ignoring the "null" that marks it optional. */
export function baseType(field: SchemaField): string {
  const t = field.type;
  if (Array.isArray(t)) return t.find((x) => x !== "null") ?? "string";
  return t ?? "string";
}

function isRequired(schema: TaskType["params_schema"], name: string): boolean {
  return (schema.required ?? []).includes(name);
}

/**
 * Coerce a form value to what the API expects.
 *
 * Returns `undefined` for a field left blank, and the caller omits it — so the
 * server applies the default from the params struct rather than the form
 * inventing one. An empty numeric input must not become 0, which for most of
 * these fields is a value `validate_params` rejects.
 */
export function coerce(field: SchemaField, raw: string | boolean): unknown {
  if (typeof raw === "boolean") return raw;
  const text = raw.trim();
  if (text === "") return undefined;
  switch (baseType(field)) {
    case "integer":
      return Number.parseInt(text, 10);
    case "number":
      return Number.parseFloat(text);
    case "array":
      // Comma-separated, which is how a catalog list reads most naturally.
      return text
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    default:
      return text;
  }
}

export function TaskForm({
  task,
  onSubmit,
  onCancel,
  busy,
}: {
  task: TaskType;
  onSubmit: (params: Record<string, unknown>) => void;
  onCancel: () => void;
  busy: boolean;
}) {
  const fields = useMemo(
    () => Object.entries(task.params_schema.properties ?? {}),
    [task],
  );
  const [values, setValues] = useState<Record<string, string | boolean>>({});
  const [confirmed, setConfirmed] = useState(false);

  // A task whose parameters cannot be expressed as flat fields — a tagged union
  // like enrich_reprocess's selection — is submitted as JSON rather than
  // guessed at with a half-working widget.
  const nested = fields.filter(([, f]) => f.oneOf || baseType(f) === "object");
  const [rawJson, setRawJson] = useState("{}");
  const [jsonError, setJsonError] = useState<string | null>(null);
  const useRawJson = nested.length > 0;

  // Structured tasks are entered as one JSON value, so their required fields
  // do not live in the flat form state. Validation for those happens when the
  // JSON is parsed and again on the API; keeping this flat-field check would
  // make every such form permanently disabled.
  const missing = useRawJson
    ? []
    : (task.params_schema.required ?? []).filter((name) => {
        const v = values[name];
        return v === undefined || (typeof v === "string" && v.trim() === "");
      });

  function submit() {
    if (useRawJson) {
      try {
        const parsed = JSON.parse(rawJson);
        setJsonError(null);
        onSubmit(parsed);
      } catch (e) {
        setJsonError(e instanceof Error ? e.message : String(e));
      }
      return;
    }
    const params: Record<string, unknown> = {};
    for (const [name, field] of fields) {
      const raw = values[name];
      if (raw === undefined) continue;
      const value = coerce(field, raw);
      // Omitted rather than sent as null: the server fills in the default.
      if (value !== undefined) params[name] = value;
    }
    onSubmit(params);
  }

  return (
    <div className="border rounded-lg p-4 mb-6">
      <h3 className="font-semibold">{task.title}</h3>
      <p className="text-sm text-muted-foreground mb-1">{task.description}</p>
      {task.destructive && (
        <p className="text-sm text-destructive mb-3">
          This task can destroy data. Check what it will do before running it.
        </p>
      )}

      {useRawJson ? (
        <div className="mb-3">
          <Label htmlFor="raw-params" className="text-xs">
            Parameters (JSON)
          </Label>
          <p className="text-xs text-muted-foreground mb-1">
            This task takes structured parameters, so they are entered directly.
            See the field list in <code>/docs</code>.
          </p>
          <textarea
            id="raw-params"
            className="w-full rounded border bg-background p-2 font-mono text-xs"
            rows={6}
            value={rawJson}
            onChange={(e) => setRawJson(e.target.value)}
            spellCheck={false}
          />
          {jsonError && <p className="text-xs text-destructive mt-1">{jsonError}</p>}
        </div>
      ) : (
        <div className="grid gap-3 mb-3 sm:grid-cols-2">
          {fields.map(([name, field]) => {
            const required = isRequired(task.params_schema, name);
            const type = baseType(field);
            return (
              <div key={name} className={type === "boolean" ? "sm:col-span-2" : ""}>
                <Label htmlFor={name} className="text-xs">
                  {name}
                  {required && <span className="text-destructive"> *</span>}
                </Label>
                {type === "boolean" ? (
                  <div className="flex items-center gap-2 mt-1">
                    <Checkbox
                      id={name}
                      checked={values[name] === true}
                      onCheckedChange={(checked) =>
                        setValues((v) => ({ ...v, [name]: checked === true }))
                      }
                    />
                    <span className="text-xs text-muted-foreground">
                      {field.description}
                    </span>
                  </div>
                ) : (
                  <>
                    {field.enum ? (
                      <select
                        id={name}
                        className="w-full rounded border bg-background p-2 text-sm"
                        value={String(values[name] ?? "")}
                        onChange={(e) =>
                          setValues((v) => ({ ...v, [name]: e.target.value }))
                        }
                      >
                        <option value="">
                          {required ? "select…" : "(default)"}
                        </option>
                        {field.enum.map((choice) => (
                          <option key={choice} value={choice}>
                            {choice}
                          </option>
                        ))}
                      </select>
                    ) : (
                      <Input
                        id={name}
                        type={type === "integer" || type === "number" ? "number" : "text"}
                        value={String(values[name] ?? "")}
                        placeholder={required ? "" : "(default)"}
                        onChange={(e) =>
                          setValues((v) => ({ ...v, [name]: e.target.value }))
                        }
                      />
                    )}
                    {field.description && (
                      <p className="text-xs text-muted-foreground mt-1">
                        {field.description}
                      </p>
                    )}
                  </>
                )}
              </div>
            );
          })}
        </div>
      )}

      {task.destructive && (
        <div className="flex items-center gap-2 mb-3">
          <Checkbox
            id="confirm-destructive"
            checked={confirmed}
            onCheckedChange={(checked) => setConfirmed(checked === true)}
          />
          <Label htmlFor="confirm-destructive" className="text-xs">
            I understand this can destroy data
          </Label>
        </div>
      )}

      <div className="flex items-center gap-2">
        <Button
          size="sm"
          disabled={busy || missing.length > 0 || (task.destructive && !confirmed)}
          onClick={submit}
        >
          {busy ? "Starting…" : "Run task"}
        </Button>
        <Button variant="outline" size="sm" onClick={onCancel}>
          Cancel
        </Button>
        {missing.length > 0 && (
          <span className="text-xs text-muted-foreground">
            required: {missing.join(", ")}
          </span>
        )}
      </div>
    </div>
  );
}
