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

/**
 * Unwrap the `oneOf` that some optional fields arrive as.
 *
 * utoipa writes `Option<T>` two ways depending on what `T` is: a type pair like
 * `["integer", "null"]`, or a two-arm `oneOf`. Only the first is a flat field
 * on its face, so taking any `oneOf` as a tagged union sent the whole form to
 * the JSON escape hatch over one optional parameter — and with it every other
 * field on that task.
 *
 * The outer description is kept: the doc comment is written on the field, and
 * the arm it resolves to carries none.
 */
export function unwrapOptional(field: SchemaField): SchemaField {
  if (!field.oneOf) return field;
  const arms = field.oneOf.filter((arm) => baseType(arm) !== "null" || arm.$ref);
  if (arms.length !== 1) return field;
  return { description: field.description, ...arms[0] };
}

/**
 * Whether a field has to be entered as JSON rather than as one input.
 *
 * A struct or enum parameter arrives as a `$ref` into a definitions section
 * that `schema_of` does not emit, so there is nothing to render and nothing to
 * follow. Such a field used to render as a text box, because `$ref` carries no
 * `type` and the fallback is "string" — which quietly accepted a string for a
 * parameter that is not one, and meant the escape hatch this check exists for
 * never opened.
 */
export function isStructured(field: SchemaField): boolean {
  const resolved = unwrapOptional(field);
  if (resolved.$ref || resolved.oneOf) return true;
  return baseType(resolved) === "object" || !!resolved.properties;
}

function isRequired(schema: TaskType["params_schema"], name: string): boolean {
  return (schema.required ?? []).includes(name);
}

/** A form value coerced for the API, or the reason it could not be. */
export type Parsed = { value?: unknown; error?: string };

/**
 * Coerce a form value to what the API expects, or say why it cannot be.
 *
 * A blank field comes back with neither, and the caller omits it — so the
 * server applies the default from the params struct rather than the form
 * inventing one. An empty numeric input must not become 0, which for most of
 * these fields is a value `validate_params` rejects.
 *
 * Numeric text that is not the number it looks like is an error rather than an
 * omission, and the difference matters: `parseInt` took the leading digits and
 * discarded the rest, so `1e9` became 1, `2.5` became 2 and `1,000` became 1,
 * all of them passing a check for `NaN`. An ingest asked for with a batch size
 * of `1e9` ran with a batch size of 1 and reported nothing wrong.
 *
 * The bounds in the schema are applied here too. They come from the same struct
 * the server validates against, so a value outside them is a 400 waiting to
 * happen, and saying so next to the field beats saying it after a round trip.
 */
export function parseField(field: SchemaField, raw: string | boolean): Parsed {
  if (typeof raw === "boolean") return { value: raw };
  const text = raw.trim();
  if (text === "") return {};
  const resolved = unwrapOptional(field);
  const type = baseType(resolved);

  switch (type) {
    case "integer":
    case "number": {
      // `Number` rejects what it cannot represent exactly, where `parseInt`
      // and `parseFloat` take a prefix and silently drop the remainder.
      const parsed = Number(text);
      if (!Number.isFinite(parsed)) return { error: "not a number" };
      if (type === "integer" && !Number.isInteger(parsed)) {
        return { error: "whole numbers only" };
      }
      if (resolved.minimum !== undefined && parsed < resolved.minimum) {
        return { error: `at least ${resolved.minimum}` };
      }
      if (resolved.maximum !== undefined && parsed > resolved.maximum) {
        return { error: `at most ${resolved.maximum}` };
      }
      return { value: parsed };
    }
    case "array": {
      // Comma-separated, which is how a catalog list reads most naturally.
      const entries = text
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      const items = resolved.items;
      // A list of numbers is coerced per entry rather than sent as strings,
      // which serde would reject for the whole list on the first one.
      if (!items || !["integer", "number"].includes(baseType(items))) {
        return { value: entries };
      }
      const values: unknown[] = [];
      for (const entry of entries) {
        const parsed = parseField(items, entry);
        if (parsed.error) return { error: `${entry}: ${parsed.error}` };
        values.push(parsed.value);
      }
      return { value: values };
    }
    default:
      return { value: text };
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
  // like a selection, or a struct the schema only references — is submitted as
  // JSON rather than guessed at with a half-working widget.
  const [rawJson, setRawJson] = useState("{}");
  const [jsonError, setJsonError] = useState<string | null>(null);
  const useRawJson = fields.some(([, field]) => isStructured(field));

  // Parsed once per render and read everywhere below, so what the button
  // decides from and what the request carries cannot disagree. Reading the raw
  // text instead is how "-" counted as a filled-in required field: it enabled
  // Run and was then dropped from the payload.
  const parsed = useMemo(
    () =>
      fields.map(
        ([name, field]) =>
          [name, field, parseField(field, values[name] ?? "")] as const,
      ),
    [fields, values],
  );
  const invalid = parsed.filter(([, , result]) => result.error);

  // Structured tasks are entered as one JSON value, so their required fields
  // do not live in the flat form state. Validation for those happens when the
  // JSON is parsed and again on the API; keeping this flat-field check would
  // make every such form permanently disabled.
  const missing = useRawJson
    ? []
    : (task.params_schema.required ?? []).filter((name) => {
        const entry = parsed.find(([field]) => field === name);
        return !entry || entry[2].value === undefined;
      });

  function submit() {
    if (useRawJson) {
      let value: unknown;
      try {
        value = JSON.parse(rawJson);
      } catch (e) {
        setJsonError(e instanceof Error ? e.message : String(e));
        return;
      }
      // `JSON.parse` is happy with `7` or `null`, which reach the API as a
      // body it cannot deserialize into a params struct at all.
      if (value === null || typeof value !== "object" || Array.isArray(value)) {
        setJsonError("parameters have to be a JSON object");
        return;
      }
      setJsonError(null);
      onSubmit(value as Record<string, unknown>);
      return;
    }
    const params: Record<string, unknown> = {};
    for (const [name, , result] of parsed) {
      // Omitted rather than sent as null: the server fills in the default.
      if (result.value !== undefined) params[name] = result.value;
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
            onChange={(e) => {
              setRawJson(e.target.value);
              // Cleared on edit: the old message described text that is no
              // longer there, and pointed at a column that had moved.
              setJsonError(null);
            }}
            spellCheck={false}
          />
          {jsonError && <p className="text-xs text-destructive mt-1">{jsonError}</p>}
        </div>
      ) : (
        <div className="grid gap-3 mb-3 sm:grid-cols-2">
          {parsed.map(([name, field, result]) => {
            const required = isRequired(task.params_schema, name);
            const resolved = unwrapOptional(field);
            const type = baseType(resolved);
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
                      {resolved.description}
                    </span>
                  </div>
                ) : (
                  <>
                    {resolved.enum ? (
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
                        {resolved.enum.map((choice) => (
                          <option key={choice} value={choice}>
                            {choice}
                          </option>
                        ))}
                      </select>
                    ) : (
                      <Input
                        id={name}
                        type={type === "integer" || type === "number" ? "number" : "text"}
                        // The same bounds the parse applies, so the number box
                        // offers its own stepper and warning for them.
                        min={resolved.minimum}
                        max={resolved.maximum}
                        step={type === "integer" ? 1 : undefined}
                        value={String(values[name] ?? "")}
                        placeholder={required ? "" : "(default)"}
                        aria-invalid={result.error ? true : undefined}
                        aria-describedby={result.error ? `${name}-error` : undefined}
                        onChange={(e) =>
                          setValues((v) => ({ ...v, [name]: e.target.value }))
                        }
                      />
                    )}
                    {result.error ? (
                      <p id={`${name}-error`} className="text-xs text-destructive mt-1">
                        {result.error}
                      </p>
                    ) : (
                      resolved.description && (
                        <p className="text-xs text-muted-foreground mt-1">
                          {resolved.description}
                        </p>
                      )
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
          disabled={
            busy ||
            missing.length > 0 ||
            invalid.length > 0 ||
            (task.destructive && !confirmed)
          }
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
        {missing.length === 0 && invalid.length > 0 && (
          <span className="text-xs text-destructive">
            fix: {invalid.map(([name]) => name).join(", ")}
          </span>
        )}
      </div>
    </div>
  );
}
