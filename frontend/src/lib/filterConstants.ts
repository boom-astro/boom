/**
 * MongoDB operator definitions and Avro-schema field flattener.
 * Ported from Fritz's filterConstants.js.
 */

import type { FieldOption, OperatorDef } from "./filterSchema";

// ─── Operators ────────────────────────────────────────────────────

export const OPERATORS: OperatorDef[] = [
  { label: "=",  mongoOp: "$eq",     applicableTo: ["number", "string", "boolean"] },
  { label: "≠",  mongoOp: "$ne",     applicableTo: ["number", "string", "boolean"] },
  { label: ">",  mongoOp: "$gt",     applicableTo: ["number"] },
  { label: "≥",  mongoOp: "$gte",    applicableTo: ["number"] },
  { label: "<",  mongoOp: "$lt",     applicableTo: ["number"] },
  { label: "≤",  mongoOp: "$lte",    applicableTo: ["number"] },
  { label: "in", mongoOp: "$in",     applicableTo: ["number", "string"] },
  { label: "exists", mongoOp: "$exists", applicableTo: ["number", "string", "boolean"] },
];

export function getOperatorsForType(type: string): OperatorDef[] {
  return OPERATORS.filter((op) => op.applicableTo.includes(type as "number" | "string" | "boolean"));
}

// ─── Avro schema → flat field list ────────────────────────────────

function getSimpleType(avroType: unknown): "number" | "string" | "boolean" | "array" {
  if (typeof avroType === "string") {
    switch (avroType) {
      case "double":
      case "float":
      case "int":
      case "long":
        return "number";
      case "boolean":
        return "boolean";
      default:
        return "string";
    }
  }
  if (Array.isArray(avroType)) {
    const nonNull = avroType.find((t: unknown) => t !== "null");
    return nonNull ? getSimpleType(nonNull) : "string";
  }
  if (typeof avroType === "object" && avroType !== null) {
    const obj = avroType as Record<string, unknown>;
    if (obj.type === "array") return "array";
    if (obj.type === "record") return "string"; // treat nested records as opaque
    return getSimpleType(obj.type);
  }
  return "string";
}

interface AvroField {
  name: string;
  type: unknown;
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/**
 * Flatten an Avro schema into a flat list of field options with
 * dot-path labels (e.g. "candidate.ra") and simplified types.
 */
export function flattenAvroSchema(schema: { fields?: AvroField[] }): FieldOption[] {
  const result: FieldOption[] = [];

  function process(field: AvroField, parentPath: string) {
    const path = parentPath ? `${parentPath}.${field.name}` : field.name;
    const group = parentPath ? capitalize(parentPath.split(".")[0]) : "Other Fields";

    let actualType: unknown = field.type;

    // Unwrap union types (e.g. ["null", "double"])
    if (Array.isArray(actualType)) {
      actualType = actualType.find((t: unknown) => t !== "null") ?? actualType[0];
    }

    // Recurse into records
    if (
      typeof actualType === "object" &&
      actualType !== null &&
      (actualType as Record<string, unknown>).type === "record" &&
      (actualType as Record<string, unknown>).fields
    ) {
      const fields = (actualType as Record<string, unknown>).fields as AvroField[];
      for (const nested of fields) {
        process(nested, path);
      }
      return;
    }

    // Skip arrays for now (they need special handling)
    const simpleType = getSimpleType(actualType);
    if (simpleType === "array") return;

    result.push({ label: path, type: simpleType, group });
  }

  if (schema?.fields) {
    for (const field of schema.fields) {
      process(field, "");
    }
  }

  return result;
}
