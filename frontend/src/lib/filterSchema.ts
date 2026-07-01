/**
 * Filter data model types — mirrors the block/condition tree used by
 * Fritz/SkyPortal so filters are portable between the two systems.
 */

/** A single condition: field + operator + value */
export interface FilterCondition {
  id: string;
  category: "condition";
  field: string;
  operator: string;          // MongoDB operator e.g. "$gt", "$lt", "$eq"
  value: string | number | boolean;
}

/** A logical block containing children joined by AND / OR */
export interface FilterBlock {
  id: string;
  category: "block";
  operator: "and" | "or";
  children: FilterNode[];
}

/** A node in the filter tree is either a block or a condition */
export type FilterNode = FilterBlock | FilterCondition;

/** Flattened field option derived from the Avro schema */
export interface FieldOption {
  label: string;             // dot-path e.g. "candidate.drb"
  type: "number" | "string" | "boolean" | "array";
  group: string;             // grouping for the dropdown e.g. "Candidate"
}

/** MongoDB operator metadata */
export interface OperatorDef {
  label: string;             // human-readable e.g. ">"
  mongoOp: string;           // e.g. "$gt"
  applicableTo: ("number" | "string" | "boolean")[];
}

// ─── Helpers ──────────────────────────────────────────────────────

let _nextId = 0;

export function generateId(prefix = "node"): string {
  _nextId += 1;
  return `${prefix}-${Date.now()}-${_nextId}`;
}

export function createEmptyCondition(): FilterCondition {
  return {
    id: generateId("cond"),
    category: "condition",
    field: "",
    operator: "$gt",
    value: "",
  };
}

export function createEmptyBlock(op: "and" | "or" = "and"): FilterBlock {
  return {
    id: generateId("block"),
    category: "block",
    operator: op,
    children: [createEmptyCondition()],
  };
}

export function createDefaultFilter(): FilterBlock[] {
  return [
    {
      id: "root-block",
      category: "block",
      operator: "and",
      children: [createEmptyCondition()],
    },
  ];
}
