/**
 * Converts a block/condition filter tree into a MongoDB aggregation pipeline.
 * Ported from Fritz's mongoPipelineBuilder.js.
 */

import type { FilterNode, FilterBlock, FilterCondition } from "./filterSchema";

/**
 * Convert a single condition into a MongoDB match expression.
 */
function conditionToMongo(cond: FilterCondition): Record<string, unknown> | null {
  if (!cond.field || !cond.operator) return null;

  // Parse value to the correct type
  let val: unknown = cond.value;
  if (typeof val === "string") {
    const trimmed = val.trim();
    if (trimmed === "true") val = true;
    else if (trimmed === "false") val = false;
    else if (trimmed !== "" && !isNaN(Number(trimmed))) val = Number(trimmed);
    else val = trimmed;
  }

  // Handle $exists specially (value is boolean)
  if (cond.operator === "$exists") {
    return { [cond.field]: { $exists: val !== false && val !== "false" && val !== 0 } };
  }

  // Handle $in specially (value should be comma-separated)
  if (cond.operator === "$in") {
    const items = String(cond.value)
      .split(",")
      .map((s) => {
        const t = s.trim();
        return !isNaN(Number(t)) ? Number(t) : t;
      });
    return { [cond.field]: { $in: items } };
  }

  // Handle $eq: MongoDB shorthand is just { field: value }
  if (cond.operator === "$eq") {
    return { [cond.field]: val };
  }

  // General case: { field: { $op: value } }
  return { [cond.field]: { [cond.operator]: val } };
}

/**
 * Convert a block (AND/OR with children) into a MongoDB match expression.
 */
function blockToMongo(block: FilterBlock): Record<string, unknown> | null {
  const childExprs: Record<string, unknown>[] = [];

  for (const child of block.children) {
    if (child.category === "condition") {
      const expr = conditionToMongo(child);
      if (expr) childExprs.push(expr);
    } else if (child.category === "block") {
      const expr = blockToMongo(child);
      if (expr) childExprs.push(expr);
    }
  }

  if (childExprs.length === 0) return null;

  // Single expression in AND block doesn't need $and wrapper
  if (childExprs.length === 1 && block.operator === "and") {
    return childExprs[0];
  }

  // For AND: if all expressions have unique top-level keys, merge them
  // instead of using $and (produces cleaner MongoDB)
  if (block.operator === "and") {
    const allKeys = childExprs.flatMap((e) => Object.keys(e));
    const uniqueKeys = new Set(allKeys);
    if (uniqueKeys.size === allKeys.length) {
      // All keys are unique, we can merge
      return Object.assign({}, ...childExprs);
    }
    // Duplicate keys present, must use $and
    return { $and: childExprs };
  }

  // OR block
  return { $or: childExprs };
}

/**
 * Convert a filter tree (array of root blocks) into a full MongoDB
 * aggregation pipeline: [$match, ..., $project].
 *
 * The $project stage always includes objectId: 1.
 * Additional projection fields can be passed in.
 */
export function convertToMongoPipeline(
  roots: FilterNode[],
  projectionFields?: string[],
): Record<string, unknown>[] {
  // Build the $match stage from all root blocks
  const matchExprs: Record<string, unknown>[] = [];

  for (const root of roots) {
    if (root.category === "block") {
      const expr = blockToMongo(root);
      if (expr) matchExprs.push(expr);
    } else if (root.category === "condition") {
      const expr = conditionToMongo(root);
      if (expr) matchExprs.push(expr);
    }
  }

  if (matchExprs.length === 0) {
    return [];
  }

  // Merge match expressions
  let matchStage: Record<string, unknown>;
  if (matchExprs.length === 1) {
    matchStage = matchExprs[0];
  } else {
    // Check if we can merge (no duplicate top-level keys)
    const allKeys = matchExprs.flatMap((e) => Object.keys(e));
    const uniqueKeys = new Set(allKeys);
    if (uniqueKeys.size === allKeys.length) {
      matchStage = Object.assign({}, ...matchExprs);
    } else {
      matchStage = { $and: matchExprs };
    }
  }

  // Build the $project stage
  const project: Record<string, unknown> = {
    objectId: 1,
    "candidate.magpsf": 1,
    "candidate.ra": 1,
    "candidate.dec": 1,
    "candidate.jd": 1,
    "candidate.drb": 1,
    classifications: 1,
    properties: 1,
    coordinates: 1,
  };

  if (projectionFields) {
    for (const field of projectionFields) {
      if (field && field !== "objectId") {
        project[field] = 1;
      }
    }
  }

  return [{ $match: matchStage }, { $project: project }];
}

/**
 * Format a MongoDB pipeline as a pretty-printed JSON string.
 */
export function formatPipeline(pipeline: Record<string, unknown>[]): string {
  return JSON.stringify(pipeline, null, 2);
}

/**
 * Validate that a pipeline has at least a $match and ends with $project
 * containing objectId: 1.
 */
export function isValidPipeline(pipeline: Record<string, unknown>[]): boolean {
  if (!Array.isArray(pipeline) || pipeline.length === 0) return false;

  const hasMatch = pipeline.some((s) => "$match" in s);
  if (!hasMatch) return false;

  const last = pipeline[pipeline.length - 1];
  if (!("$project" in last)) return false;

  const proj = last.$project as Record<string, unknown> | undefined;
  return proj?.objectId === 1;
}
