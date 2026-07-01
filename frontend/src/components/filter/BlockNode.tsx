/**
 * BlockNode — a logical block (AND / OR) containing child conditions and nested blocks.
 */

import { Button } from "@/components/ui/button";
import { Plus, GitBranch, Trash2 } from "lucide-react";
import type { FilterBlock, FilterNode, FieldOption } from "@/lib/filterSchema";
import { createEmptyCondition, createEmptyBlock } from "@/lib/filterSchema";
import { ConditionRow } from "./ConditionRow";

interface BlockNodeProps {
  block: FilterBlock;
  fieldOptions: FieldOption[];
  onChange: (updated: FilterBlock) => void;
  onRemove?: () => void;
  isRoot?: boolean;
  depth?: number;
}

export function BlockNode({ block, fieldOptions, onChange, onRemove, isRoot = false, depth = 0 }: BlockNodeProps) {
  // Toggle block operator
  const toggleOperator = () => {
    onChange({ ...block, operator: block.operator === "and" ? "or" : "and" });
  };

  // Update a specific child
  const updateChild = (index: number, updated: FilterNode) => {
    const newChildren = [...block.children];
    newChildren[index] = updated;
    onChange({ ...block, children: newChildren });
  };

  // Remove a child
  const removeChild = (index: number) => {
    const newChildren = block.children.filter((_, i) => i !== index);
    // If no children left, add an empty condition
    if (newChildren.length === 0) {
      newChildren.push(createEmptyCondition());
    }
    onChange({ ...block, children: newChildren });
  };

  // Add a new condition
  const addCondition = () => {
    onChange({ ...block, children: [...block.children, createEmptyCondition()] });
  };

  // Add a nested block
  const addNestedBlock = () => {
    const nestedOp = block.operator === "and" ? "or" : "and";
    onChange({ ...block, children: [...block.children, createEmptyBlock(nestedOp)] });
  };

  const borderColor = block.operator === "and"
    ? "border-blue-500/30 bg-blue-500/5"
    : "border-amber-500/30 bg-amber-500/5";

  const badgeColor = block.operator === "and"
    ? "bg-blue-500/15 text-blue-700 dark:text-blue-400 hover:bg-blue-500/25"
    : "bg-amber-500/15 text-amber-700 dark:text-amber-400 hover:bg-amber-500/25";

  return (
    <div className={`border-l-2 ${borderColor} rounded-md pl-3 py-2 pr-2 space-y-2`}>
      {/* Block header */}
      <div className="flex items-center gap-2">
        <button
          onClick={toggleOperator}
          className={`px-2 py-0.5 rounded text-[11px] font-semibold uppercase tracking-wide cursor-pointer transition-colors ${badgeColor}`}
        >
          {block.operator}
        </button>

        {!isRoot && onRemove && (
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 text-muted-foreground hover:text-destructive ml-auto"
            onClick={onRemove}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        )}
      </div>

      {/* Children */}
      <div className="space-y-2">
        {block.children.map((child, i) => {
          if (child.category === "condition") {
            return (
              <ConditionRow
                key={child.id}
                condition={child}
                fieldOptions={fieldOptions}
                onChange={(updated) => updateChild(i, updated)}
                onRemove={() => removeChild(i)}
                removable={block.children.length > 1}
              />
            );
          }
          if (child.category === "block") {
            return (
              <BlockNode
                key={child.id}
                block={child}
                fieldOptions={fieldOptions}
                onChange={(updated) => updateChild(i, updated)}
                onRemove={() => removeChild(i)}
                depth={depth + 1}
              />
            );
          }
          return null;
        })}
      </div>

      {/* Add buttons */}
      <div className="flex gap-2 pt-1">
        <Button variant="ghost" size="sm" className="h-7 text-xs text-muted-foreground" onClick={addCondition}>
          <Plus className="h-3 w-3 mr-1" /> Condition
        </Button>
        {depth < 3 && (
          <Button variant="ghost" size="sm" className="h-7 text-xs text-muted-foreground" onClick={addNestedBlock}>
            <GitBranch className="h-3 w-3 mr-1" /> {block.operator === "and" ? "OR" : "AND"} Group
          </Button>
        )}
      </div>
    </div>
  );
}
