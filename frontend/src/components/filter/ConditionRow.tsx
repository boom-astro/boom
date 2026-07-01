/**
 * ConditionRow — a single filter condition: field dropdown + operator + value.
 * Uses shadcn/ui components to match the rest of babamul-web.
 */

import { Select, SelectTrigger, SelectContent, SelectItem, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { X } from "lucide-react";
import type { FilterCondition, FieldOption } from "@/lib/filterSchema";
import { OPERATORS } from "@/lib/filterConstants";

interface ConditionRowProps {
  condition: FilterCondition;
  fieldOptions: FieldOption[];
  onChange: (updated: FilterCondition) => void;
  onRemove: () => void;
  removable: boolean;
}

export function ConditionRow({ condition, fieldOptions, onChange, onRemove, removable }: ConditionRowProps) {
  // Group field options by their group name
  const grouped = new Map<string, FieldOption[]>();
  for (const opt of fieldOptions) {
    const group = opt.group;
    if (!grouped.has(group)) grouped.set(group, []);
    grouped.get(group)!.push(opt);
  }

  // Find the currently selected field's type to filter operators
  const selectedField = fieldOptions.find((f) => f.label === condition.field);
  const fieldType = selectedField?.type ?? "number";
  const availableOps = OPERATORS.filter((op) => op.applicableTo.includes(fieldType as "number" | "string" | "boolean"));

  return (
    <div className="flex items-center gap-2 group">
      {/* Field selector */}
      <Select
        value={condition.field || "__empty__"}
        onValueChange={(v) => onChange({ ...condition, field: v === "__empty__" ? "" : v })}
      >
        <SelectTrigger className="w-[220px] text-xs h-8">
          <SelectValue placeholder="Select field..." />
        </SelectTrigger>
        <SelectContent className="max-h-[300px]">
          {Array.from(grouped.entries()).map(([group, fields]) => (
            <div key={group}>
              <div className="px-2 py-1.5 text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">
                {group}
              </div>
              {fields.map((f) => (
                <SelectItem key={f.label} value={f.label} className="text-xs">
                  {f.label}
                </SelectItem>
              ))}
            </div>
          ))}
        </SelectContent>
      </Select>

      {/* Operator selector */}
      <Select
        value={condition.operator}
        onValueChange={(v) => onChange({ ...condition, operator: v })}
      >
        <SelectTrigger className="w-[90px] text-xs h-8">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {availableOps.map((op) => (
            <SelectItem key={op.mongoOp} value={op.mongoOp} className="text-xs">
              {op.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {/* Value input */}
      {condition.operator !== "$exists" && (
        <Input
          value={String(condition.value)}
          onChange={(e) => onChange({ ...condition, value: e.target.value })}
          placeholder="Value..."
          className="w-[140px] text-xs h-8"
        />
      )}

      {/* Remove button */}
      {removable && (
        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7 opacity-0 group-hover:opacity-100 transition-opacity text-muted-foreground hover:text-destructive"
          onClick={onRemove}
        >
          <X className="h-3.5 w-3.5" />
        </Button>
      )}
    </div>
  );
}
