/**
 * FilterBuilder — main visual filter builder component.
 * Renders a block/condition tree and generates a live MongoDB pipeline preview.
 * Supports toggling to raw JSON "Advanced Mode" for power users.
 * Includes LLM chat for natural language filter generation and Import/Export.
 */

import { useState, useMemo, useCallback, useEffect, forwardRef, useImperativeHandle } from "react";
import { Button } from "@/components/ui/button";
import { Code, Layers, ArrowUpDown } from "lucide-react";
import type { FilterBlock, FilterNode, FieldOption } from "@/lib/filterSchema";
import { createDefaultFilter } from "@/lib/filterSchema";
import { convertToMongoPipeline, formatPipeline } from "@/lib/mongoPipelineBuilder";
import { flattenAvroSchema } from "@/lib/filterConstants";
import { BlockNode } from "./BlockNode";
import { MongoPreview } from "./MongoPreview";
import { LLMFilterChat } from "./LLMFilterChat";
import { ImportExportDialog } from "./ImportExportDialog";
import type { AvroSchema } from "@/lib/api";

interface FilterBuilderProps {
  /** The Avro schema for the current survey (used to populate field dropdowns) */
  schema: AvroSchema | null;
  /** Additional projection fields to include (beyond objectId) */
  projectionFields?: string[];
  /** External raw pipeline text for Advanced Mode sync */
  rawPipelineText: string;
  onRawPipelineChange: (text: string) => void;
}

/** Methods exposed via ref */
export interface FilterBuilderHandle {
  addConditionWithField: (field: string) => void;
}

export const FilterBuilder = forwardRef<FilterBuilderHandle, FilterBuilderProps>(function FilterBuilder({
  schema,
  projectionFields,
  rawPipelineText,
  onRawPipelineChange,
}, ref) {
  const [filters, setFilters] = useState<FilterBlock[]>(createDefaultFilter);
  const [mode, setMode] = useState<"visual" | "advanced">("visual");
  const [importExportOpen, setImportExportOpen] = useState(false);

  // Expose addConditionWithField to parent via ref
  useImperativeHandle(ref, () => ({
    addConditionWithField(field: string) {
      setFilters((prev) => {
        const newCond = {
          id: `cond-${Date.now()}`,
          category: "condition" as const,
          field,
          operator: "$gt",
          value: "",
        };
        // Add to the first root block's children
        if (prev.length > 0) {
          const updated = [...prev];
          updated[0] = {
            ...updated[0],
            children: [...updated[0].children, newCond],
          };
          return updated;
        }
        return prev;
      });
      // Switch to visual mode if in advanced
      setMode("visual");
    },
  }), []);

  // Flatten the Avro schema into field options for dropdowns
  const fieldOptions: FieldOption[] = useMemo(() => {
    if (!schema) return [];
    return flattenAvroSchema(schema);
  }, [schema]);

  // Generate the MongoDB pipeline from the current filter tree
  const pipeline = useMemo(() => {
    if (mode === "advanced") return [];
    return convertToMongoPipeline(filters, projectionFields);
  }, [filters, projectionFields, mode]);

  const pipelineText = useMemo(() => formatPipeline(pipeline), [pipeline]);

  // Sync the generated pipeline text to the parent when in visual mode
  useEffect(() => {
    if (mode === "visual" && pipeline.length > 0) {
      onRawPipelineChange(pipelineText);
    }
  }, [pipelineText, mode, onRawPipelineChange, pipeline.length]);

  // Update a root block
  const updateRoot = useCallback((index: number, updated: FilterNode) => {
    setFilters((prev) => {
      const next = [...prev];
      next[index] = updated as FilterBlock;
      return next;
    });
  }, []);

  const toggleMode = () => {
    if (mode === "visual") {
      // Switching to advanced: sync current pipeline text
      onRawPipelineChange(pipelineText);
    }
    setMode((m) => (m === "visual" ? "advanced" : "visual"));
  };

  // Import: try to parse raw JSON back into filter tree
  const handleImportFromJson = useCallback((json: string) => {
    try {
      const parsed = JSON.parse(json);
      if (Array.isArray(parsed) && parsed.length > 0 && parsed[0].category === "block") {
        // This is a block/condition tree (SkyPortal format)
        setFilters(parsed as FilterBlock[]);
        setMode("visual");
        return true;
      }
    } catch {
      // Not valid JSON
    }
    return false;
  }, []);

  // Handle LLM-generated filters
  const handleLLMFilter = useCallback((generated: FilterBlock[]) => {
    setFilters(generated);
    setMode("visual");
  }, []);

  // Handle import from dialog
  const handleImportFromDialog = useCallback((imported: FilterBlock[]) => {
    setFilters(imported);
    setMode("visual");
  }, []);

  return (
    <div className="space-y-3">
      {/* LLM Chat */}
      {mode === "visual" && (
        <LLMFilterChat
          fieldOptions={fieldOptions}
          onFilterGenerated={handleLLMFilter}
        />
      )}

      {/* Mode toggle + Import/Export */}
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground">
          {mode === "visual" ? "Visual Builder" : "Advanced Mode (Raw JSON)"}
        </span>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            className="h-7 text-xs"
            onClick={() => setImportExportOpen(true)}
          >
            <ArrowUpDown className="h-3 w-3 mr-1" /> Import / Export
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="h-7 text-xs"
            onClick={toggleMode}
          >
            {mode === "visual" ? (
              <><Code className="h-3 w-3 mr-1" /> Switch to Raw JSON</>
            ) : (
              <><Layers className="h-3 w-3 mr-1" /> Switch to Visual</>
            )}
          </Button>
        </div>
      </div>

      {mode === "visual" ? (
        <>
          {/* Visual filter tree */}
          <div className="space-y-2">
            {filters.map((block, i) => (
              <BlockNode
                key={block.id}
                block={block}
                fieldOptions={fieldOptions}
                onChange={(updated) => updateRoot(i, updated)}
                isRoot
              />
            ))}
          </div>

          {/* MongoDB preview */}
          <MongoPreview pipeline={pipelineText} />
        </>
      ) : (
        /* Advanced mode: raw JSON textarea (same as the original) */
        <div>
          <textarea
            value={rawPipelineText}
            onChange={(e) => onRawPipelineChange(e.target.value)}
            className="w-full h-64 font-mono text-xs bg-muted/50 border border-input rounded-md p-3 resize-y focus:outline-none focus:ring-2 focus:ring-ring"
            spellCheck={false}
            placeholder="Enter your MongoDB aggregation pipeline as a JSON array..."
          />
          <div className="flex items-center justify-between mt-1">
            <p className="text-[11px] text-muted-foreground">
              Must contain at least one <code>$match</code> and end with a <code>$project</code> that includes <code>"objectId": 1</code>.
            </p>
            <Button
              variant="ghost"
              size="sm"
              className="h-6 text-xs text-muted-foreground"
              onClick={() => handleImportFromJson(rawPipelineText)}
            >
              Import as Visual Filter
            </Button>
          </div>
        </div>
      )}

      {/* Import/Export Dialog */}
      <ImportExportDialog
        open={importExportOpen}
        onClose={() => setImportExportOpen(false)}
        onImport={handleImportFromDialog}
        currentFilters={filters}
      />
    </div>
  );
});
