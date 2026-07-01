/**
 * ImportExportDialog — allows importing SkyPortal filter JSON and exporting
 * the current filter tree for use in Fritz/SkyPortal.
 */

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Import, Download, X } from "lucide-react";
import type { FilterBlock } from "@/lib/filterSchema";

interface ImportExportDialogProps {
  open: boolean;
  onClose: () => void;
  onImport: (filters: FilterBlock[]) => void;
  currentFilters: FilterBlock[];
}

export function ImportExportDialog({ open, onClose, onImport, currentFilters }: ImportExportDialogProps) {
  const [importText, setImportText] = useState("");
  const [importError, setImportError] = useState<string | null>(null);
  const [tab, setTab] = useState<"import" | "export">("import");

  if (!open) return null;

  const handleImport = () => {
    try {
      const parsed = JSON.parse(importText);

      // Validate it looks like a filter tree
      if (!Array.isArray(parsed) || parsed.length === 0) {
        setImportError("Expected a JSON array of filter blocks.");
        return;
      }

      // Check that at least the first element has the expected structure
      const first = parsed[0];
      if (!first.category || !first.id) {
        setImportError('Each block must have "id" and "category" fields. This does not look like a SkyPortal filter.');
        return;
      }

      setImportError(null);
      onImport(parsed as FilterBlock[]);
      onClose();
    } catch {
      setImportError("Invalid JSON. Please check your syntax.");
    }
  };

  const exportJson = JSON.stringify(currentFilters, null, 2);

  const handleCopyExport = async () => {
    await navigator.clipboard.writeText(exportJson);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-card border border-border rounded-lg shadow-xl w-full max-w-lg mx-4">
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-border">
          <h3 className="font-semibold text-sm">Import / Export Filter</h3>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>

        {/* Tab switcher */}
        <div className="flex border-b border-border">
          <button
            onClick={() => setTab("import")}
            className={`flex-1 px-4 py-2 text-xs font-medium transition-colors ${
              tab === "import"
                ? "text-primary border-b-2 border-primary"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            <Import className="h-3 w-3 inline mr-1" /> Import from SkyPortal
          </button>
          <button
            onClick={() => setTab("export")}
            className={`flex-1 px-4 py-2 text-xs font-medium transition-colors ${
              tab === "export"
                ? "text-primary border-b-2 border-primary"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            <Download className="h-3 w-3 inline mr-1" /> Export
          </button>
        </div>

        {/* Content */}
        <div className="p-4">
          {tab === "import" ? (
            <div className="space-y-3">
              <p className="text-xs text-muted-foreground">
                Paste the filter JSON from SkyPortal / Fritz. This is the block/condition tree, not raw MongoDB.
              </p>
              <textarea
                value={importText}
                onChange={(e) => { setImportText(e.target.value); setImportError(null); }}
                className="w-full h-48 font-mono text-xs bg-muted/50 border border-input rounded-md p-3 resize-y focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder='[{"id": "root-block", "category": "block", "operator": "and", "children": [...]}]'
                spellCheck={false}
              />
              {importError && <p className="text-xs text-red-500">{importError}</p>}
              <div className="flex justify-end">
                <Button size="sm" onClick={handleImport} disabled={!importText.trim()}>
                  Import Filter
                </Button>
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              <p className="text-xs text-muted-foreground">
                Copy this JSON and paste it into SkyPortal / Fritz to use the same filter there.
              </p>
              <pre className="bg-muted/50 border border-input rounded-md p-3 text-xs font-mono overflow-x-auto max-h-[250px] overflow-y-auto whitespace-pre">
                {exportJson}
              </pre>
              <div className="flex justify-end">
                <Button size="sm" variant="outline" onClick={handleCopyExport}>
                  Copy to Clipboard
                </Button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
