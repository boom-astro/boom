/**
 * MongoPreview — live preview of the generated MongoDB aggregation pipeline.
 */

import { Button } from "@/components/ui/button";
import { Copy, Check } from "lucide-react";
import { useState } from "react";

interface MongoPreviewProps {
  pipeline: string;
}

export function MongoPreview({ pipeline }: MongoPreviewProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(pipeline);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">
          Generated MongoDB Pipeline
        </span>
        <Button
          variant="ghost"
          size="sm"
          className="h-6 text-xs text-muted-foreground"
          onClick={handleCopy}
        >
          {copied ? <Check className="h-3 w-3 mr-1" /> : <Copy className="h-3 w-3 mr-1" />}
          {copied ? "Copied" : "Copy"}
        </Button>
      </div>
      <pre className="bg-muted/50 border border-input rounded-md p-3 text-xs font-mono overflow-x-auto max-h-[200px] overflow-y-auto whitespace-pre">
        {pipeline || "// Add conditions to generate a pipeline"}
      </pre>
    </div>
  );
}
