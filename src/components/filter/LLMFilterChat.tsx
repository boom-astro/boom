/**
 * LLMFilterChat — chat input for natural language filter generation.
 */

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Sparkles, Loader2 } from "lucide-react";
import type { FilterBlock, FieldOption } from "@/lib/filterSchema";
import { generateFilterFromNaturalLanguage } from "@/lib/llmFilterAgent";

interface LLMFilterChatProps {
  fieldOptions: FieldOption[];
  onFilterGenerated: (filters: FilterBlock[]) => void;
}

export function LLMFilterChat({ fieldOptions, onFilterGenerated }: LLMFilterChatProps) {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const groqApiKey = import.meta.env.VITE_GROQ_API_KEY || "";

  const handleGenerate = async () => {
    if (!query.trim()) return;

    setLoading(true);
    setError(null);

    const result = await generateFilterFromNaturalLanguage(query, fieldOptions, groqApiKey);

    if (result.filters) {
      onFilterGenerated(result.filters);
      setQuery("");
    } else {
      setError(result.error);
    }

    setLoading(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleGenerate();
    }
  };

  if (!groqApiKey) {
    return null; // Don't render if no API key configured
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-1.5">
        <Sparkles className="h-3.5 w-3.5 text-amber-500" />
        <span className="text-xs font-medium text-muted-foreground">
          Describe your filter in plain English
        </span>
      </div>
      <div className="flex gap-2">
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          className="flex-1 text-xs bg-muted/50 border border-input rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-ring placeholder:text-muted-foreground/60"
          placeholder='e.g. "Find all real transients brighter than magnitude 18 near galaxies"'
          disabled={loading}
        />
        <Button
          size="sm"
          className="h-8 text-xs"
          onClick={handleGenerate}
          disabled={loading || !query.trim()}
        >
          {loading ? (
            <><Loader2 className="h-3 w-3 mr-1 animate-spin" /> Generating...</>
          ) : (
            <><Sparkles className="h-3 w-3 mr-1" /> Generate</>
          )}
        </Button>
      </div>
      {error && <p className="text-xs text-red-500">{error}</p>}
    </div>
  );
}
