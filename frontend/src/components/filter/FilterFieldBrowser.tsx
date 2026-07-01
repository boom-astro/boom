/**
 * FilterFieldBrowser — a categorized, searchable, clickable field reference panel.
 * Replaces the raw SchemaViewer for the filter page.
 * Click a field chip to auto-add a condition in the visual builder.
 */

import { useState, useMemo } from "react";
import { Search, ChevronDown, ChevronRight, Plus } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import type { FieldOption } from "@/lib/filterSchema";

/** Human-readable descriptions for commonly used fields */
const FIELD_DESCRIPTIONS: Record<string, string> = {
  // Candidate
  "candidate.drb": "Deep-learning real/bogus score. 0=bogus, 1=real.",
  "candidate.rb": "Random forest real/bogus score. 0=bogus, 1=real.",
  "candidate.magpsf": "PSF magnitude. Lower = brighter.",
  "candidate.sigmapsf": "1-sigma uncertainty on magpsf.",
  "candidate.ra": "Right ascension (J2000, degrees).",
  "candidate.dec": "Declination (J2000, degrees).",
  "candidate.jd": "Julian Date of observation.",
  "candidate.fid": "Filter ID. 1=g, 2=r, 3=i.",
  "candidate.ndethist": "Number of spatially coincident detections.",
  "candidate.ncovhist": "Number of times observed (any detection).",
  "candidate.sgscore1": "Star/galaxy score of nearest PS1 source. 0=galaxy, 1=star.",
  "candidate.distpsnr1": "Distance to nearest PS1 source (arcsec).",
  "candidate.classtar": "SExtractor star/galaxy classifier. 0=galaxy, 1=star.",
  "candidate.fwhm": "Full width at half max of detection (pixels).",
  "candidate.isdiffpos": "Positive subtraction? 't'=yes (real transients).",
  "candidate.ssdistnr": "Distance to nearest known solar system object.",
  "candidate.programid": "Program ID. 1=public, 2/3=private.",
  // Classifications
  "classifications.acai_h": "ACAI 'human-interesting' transient score. Best for supernovae.",
  "classifications.acai_n": "ACAI 'nuclear' transient score. High = near galaxy nucleus.",
  "classifications.acai_v": "ACAI 'variable star' score.",
  "classifications.acai_o": "ACAI 'orphan' transient score. High = hostless.",
  "classifications.acai_b": "ACAI 'bogus' score. High = likely artifact.",
  "classifications.btsbot": "BTS Bot real transient score.",
  // Properties
  "properties.rock": "Likely solar system object (asteroid).",
  "properties.star": "Likely stellar, not a transient.",
  "properties.near_brightstar": "Near a bright star (artifact risk).",
  "properties.stationary": "Not moving (non-asteroid).",
  // Coordinates
  "coordinates.l": "Galactic longitude (degrees).",
  "coordinates.b": "Galactic latitude (degrees). |b|<15 = galactic plane.",
};

/** Priority ordering: these groups appear first and expanded */
const GROUP_ORDER = ["Classifications", "Properties", "Candidate", "Coordinates"];

type GroupedFields = { group: string; fields: FieldOption[] };

interface FilterFieldBrowserProps {
  fieldOptions: FieldOption[];
  onFieldClick?: (fieldPath: string) => void;
}

export function FilterFieldBrowser({ fieldOptions, onFieldClick }: FilterFieldBrowserProps) {
  const [search, setSearch] = useState("");
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});

  const grouped = useMemo(() => {
    const map = new Map<string, FieldOption[]>();
    for (const opt of fieldOptions) {
      const g = opt.group || "Other";
      if (!map.has(g)) map.set(g, []);
      map.get(g)!.push(opt);
    }

    // Sort groups by priority
    const result: GroupedFields[] = [];
    for (const g of GROUP_ORDER) {
      if (map.has(g)) {
        result.push({ group: g, fields: map.get(g)! });
        map.delete(g);
      }
    }
    // Add remaining groups
    for (const [g, fields] of map) {
      result.push({ group: g, fields });
    }
    return result;
  }, [fieldOptions]);

  const query = search.toLowerCase().trim();

  const filteredGroups = useMemo(() => {
    if (!query) return grouped;
    return grouped
      .map((g) => ({
        ...g,
        fields: g.fields.filter(
          (f) =>
            f.label.toLowerCase().includes(query) ||
            (FIELD_DESCRIPTIONS[f.label] || "").toLowerCase().includes(query)
        ),
      }))
      .filter((g) => g.fields.length > 0);
  }, [grouped, query]);

  const toggleGroup = (group: string) => {
    setCollapsed((prev) => ({ ...prev, [group]: !prev[group] }));
  };

  const typeBadgeColor = (type: string) => {
    switch (type) {
      case "number": return "bg-blue-500/15 text-blue-400 border-blue-500/30";
      case "boolean": return "bg-amber-500/15 text-amber-400 border-amber-500/30";
      case "string": return "bg-emerald-500/15 text-emerald-400 border-emerald-500/30";
      default: return "bg-zinc-500/15 text-zinc-400 border-zinc-500/30";
    }
  };

  const groupDescription = (group: string) => {
    switch (group) {
      case "Classifications": return "ML classifier scores (0-1)";
      case "Properties": return "Computed boolean flags";
      case "Candidate": return "Alert observation data";
      case "Coordinates": return "Galactic coordinates";
      default: return "";
    }
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm">Filterable Fields</CardTitle>
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
            <Input
              placeholder="Search fields..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="h-7 w-40 pl-7 text-xs"
            />
          </div>
        </div>
      </CardHeader>
      <CardContent className="pt-0">
        <div className="space-y-3 max-h-[600px] overflow-y-auto pr-1">
          {filteredGroups.map(({ group, fields }) => {
            const isCollapsed = collapsed[group] ?? false;
            return (
              <div key={group}>
                <button
                  onClick={() => toggleGroup(group)}
                  className="flex items-center gap-1.5 w-full text-left mb-1.5 group"
                >
                  {isCollapsed ? (
                    <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
                  ) : (
                    <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
                  )}
                  <span className="text-xs font-semibold">{group}</span>
                  <span className="text-[10px] text-muted-foreground ml-1">
                    {groupDescription(group)}
                  </span>
                  <span className="text-[10px] text-muted-foreground ml-auto">
                    {fields.length}
                  </span>
                </button>

                {!isCollapsed && (
                  <div className="flex flex-wrap gap-1 ml-5">
                    <TooltipProvider delayDuration={200}>
                      {fields.map((field) => (
                        <Tooltip key={field.label}>
                          <TooltipTrigger asChild>
                            <button
                              onClick={() => onFieldClick?.(field.label)}
                              className={`
                                inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[11px]
                                border transition-all duration-150
                                hover:ring-1 hover:ring-primary/40 hover:scale-[1.02]
                                active:scale-95 cursor-pointer
                                ${typeBadgeColor(field.type)}
                              `}
                            >
                              <span className="font-mono">
                                {field.label.includes(".")
                                  ? field.label.split(".").pop()
                                  : field.label}
                              </span>
                              <Badge
                                variant="outline"
                                className={`text-[9px] px-1 py-0 h-3.5 ${typeBadgeColor(field.type)}`}
                              >
                                {field.type}
                              </Badge>
                              <Plus className="h-2.5 w-2.5 opacity-0 group-hover:opacity-50 transition-opacity" />
                            </button>
                          </TooltipTrigger>
                          <TooltipContent side="top" className="max-w-xs">
                            <p className="font-mono text-xs font-semibold">{field.label}</p>
                            {FIELD_DESCRIPTIONS[field.label] && (
                              <p className="text-xs text-muted-foreground mt-0.5">
                                {FIELD_DESCRIPTIONS[field.label]}
                              </p>
                            )}
                          </TooltipContent>
                        </Tooltip>
                      ))}
                    </TooltipProvider>
                  </div>
                )}
              </div>
            );
          })}

          {filteredGroups.length === 0 && (
            <p className="text-xs text-muted-foreground text-center py-4">
              No fields matching "{search}"
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
