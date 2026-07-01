import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Select, SelectTrigger, SelectContent, SelectItem, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertTriangle } from "lucide-react";
import { FilterBuilder } from "@/components/filter/FilterBuilder";
import type { FilterBuilderHandle } from "@/components/filter/FilterBuilder";
import { FilterFieldBrowser } from "@/components/filter/FilterFieldBrowser";
import { FilterHealthPanel } from "@/components/filter/FilterHealthPanel";
import { flattenAvroSchema } from "@/lib/filterConstants";
import api, { type FilterTestParams, type FilterTestCountResult, type AvroSchema } from "@/lib/api";
import { ZTF_FALLBACK_SCHEMA } from "@/lib/ztfFallbackSchema";

const DEFAULT_PIPELINE = `[
  {
    "$match": {
      "candidate.drb": { "$gt": 0.5 }
    }
  },
  {
    "$project": {
      "_id": 1,
      "objectId": 1,
      "candidate.ra": 1,
      "candidate.dec": 1,
      "candidate.magpsf": 1,
      "candidate.jd": 1
    }
  }
]`;

const FORBIDDEN_STAGES = ["$lookup", "$unionWith", "$out", "$merge"];

function validatePipeline(raw: string): { valid: boolean; error: string | null; pipeline: Record<string, unknown>[] | null } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { valid: false, error: "Invalid JSON. Please check your syntax.", pipeline: null };
  }

  if (!Array.isArray(parsed) || parsed.length === 0) {
    return { valid: false, error: "Pipeline must be a non-empty JSON array.", pipeline: null };
  }

  const stages = parsed as Record<string, unknown>[];

  // Check for forbidden stages
  for (const stage of stages) {
    const keys = Object.keys(stage);
    for (const key of keys) {
      if (FORBIDDEN_STAGES.includes(key)) {
        return { valid: false, error: `Stage "${key}" is not allowed in filter pipelines.`, pipeline: null };
      }
    }
  }

  // Must contain at least one $match
  const hasMatch = stages.some((s) => "$match" in s);
  if (!hasMatch) {
    return { valid: false, error: "Pipeline must contain at least one $match stage.", pipeline: null };
  }

  // Must end with $project containing objectId: 1
  const lastStage = stages[stages.length - 1];
  if (!("$project" in lastStage)) {
    return { valid: false, error: "Pipeline must end with a $project stage.", pipeline: null };
  }
  const proj = lastStage["$project"] as Record<string, unknown> | undefined;
  if (!proj || proj["objectId"] !== 1) {
    return { valid: false, error: 'Final $project must include "objectId": 1.', pipeline: null };
  }

  return { valid: true, error: null, pipeline: stages };
}

// Flatten nested objects for table display (e.g. {"candidate": {"ra": 1}} -> {"candidate.ra": 1})
function flattenObject(obj: Record<string, unknown>, prefix = ""): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      Object.assign(result, flattenObject(value as Record<string, unknown>, fullKey));
    } else {
      result[fullKey] = value;
    }
  }
  return result;
}

function friendlyError(err: unknown, fallback: string): string {
  if (err instanceof TypeError) {
    return "Backend unreachable.";
  }
  const raw = err instanceof Error ? err.message : "";
  const statusMatch = raw.match(/:\s*(\d{3})\s*(.*)$/);
  if (statusMatch) {
    const status = parseInt(statusMatch[1], 10);
    return `Error status code: ${status}`;
  }
  return raw || fallback;
}

export default function Filters() {
  const [activeTab, setActiveTab] = useState<"editor" | "results">("editor");
  const [survey, setSurvey] = useState<"ZTF" | "LSST">("ZTF");
  const [pipelineText, setPipelineText] = useState(DEFAULT_PIPELINE);
  const [startJd, setStartJd] = useState("2461138.5");
  const [endJd, setEndJd] = useState("2461140.5");
  const [limit, setLimit] = useState("10");

  // Ref for FilterBuilder imperative handle
  const filterBuilderRef = useRef<FilterBuilderHandle>(null);

  // Stable callback for FilterBuilder
  const handlePipelineTextChange = useCallback((text: string) => {
    setPipelineText(text);
    setCountResult(null);
  }, []);

  // Schema state
  const [schema, setSchema] = useState<AvroSchema | null>(null);
  const [schemaLoading, setSchemaLoading] = useState(false);

  // Flatten schema for FilterFieldBrowser
  const fieldOptions = useMemo(() => {
    if (!schema) return [];
    return flattenAvroSchema(schema);
  }, [schema]);

  // Results state
  const [countResult, setCountResult] = useState<FilterTestCountResult | null>(null);
  const [totalCountLoading, setTotalCountLoading] = useState(false);
  const [totalCount, setTotalCount] = useState<number | null>(null);
  const [queryTimeMs, setQueryTimeMs] = useState<number | null>(null);
  const [results, setResults] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [countLoading, setCountLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load schema on mount and survey change (falls back to hardcoded schema)
  useEffect(() => {
    let cancelled = false;
    setSchemaLoading(true);
    api.fetchBoomSchema(survey)
      .then((s) => { if (!cancelled) setSchema(s); })
      .catch(() => {
        if (!cancelled) {
          console.warn("Schema API unreachable, using fallback ZTF schema");
          if (survey === "ZTF") setSchema(ZTF_FALLBACK_SCHEMA as AvroSchema);
        }
      })
      .finally(() => { if (!cancelled) setSchemaLoading(false); });
    return () => { cancelled = true; };
  }, [survey]);

  function buildParams(pipeline: Record<string, unknown>[]): FilterTestParams {
    const params: FilterTestParams = {
      pipeline,
      survey,
      permissions: { [survey]: [1] },
    };
    if (startJd) params.start_jd = parseFloat(startJd);
    if (endJd) params.end_jd = parseFloat(endJd);
    if (limit) params.limit = parseInt(limit, 10);
    return params;
  }

  function fetchTotalForWindow(): Promise<void> {
    if (!startJd || !endJd) return Promise.resolve();
    setTotalCountLoading(true);
    return api.fetchTotalAlertCount(survey, parseFloat(startJd), parseFloat(endJd), { [survey]: [1] })
      .then((c) => setTotalCount(c))
      .catch(() => setTotalCount(null))
      .finally(() => setTotalCountLoading(false));
  }

  async function handleCount() {
    const { valid, error: validationError, pipeline } = validatePipeline(pipelineText);
    if (!valid || !pipeline) {
      setError(validationError);
      return;
    }
    setError(null);
    setCountLoading(true);
    setTotalCount(null);
    try {
      const [result] = await Promise.all([
        api.fetchFilterTestCount(buildParams(pipeline)),
        fetchTotalForWindow(),
      ]);
      setCountResult(result);
    } catch (err) {
      setError(friendlyError(err, "Filter count failed"));
    } finally {
      setCountLoading(false);
    }
  }

  async function handleRunFilter() {
    const { valid, error: validationError, pipeline } = validatePipeline(pipelineText);
    if (!valid || !pipeline) {
      setError(validationError);
      return;
    }
    setError(null);
    setLoading(true);
    setResults([]);
    setTotalCount(null);
    setQueryTimeMs(null);
    const t0 = performance.now();
    try {
      const params = buildParams(pipeline);
      // Fire both requests in parallel: filter results + total count
      const [data] = await Promise.all([
        api.fetchFilterTest(params),
        fetchTotalForWindow(),
      ]);
      setQueryTimeMs(Math.round(performance.now() - t0));
      setResults(data);
      // Also set the count from results length if we didn't already have it
      if (!countResult) {
        setCountResult({ count: data.length, pipeline });
      }
      setActiveTab("results");
    } catch (err) {
      setError(friendlyError(err, "Filter test failed"));
    } finally {
      setLoading(false);
    }
  }

  // Compute table columns from results
  const flatResults = results.map((r) => flattenObject(r));
  const columns = flatResults.length > 0
    ? Array.from(new Set(flatResults.flatMap((r) => Object.keys(r)))).filter((k) => k !== "_id")
    : [];

  return (
    <div className="px-4 lg:px-6 space-y-4">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 max-w-7xl mx-auto">
        {/* Left column: Pipeline Editor + Results */}
        <div className="lg:col-span-2 space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Filter Tester</CardTitle>
              <CardDescription>Build and test MongoDB aggregation pipelines against live alert data.</CardDescription>
            </CardHeader>
            <CardContent>
              <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as "editor" | "results")}>
                <TabsList className="grid w-full grid-cols-2 mb-4">
                  <TabsTrigger value="editor">Pipeline Editor</TabsTrigger>
                  <TabsTrigger value="results">
                    Results
                    {countResult && (
                      <span className="ml-2 text-xs bg-primary/10 text-primary px-1.5 py-0.5 rounded-full">
                        {countResult.count}
                      </span>
                    )}
                  </TabsTrigger>
                </TabsList>

                <TabsContent value="editor" forceMount className={activeTab !== "editor" ? "hidden" : undefined}>
                  <div className="space-y-4">
                    <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
                      <div className="sm:col-span-4">
                        <Label className="text-xs font-medium mb-1 block text-muted-foreground">Survey</Label>
                        <Select value={survey} onValueChange={(v) => { setSurvey(v as "ZTF" | "LSST"); setCountResult(null); }}>
                          <SelectTrigger className="w-full">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="ZTF">ZTF</SelectItem>
                            <SelectItem value="LSST">LSST</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>

                    {/* Visual Filter Builder (replaces raw textarea) */}
                    <FilterBuilder
                      ref={filterBuilderRef}
                      schema={schema}
                      rawPipelineText={pipelineText}
                      onRawPipelineChange={handlePipelineTextChange}
                    />

                    <Separator />

                    <div>
                      <h3 className="font-semibold text-sm mb-2">Time Range</h3>
                    </div>

                    <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
                      <div className="sm:col-span-2">
                        <Label htmlFor="startJd" className="text-xs font-medium mb-1 block text-muted-foreground">Start JD</Label>
                        <Input id="startJd" type="number" step="any" value={startJd} onChange={(e) => { setStartJd(e.target.value); setCountResult(null); }} placeholder="2461404.5" />
                      </div>
                      <div className="sm:col-span-2">
                        <Label htmlFor="endJd" className="text-xs font-medium mb-1 block text-muted-foreground">End JD</Label>
                        <Input id="endJd" type="number" step="any" value={endJd} onChange={(e) => { setEndJd(e.target.value); setCountResult(null); }} placeholder="2461406.5" />
                      </div>
                    </div>

                    <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
                      <div className="sm:col-span-2">
                        <Label htmlFor="limit" className="text-xs font-medium mb-1 block text-muted-foreground">Result Limit</Label>
                        <Input id="limit" type="number" value={limit} onChange={(e) => { setLimit(e.target.value); setCountResult(null); }} placeholder="10" />
                      </div>
                    </div>

                    {error && (
                      <div className="text-red-500 text-sm">{error}</div>
                    )}

                    <div className="flex justify-end gap-2 mt-4">
                      <Button variant="outline" onClick={handleCount} disabled={countLoading}>
                        {countLoading ? "Counting..." : "Count"}
                      </Button>
                      <Button onClick={handleRunFilter} disabled={loading}>
                        {loading ? "Running..." : "Run Filter"}
                      </Button>
                    </div>

                    {countResult && !countLoading && (
                      <div className="text-sm text-muted-foreground mt-2">
                        Matched <span className="font-semibold text-foreground">{countResult.count}</span> alerts.
                      </div>
                    )}
                  </div>
                </TabsContent>

                <TabsContent value="results" forceMount className={activeTab !== "results" ? "hidden" : undefined}>
                  {loading && (
                    <div className="space-y-2">
                      {[1, 2, 3, 4, 5].map((i) => (
                        <Skeleton key={i} className="h-8 w-full" />
                      ))}
                    </div>
                  )}

                  {!loading && error && (
                    <div className="flex items-start gap-2 rounded-md border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-400">
                      <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                      <span>{error}</span>
                    </div>
                  )}

                  {/* Filter Health Panel — shown after results are available */}
                  {!loading && !error && (results.length > 0 || countResult) && (
                    <div className="mb-4">
                      <FilterHealthPanel
                        matchedCount={countResult?.count ?? results.length}
                        totalCount={totalCount}
                        totalCountLoading={totalCountLoading}
                        results={results}
                        queryTimeMs={queryTimeMs}
                      />
                    </div>
                  )}

                  {!loading && !error && results.length > 0 && (
                    <div className="overflow-x-auto">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            {columns.map((col) => (
                              <TableHead key={col} className="text-xs font-mono whitespace-nowrap">{col}</TableHead>
                            ))}
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {flatResults.map((row, i) => (
                            <TableRow key={i}>
                              {columns.map((col) => (
                                <TableCell key={col} className="text-xs font-mono whitespace-nowrap">
                                  {row[col] !== undefined && row[col] !== null ? String(row[col]) : "—"}
                                </TableCell>
                              ))}
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                      <div className="text-sm text-muted-foreground mt-3">
                        Showing {results.length} result{results.length !== 1 ? "s" : ""}.
                      </div>
                    </div>
                  )}

                  {!loading && !error && results.length === 0 && !countResult && (
                    <div className="text-center text-muted-foreground py-8">
                      No results yet. Write a pipeline and click "Run Filter" to see matching alerts.
                    </div>
                  )}
                </TabsContent>
              </Tabs>
            </CardContent>
          </Card>
        </div>

        {/* Right column: Field Browser */}
        <div className="space-y-4">
          {schemaLoading && (
            <Card>
              <CardHeader>
                <Skeleton className="h-5 w-24" />
                <Skeleton className="h-4 w-48 mt-1" />
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {[1, 2, 3, 4, 5, 6, 7, 8].map((i) => (
                    <Skeleton key={i} className="h-4 w-full" />
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
          {!schemaLoading && fieldOptions.length > 0 && (
            <FilterFieldBrowser
              fieldOptions={fieldOptions}
              onFieldClick={(field) => filterBuilderRef.current?.addConditionWithField(field)}
            />
          )}
        </div>
      </div>
    </div>
  );
}
