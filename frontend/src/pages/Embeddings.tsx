import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  fetchEmbeddingsCount,
  fetchSimilarObjects,
  type AppleCiderFusion,
  type SimilarObject,
} from "@/lib/api";
import { formatLabel, formatPercent, rankedClasses } from "@/lib/applecider";

/** Matches the server's clamp. */
const MAX_TOP_K = 100;
const DEFAULT_TOP_K = 10;

const EMBEDDING_SURVEY = "ztf";

/** Classes shown inline; the rest are in the hover breakdown. */
const TOP_CLASSES = 3;

function formatScore(score: number): string {
  return score.toFixed(4);
}

function ProbabilityBar({ name, p }: { name: string; p: number }) {
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-36 truncate">{name}</span>
      <div className="bg-muted h-1.5 w-20 overflow-hidden rounded-full">
        <div className="bg-primary h-full" style={{ width: `${Math.min(Math.max(p, 0), 1) * 100}%` }} />
      </div>
      <span className="w-9 text-right tabular-nums text-muted-foreground">{formatPercent(p)}</span>
    </div>
  );
}

function Decision({ fusion }: { fusion: AppleCiderFusion | null | undefined }) {
  if (!fusion) return <span className="text-sm text-muted-foreground">Not classified</span>;
  if (fusion.abstain_completely || !fusion.decision_label) {
    return <Badge variant="outline">Abstained</Badge>;
  }
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <Badge>{formatLabel(fusion.decision_label)}</Badge>
      {fusion.decision_probability !== undefined && (
        <span className="text-sm tabular-nums">{formatPercent(fusion.decision_probability)}</span>
      )}
      {/* A family- or domain-level label is coarser than a class; say so. */}
      {fusion.decision_level && fusion.decision_level !== "class" && (
        <span className="text-xs text-muted-foreground">({fusion.decision_level})</span>
      )}
    </div>
  );
}

function ClassProbabilities({ fusion }: { fusion: AppleCiderFusion | null | undefined }) {
  if (!fusion) return <span className="text-sm text-muted-foreground">-</span>;
  const ranked = rankedClasses(fusion.class_probabilities);
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div tabIndex={0} className="inline-flex cursor-help flex-col gap-1 rounded-sm">
          {ranked.slice(0, TOP_CLASSES).map(([name, p]) => (
            <ProbabilityBar key={name} name={name} p={p} />
          ))}
        </div>
      </TooltipTrigger>
      <TooltipContent side="left" className="flex flex-col gap-1">
        {ranked.map(([name, p]) => (
          <div key={name} className="flex justify-between gap-4 text-xs">
            <span>{name}</span>
            <span className="tabular-nums">{formatPercent(p)}</span>
          </div>
        ))}
      </TooltipContent>
    </Tooltip>
  );
}

export default function Embeddings() {
  const [objectId, setObjectId] = useState("");
  const [topK, setTopK] = useState(String(DEFAULT_TOP_K));
  const [results, setResults] = useState<SimilarObject[] | null>(null);
  const [searchedId, setSearchedId] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const [count, setCount] = useState<number | null>(null);
  const [countError, setCountError] = useState<string | null>(null);

  const loadCount = useCallback(() => {
    fetchEmbeddingsCount()
      .then((n) => {
        setCount(n);
        setCountError(null);
      })
      .catch((e) => setCountError(e instanceof Error ? e.message : "Failed to fetch count"));
  }, []);

  useEffect(() => {
    loadCount();
  }, [loadCount]);

  async function runSearch(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = objectId.trim();
    if (!trimmed) return;
    const k = Math.min(Math.max(Math.round(Number(topK)) || DEFAULT_TOP_K, 1), MAX_TOP_K);
    setTopK(String(k));

    setLoading(true);
    setError(null);
    setNotice(null);
    try {
      const { results, message } = await fetchSimilarObjects(trimmed, k);
      setResults(results);
      setSearchedId(trimmed);
      // No embedding stored is a normal answer, shown as a notice, not an error.
      if (results.length === 0) setNotice(message ?? `No neighbors found for ${trimmed}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Similarity search failed");
      setResults(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="px-4 lg:px-6 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Embeddings</h1>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Stored Embeddings</CardDescription>
            <CardTitle className="text-2xl tabular-nums">
              {countError ? "-" : count === null ? "…" : count.toLocaleString()}
            </CardTitle>
            {countError && <p className="text-sm text-destructive">{countError}</p>}
          </CardHeader>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Model</CardDescription>
            <CardTitle className="text-2xl">AppleCiDEr fusion</CardTitle>
            <p className="text-sm text-muted-foreground">
              384-dimensional, L2-normalized. Ranked by cosine similarity.
            </p>
          </CardHeader>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Similarity Search</CardTitle>
          <CardDescription>
            Find the objects whose fusion embedding is closest to a given ZTF object's.
            The seed object is excluded from its own results.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <form onSubmit={runSearch} className="flex flex-wrap items-end gap-3">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="object-id">Object ID</Label>
              <Input
                id="object-id"
                value={objectId}
                onChange={(e) => setObjectId(e.target.value)}
                placeholder="ZTF18abcdefg"
                className="w-60 font-mono"
                autoComplete="off"
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="top-k">Neighbors</Label>
              <Input
                id="top-k"
                type="number"
                min={1}
                max={MAX_TOP_K}
                value={topK}
                onChange={(e) => setTopK(e.target.value)}
                className="w-24"
              />
            </div>
            <Button type="submit" disabled={loading || !objectId.trim()}>
              {loading ? "Searching…" : "Search"}
            </Button>
          </form>

          {error && <p className="text-sm text-destructive">{error}</p>}
          {notice && !error && <p className="text-sm text-muted-foreground">{notice}</p>}

          {results && results.length > 0 && (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Object</TableHead>
                  <TableHead>Classification</TableHead>
                  <TableHead>Class probabilities</TableHead>
                  <TableHead className="text-right">Similarity</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {results.map((hit) => (
                  <TableRow key={hit.object_id}>
                    <TableCell className="font-mono text-sm">
                      <Link
                        to={`/objects/${EMBEDDING_SURVEY}/${encodeURIComponent(hit.object_id)}`}
                        className="underline"
                      >
                        {hit.object_id}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <Decision fusion={hit.applecider_fusion} />
                    </TableCell>
                    <TableCell>
                      <ClassProbabilities fusion={hit.applecider_fusion} />
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{formatScore(hit.score)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}

          {results && results.length > 0 && (
            <p className="text-sm text-muted-foreground">
              {results.length} neighbor{results.length === 1 ? "" : "s"} of{" "}
              <span className="font-mono">{searchedId}</span>
            </p>
          )}
        </CardContent>
      </Card>

    </div>
  );
}
