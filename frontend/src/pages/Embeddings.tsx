import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ClassProbabilities, Decision } from "@/components/applecider-classification";
import {
  EmbeddingsDisabled,
  fetchEmbeddingsCount,
  fetchSimilarObjects,
  type QueryObject,
  type SimilarObject,
} from "@/lib/api";

/** Matches the server's clamp. */
const MAX_TOP_K = 100;
const DEFAULT_TOP_K = 10;

const EMBEDDING_SURVEY = "ztf";

/** Automatic count retries wait 5s, doubling after each failure up to an hour. */
const COUNT_RETRY_BASE_MS = 5_000;
const COUNT_RETRY_MAX_MS = 60 * 60 * 1000;

function formatScore(score: number): string {
  return score.toFixed(4);
}

function ObjectLink({ objectId }: { objectId: string }) {
  return (
    <Link to={`/objects/${EMBEDDING_SURVEY}/${encodeURIComponent(objectId)}`} className="underline">
      {objectId}
    </Link>
  );
}

export default function Embeddings() {
  const [objectId, setObjectId] = useState("");
  const [topK, setTopK] = useState(String(DEFAULT_TOP_K));
  const [results, setResults] = useState<SimilarObject[] | null>(null);
  const [query, setQuery] = useState<QueryObject | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const [count, setCount] = useState<number | null>(null);
  const [countError, setCountError] = useState<string | null>(null);
  const [countDisabled, setCountDisabled] = useState(false);

  // Bumping this reloads the count; a failed load schedules the next bump.
  const [countLoad, setCountLoad] = useState(0);
  const countFailures = useRef(0);

  useEffect(() => {
    let canceled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    fetchEmbeddingsCount()
      .then((n) => {
        if (canceled) return;
        countFailures.current = 0;
        setCount(n);
        setCountError(null);
      })
      .catch((e) => {
        if (canceled) return;
        setCountError(e instanceof Error ? e.message : "Failed to fetch count");
        if (e instanceof EmbeddingsDisabled) {
          setCountDisabled(true);
          return;
        }
        const delay = Math.min(COUNT_RETRY_BASE_MS * 2 ** countFailures.current, COUNT_RETRY_MAX_MS);
        countFailures.current += 1;
        timer = setTimeout(() => setCountLoad((n) => n + 1), delay);
      });
    return () => {
      canceled = true;
      clearTimeout(timer);
    };
  }, [countLoad]);

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
      const { query, results, message } = await fetchSimilarObjects(trimmed, k);
      setQuery(query ?? null);
      setResults(results);
      // No embedding stored is a normal answer, shown as a notice, not an error.
      if (results.length === 0) setNotice(message ?? `No neighbors found for ${trimmed}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Similarity search failed");
      setQuery(null);
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
            {countError && (
              <>
                <p className="text-sm text-destructive">{countError}</p>
                {!countDisabled && (
                  <p className="text-sm text-muted-foreground">Retrying automatically.</p>
                )}
              </>
            )}
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
          <CardDescription className="text-base">
            Enter a ZTF object ID to find the objects whose AppleCiDEr fusion embeddings are most
            similar to it. The highlighted first row is the object you searched for, with its own
            classification for comparison. Its nearest neighbors follow, ranked by cosine
            similarity, where 1 means identical.
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

          {(query || (results && results.length > 0)) && (
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
                {query && (
                  <TableRow className="border-l-4 border-l-primary bg-primary/10 hover:bg-primary/15">
                    <TableCell className="font-mono text-sm">
                      <div className="flex items-center gap-2">
                        <ObjectLink objectId={query.object_id} />
                        <Badge className="px-2.5 py-0.5 font-sans text-sm font-semibold uppercase tracking-wide">
                          Query
                        </Badge>
                      </div>
                    </TableCell>
                    <TableCell>
                      <Decision fusion={query.applecider_fusion} />
                    </TableCell>
                    <TableCell>
                      <ClassProbabilities fusion={query.applecider_fusion} />
                    </TableCell>
                    <TableCell className="text-right text-muted-foreground">-</TableCell>
                  </TableRow>
                )}
                {results && results.length > 0 && (
                  <TableRow className="bg-muted/50 hover:bg-muted/50">
                    <TableCell
                      colSpan={4}
                      className="py-2 text-base font-semibold text-foreground"
                    >
                      {results.length} nearest neighbor{results.length === 1 ? "" : "s"}
                    </TableCell>
                  </TableRow>
                )}
                {results?.map((hit) => (
                  <TableRow key={hit.object_id}>
                    <TableCell className="font-mono text-sm">
                      <ObjectLink objectId={hit.object_id} />
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
        </CardContent>
      </Card>

    </div>
  );
}
