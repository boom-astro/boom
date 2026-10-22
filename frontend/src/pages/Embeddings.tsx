import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  fetchEmbeddingsCount,
  fetchSimilarObjects,
  type SimilarObject,
} from "@/lib/api";

/** Matches the server's clamp. */
const MAX_TOP_K = 100;
const DEFAULT_TOP_K = 10;

const EMBEDDING_SURVEY = "ztf";

function formatScore(score: number): string {
  return score.toFixed(4);
}

function formatJd(jd: number | null | undefined): string {
  if (jd === null || jd === undefined) return "-";
  return jd.toFixed(5);
}

export default function Embeddings() {
  const [objectId, setObjectId] = useState("");
  const [topK, setTopK] = useState(DEFAULT_TOP_K);
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

    setLoading(true);
    setError(null);
    setNotice(null);
    try {
      const { results, message } = await fetchSimilarObjects(trimmed, topK);
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
                onChange={(e) => {
                  const n = Number(e.target.value);
                  if (Number.isFinite(n)) setTopK(Math.min(Math.max(Math.round(n), 1), MAX_TOP_K));
                }}
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
                  <TableHead className="text-right">Similarity</TableHead>
                  <TableHead className="text-right">Candid</TableHead>
                  <TableHead className="text-right">JD</TableHead>
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
                    <TableCell className="text-right tabular-nums">{formatScore(hit.score)}</TableCell>
                    <TableCell className="text-right tabular-nums font-mono text-sm">
                      {hit.candid ?? "-"}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{formatJd(hit.jd)}</TableCell>
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
