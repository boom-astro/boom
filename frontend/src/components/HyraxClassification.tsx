import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { Info, Play } from 'lucide-react';
import { toast } from 'sonner';
import { Card, CardContent, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Spinner } from '@/components/ui/spinner';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import api, { ClassificationModel, HyraxClassification as HyraxResult } from '@/lib/api';

function errorMessage(err: unknown): string {
  return err && typeof err === 'object' && 'message' in err
    ? String((err as { message?: unknown }).message)
    : String(err);
}

/** Normalize either shape the API may return into a list of labelled scores. */
function toScoreRows(result: HyraxResult): Array<{ label: string; value: number }> {
  if (result.classes) {
    return Object.entries(result.classes)
      .filter(([, v]) => typeof v === 'number' && Number.isFinite(v))
      .sort((a, b) => b[1] - a[1])
      .map(([label, value]) => ({ label, value }));
  }
  if (typeof result.score === 'number' && Number.isFinite(result.score)) {
    return [{ label: 'Score', value: result.score }];
  }
  return [];
}

export default function HyraxClassification() {
  const params = useParams();
  const survey = params.survey as string | undefined;
  const objectId = params.objectId as string | undefined;

  const [models, setModels] = useState<ClassificationModel[]>([]);
  const [modelsError, setModelsError] = useState<string | null>(null);
  const [model, setModel] = useState<string>('');
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<HyraxResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [helpDialogOpen, setHelpDialogOpen] = useState(false);

  // Which models exist is a property of the survey, so refetch when it changes.
  useEffect(() => {
    if (!survey) return;
    let cancelled = false;
    api.fetchClassificationModels(survey)
      .then(fetched => {
        if (cancelled) return;
        setModels(fetched);
        setModelsError(null);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setModels([]);
        setModelsError(errorMessage(err));
      });
    return () => { cancelled = true; };
  }, [survey]);

  // A result belongs to the object it was computed for. Without this, navigating
  // to another object leaves the previous object's scores on screen.
  useEffect(() => {
    setResult(null);
    setError(null);
  }, [survey, objectId]);

  const selected = models.find(m => m.id === model);

  async function handleSubmit() {
    if (!survey || !objectId || !model) return;
    setRunning(true);
    setError(null);
    setResult(null);
    try {
      const res = await api.classifyObject(survey, objectId, model);
      setResult(res);
      toast.success(`${selected?.name ?? model} finished running on ${objectId}`);
    } catch (err: unknown) {
      const msg = errorMessage(err);
      setError(msg);
      toast.error('Classification failed', { description: msg });
    } finally {
      setRunning(false);
    }
  }

  const rows = result ? toScoreRows(result) : [];

  return (
    <Card className="@container/card col-span-1 h-full bg-card text-card-foreground flex flex-col">
      <CardContent className="space-y-4 pt-0 flex-1 flex flex-col min-h-0">
        <div className="flex items-center gap-2 min-w-0">
          <CardTitle className="text-lg">Hyrax Classification</CardTitle>
          <button
            onClick={() => setHelpDialogOpen(true)}
            title="Widget information"
            className="p-1 rounded hover:bg-slate-100 dark:hover:bg-slate-700"
          >
            <Info className="w-4 h-4 text-gray-500 dark:text-gray-400" />
          </button>
        </div>

        <div className="flex items-center gap-2">
          <Select value={model} onValueChange={setModel} disabled={running || models.length === 0}>
            <SelectTrigger className="flex-1 h-8 text-xs">
              <SelectValue placeholder={models.length === 0 ? 'No models available' : 'Select a model…'} />
            </SelectTrigger>
            <SelectContent>
              {models.map(m => (
                <SelectItem
                  key={m.id}
                  value={m.id}
                  disabled={!m.available}
                  className="text-xs"
                >
                  {m.name}{!m.available && ' (not installed)'}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button
            size="sm"
            onClick={handleSubmit}
            disabled={!model || running || !survey || !objectId || !selected?.available}
          >
            {running ? <Spinner className="size-4" /> : <Play className="size-4" />}
            {running ? 'Running…' : 'Run'}
          </Button>
        </div>

        {selected && (
          <p className="text-xs text-muted-foreground -mt-2">{selected.description}</p>
        )}

        {modelsError && (
          <Alert variant="destructive" className="py-2">
            <AlertDescription className="text-xs break-words">
              Could not load the model list: {modelsError}
            </AlertDescription>
          </Alert>
        )}

        {error && (
          <Alert variant="destructive" className="py-2">
            <AlertDescription className="text-xs break-words">{error}</AlertDescription>
          </Alert>
        )}

        <div className="flex-1 min-h-0">
          {running && (
            <div className="h-full flex items-center justify-center text-sm text-muted-foreground gap-2">
              <Spinner className="size-4" /> Running {selected?.name ?? model}…
            </div>
          )}

          {!running && result && rows.length > 0 && (
            <div className="space-y-2">
              {rows.map(({ label, value }) => {
                const pct = Math.max(0, Math.min(1, value)) * 100;
                const color = value > 0.7 ? 'bg-green-500' : value > 0.4 ? 'bg-yellow-500' : 'bg-red-500';
                return (
                  <div key={label}>
                    <div className="flex items-center justify-between text-xs mb-0.5">
                      <span className="font-medium truncate pr-2">{label}</span>
                      <span className="tabular-nums font-semibold">{(value * 100).toFixed(1)}%</span>
                    </div>
                    <div className="h-2 rounded-full bg-muted overflow-hidden">
                      <div className={`h-full ${color}`} style={{ width: `${pct}%` }} />
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {!running && result && rows.length === 0 && (
            <div className="h-full flex items-center justify-center text-sm text-muted-foreground italic px-4 py-6 text-center bg-muted/30 rounded-lg border border-dashed border-muted-foreground/30">
              The model returned no scores for this object.
            </div>
          )}

          {!running && !result && !error && (
            <div className="h-full flex items-center justify-center text-sm text-muted-foreground italic px-4 py-6 text-center bg-muted/30 rounded-lg border border-dashed border-muted-foreground/30">
              Select a Hyrax model and press Run to classify this object.
            </div>
          )}
        </div>
      </CardContent>

      {/* Help Dialog */}
      <Dialog open={helpDialogOpen} onOpenChange={setHelpDialogOpen}>
        <DialogContent className="w-[min(1000px,95vw)] max-w-none sm:!max-w-none max-h-[90vh] overflow-auto">
          <DialogHeader>
            <DialogTitle className="text-xl">Running Hyrax Models</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 text-sm">
            <div>
              <h3 className="font-semibold mb-2">What This Widget Does</h3>
              <p className="text-gray-600 dark:text-gray-300">
                The ML Scores widget shows classifications computed automatically by the ingestion pipeline.
                This widget instead lets you run a Hyrax model against this object <span className="font-semibold">on demand</span>,
                and see the resulting scores immediately.
              </p>
            </div>
            <div>
              <h3 className="font-semibold mb-2">How To Use It</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">1. Select a model:</span>
                  <span>
                    Pick from the dropdown. A short description of the selected model appears below it.
                    Models marked "not installed" are known to the server but their weights are not
                    deployed yet, so they cannot be run.
                  </span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">2. Press Run:</span>
                  <span>
                    The cutouts of this object's brightest alert are scored by the model. This may
                    take a few seconds.
                  </span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">3. Read the scores:</span>
                  <span>
                    Multiclass models list every class sorted by probability; binary models show a single score.
                    Bars are colour-coded green (&gt;70%), yellow (40–70%), red (&lt;40%).
                  </span>
                </div>
              </div>
            </div>
            <div>
              <h3 className="font-semibold mb-2">Notes</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">Not persisted:</span>
                  <span>Results are shown for this session only and are not written back to the object.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Re-running:</span>
                  <span>Selecting a different model and pressing Run replaces the displayed result.</span>
                </div>
              </div>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </Card>
  );
}
