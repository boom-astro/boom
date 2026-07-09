import { useMemo, useState } from 'react';
import { TrendingUp, TrendingDown, Minus, Clock, AlertTriangle, Info } from 'lucide-react';
import { Card, CardContent, CardTitle } from '@/components/ui/card';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { Alert, AlertDescription } from '@/components/ui/alert';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"

const width = 800;
const height = 500;
const padding = { left: 60, right: 40, top: 40, bottom: 80 };
const plotWidth = width - padding.left - padding.right;
const plotHeight = height - padding.top - padding.bottom;

// --- Types ---
type EpochEntry = { epoch: number; score?: number; date?: string; classes?: Record<string, number> };
type ClassifierEntry = { name: string; score: number; history: EpochEntry[]; isStatic?: boolean; separation?: number, description?: string | undefined };
type BinaryFamilies = Record<string, ClassifierEntry[]>;
type MulticlassData = { name: string; classes: Record<string, number>; history: EpochEntry[] };

type MapAlertResult = { binary: BinaryFamilies; multiclass: Record<string, MulticlassData> };

type AlertLike = {
  classifications?: Record<string, number>;
  candidate?: { drb?: number; sgscore1?: number; distpsnr1?: number, reliability?: number };
  classifications_history?: Record<string, number>[];
  cross_matches?: Record<string, Array<{ ra?: number; dec?: number; score?: number, distance_arcsec?: number }>>;
};

function mapAlertClassifications(alert: unknown): MapAlertResult {
  const mapped: MapAlertResult = { binary: {}, multiclass: {} };
  if (!alert) return mapped;
  const a = alert as AlertLike;
  const historySnapshots = a.classifications_history && Array.isArray(a.classifications_history)
    ? a.classifications_history
    : [];

  if (a.classifications) {
    const baseHistory = historySnapshots.length > 0 ? historySnapshots : [a.classifications];

    mapped.binary['ACAI'] = [
      { name: 'Hosted', score: a.classifications.acai_h ?? 0, history: [] },
      { name: 'Nuclear', score: a.classifications.acai_n ?? 0, history: [] },
      { name: 'Variable', score: a.classifications.acai_v ?? 0, history: [] },
      { name: 'Orphan', score: a.classifications.acai_o ?? 0, history: [] },
    ];

    mapped.binary['BTSBot'] = [
      {
        name: 'BTSBot',
        score: a.classifications.btsbot ?? 0,
        history: []
      }
    ];

    baseHistory.forEach((snapshot: Record<string, number>, idx: number) => {
      const epochEntry: EpochEntry = { epoch: idx + 1, classes: snapshot };
      mapped.binary['ACAI'].forEach((classifier: ClassifierEntry) => {
        const classKey = classifier.name === 'Hosted' ? 'acai_h'
          : classifier.name === 'Nuclear' ? 'acai_n'
          : classifier.name === 'Variable' ? 'acai_v'
          : classifier.name === 'Orphan' ? 'acai_o'
          : null;
        if (classKey && snapshot[classKey] !== undefined) {
          classifier.history.push({ ...epochEntry, score: snapshot[classKey] });
        }
      });
      const btsbotEntry = snapshot['btsbot'];
      if (btsbotEntry !== undefined) {
        const btsbotClassifier = mapped.binary['BTSBot'].find((c) => c.name === 'BTSBot');
        if (btsbotClassifier) {
          btsbotClassifier.history.push({ ...epochEntry, score: btsbotEntry });
        }
      }
    });
  }

  if (a.candidate?.drb !== undefined) {
    mapped.binary['drb'] = [
      {
        name: 'Real/Bogus',
        score: a.candidate.drb ?? 0,
        history: []
      },
    ];
  } else if (a.candidate?.reliability !== undefined) {
    mapped.binary['reliability'] = [
      {
        name: 'Real/Bogus',
        score: a.candidate.reliability ?? 0,
        history: []
      },
    ];
  }

  if (a.candidate?.sgscore1 !== undefined) {
    mapped.binary['sgscore'] = [
      {
        name: 'Star/Galaxy',
        score: a.candidate.sgscore1 ?? 0,
        isStatic: true,
        history: [
          {
            epoch: 1,
            score: a.candidate.sgscore1 ?? 0,
          },
        ],
        separation: a.candidate.distpsnr1,
      },
    ];
  }

  // Add LSPSC classifier from cross_matches if available
  if (a.cross_matches?.LSPSC && Array.isArray(a.cross_matches.LSPSC) && a.cross_matches.LSPSC.length > 0) {
    const lspscMatches = a.cross_matches.LSPSC.filter((m) => m.ra !== undefined && m.dec !== undefined && m.score !== undefined);
    if (lspscMatches.length > 0) {
      const nearestMatch = lspscMatches[0];
      mapped.binary['LSPSC'] = [
        {
          name: 'LSPSC',
          score: nearestMatch.score ?? 0,
          isStatic: true,
          history: [],
          separation: nearestMatch.distance_arcsec,
          description: `High score + small separation indicate a likely star; a low score indicates a likely a galaxy.`
        },
      ];
    }
  }

  return mapped;
}

// Anomaly detection
const detectAnomalies = (allClassifiers: BinaryFamilies) => {
  const anomalies: { severity: string; message: string }[] = [];
  const allScores: Record<string, number> = {};

  const flatList = Object.values(allClassifiers).flat() as ClassifierEntry[];
  flatList.forEach((c: ClassifierEntry) => {
    allScores[c.name] = c.score;
  });

  // Example: Check for conflicting classifications
  if (allScores['Hosted'] > 0.7 && allScores['Nuclear'] > 0.7) {
    anomalies.push({
      severity: 'high',
      message: 'Conflicting: High Hosted + Nuclear scores',
    });
  }

  return anomalies;
};

const CompactHeatmap = ({ name, score, showTrend, history, isStatic, separation, description }: { name: string; score: number; showTrend?: boolean; history: EpochEntry[]; isStatic?: boolean; separation?: number; description?: string }) => {
  const bgColor = score > 0.7 ? 'bg-green-500' : score > 0.4 ? 'bg-yellow-500' : 'bg-red-500';
  const trend = history && history.length > 1 ? (history[history.length - 1].score ?? 0) - (history[0].score ?? 0) : 0;
  const TrendIcon = trend > 0.1 ? TrendingUp : trend < -0.1 ? TrendingDown : Minus;
  const trendColor = trend > 0.1 ? 'text-green-100' : trend < -0.1 ? 'text-red-100' : 'text-white/60';
  const hasTooltipContent = description || isStatic;

  const content = (
    <div className={`${bgColor} text-white px-2.5 py-2 rounded-lg relative flex flex-col ${hasTooltipContent ? 'cursor-help' : ''}`}>
      <div className="flex items-center justify-between mb-0.5">
        <div className="text-xs font-semibold truncate flex-1 pr-2">{name}</div>
        {showTrend && history && history.length > 1 && !isStatic && (
          <TrendIcon className={`w-3 h-3 flex-shrink-0 ${trendColor}`} />
        )}
      </div>
      <div className="flex items-end justify-between">
        <div className="text-xl font-bold tabular-nums leading-none">{(score * 100).toFixed(0)}%</div>
        {separation !== undefined && (
          <div className="text-xs text-white/90 leading-none">
            {separation < 60 ? `${separation.toFixed(1)}″` : `${(separation / 60).toFixed(2)}′`}
          </div>
        )}
      </div>
    </div>
  );

  if (!hasTooltipContent) {
    return content;
  }

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        {content}
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">
        <div className="text-sm space-y-2">
          {isStatic && <p>Static score (from spatial catalog matching).</p>}
          {description && <p className="text-sm text-gray-500">Description: {description}</p>}
        </div>
      </TooltipContent>
    </Tooltip>
  );
};

const CompactSparkline = ({ classifier, onClick }: { classifier: ClassifierEntry; onClick?: () => void }) => {
  const { name, score, history } = classifier;
  const color = score > 0.7 ? '#10b981' : score > 0.4 ? '#f59e0b' : '#ef4444';

  const sparklineData = history.map((h: EpochEntry) => h.score ?? 0);
  const axisMin = 0;
  const axisMax = 1;
  const axisRange = axisMax - axisMin || 1;
  const denom = Math.max(sparklineData.length - 1, 1); // avoid div/0 when only one point

  const sparklinePath = sparklineData.map((value: number, i: number) => {
    const x = (i / denom) * 100;
    const y = 24 - ((value - axisMin) / axisRange) * 20;
    return `${i === 0 ? 'M' : 'L'} ${x} ${y}`;
  }).join(' ');

  const trend = history && history.length > 1 ? (history[history.length - 1].score ?? 0) - (history[0].score ?? 0) : 0;
  const TrendIcon = trend > 0.1 ? TrendingUp : trend < -0.1 ? TrendingDown : Minus;
  const trendColor = trend > 0.1 ? 'text-green-600' : trend < -0.1 ? 'text-red-600' : 'text-gray-400';

  return (
    <button
      onClick={onClick}
      className="py-2 px-2.5 rounded-lg text-left transition-colors w-full border-1 hover:border-gray-300"
    >
      <div className="flex items-center justify-between">
        <div className="flex-1 min-w-0">
          <h4 className="text-xs font-semibold truncate">{name}</h4>
        </div>
        <div className="flex items-center gap-1 flex-shrink-0 ml-1">
          <TrendIcon className={`w-3 h-3 ${trendColor}`} />
          <div className="text-base font-bold tabular-nums" style={{ color }}>
            {(score * 100).toFixed(0)}%
          </div>
        </div>
      </div>
      <svg viewBox="0 0 100 24" className="w-full h-8">
        <path
          d={`${sparklinePath} L 100 24 L 0 24 Z`}
          fill={color}
          opacity="0.1"
        />
        <path
          d={sparklinePath}
          fill="none"
          stroke={color}
          strokeWidth="2"
        />
        {sparklineData.map((value: number, i: number) => {
          const x = (i / denom) * 100;
          const y = 24 - ((value - axisMin) / axisRange) * 20;
          const isLast = i === sparklineData.length - 1;
          return (
            <circle
              key={i}
              cx={x}
              cy={y}
              r={isLast ? 2.5 : 1.5}
              fill={color}
            />
          );
        })}
      </svg>
      <div className="text-xs text-gray-400 mt-1 flex items-center gap-1">
        <Clock className="w-3 h-3" />
        <span>{history.length} epochs</span>
      </div>
    </button>
  );
};

type TimeSeriesDialogProps = { open: boolean; onClose: (open: boolean) => void; classifier?: ClassifierEntry | null; isMulticlass?: boolean; multiclassData?: MulticlassData | null };

const TimeSeriesDialog = ({ open, onClose, classifier, isMulticlass, multiclassData }: TimeSeriesDialogProps) => {
  // Hover state for multiclass legend interactivity (always declared to preserve hooks order)
  const [hoveredClass, setHoveredClass] = useState<string | null>(null);

  if (!open) return null;
  if (!classifier && !multiclassData) return null;

  if (isMulticlass && multiclassData) {
    // Multi-class stacked area chart
    const { name, history } = multiclassData as MulticlassData;
    const allClasses = Object.keys(history[0].classes ?? {});
    // Fix y-axis to full probability range (0-1)
    const minY = 0;
    const maxY = 1;
    const range = maxY - minY || 1;
    const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899'];

    return (
      <Dialog open={open} onOpenChange={onClose}>
        <DialogContent className="w-[min(1200px,95vw)] max-w-none sm:!max-w-none max-h-[90vh] overflow-auto">
          <DialogHeader>
            <DialogTitle className="text-xl">{name} - Evolution Over Time</DialogTitle>
          </DialogHeader>

          <div className="mt-4">
            <svg width={width} height={height} className="mx-auto">
              {/* Y-axis grid lines */}
              {[0, 0.25, 0.5, 0.75, 1.0].map((t: number) => {
                const val = minY + t * range;
                const y = padding.top + plotHeight - ((val - minY) / range) * plotHeight;
                return (
                  <g key={`grid-y-${t}`}>
                    <line
                      x1={padding.left}
                      y1={y}
                      x2={width - padding.right}
                      y2={y}
                      stroke="#e5e7eb"
                      strokeWidth="1"
                      strokeDasharray="4"
                    />
                    <text
                      x={padding.left - 10}
                      y={y}
                      textAnchor="end"
                      dominantBaseline="middle"
                      className="text-xs fill-gray-600"
                    >
                      {(val * 100).toFixed(1)}%
                    </text>
                  </g>
                );
              })}

              {/* Multi-line chart (not stacked) */}
              {allClasses.map((className: string, classIdx: number) => {
                const color = colors[classIdx % colors.length];

                // Active state: when a legend item is hovered, only that class is active
                const isActive = !hoveredClass || hoveredClass === className;
                const groupOpacity = isActive ? 0.98 : 0.12;
                const strokeWidth = isActive ? 3 : 2;

                // Line path
                const linePath = history.map((h: EpochEntry, i: number) => {
                  const x = padding.left + (i / (history.length - 1)) * plotWidth;
                  const y = padding.top + plotHeight - ((h.classes?.[className] ?? 0 - minY) / range) * plotHeight;
                  return `${i === 0 ? 'M' : 'L'} ${x} ${y}`;
                }).join(' ');

                return (
                  <g key={className} opacity={groupOpacity}>
                    {/* Line */}
                    <path
                      d={linePath}
                      fill="none"
                      stroke={color}
                      strokeWidth={strokeWidth}
                      opacity={1}
                    />
                    {/* Data points */}
                    {history.map((h: EpochEntry, i: number) => {
                      const x = padding.left + (i / (history.length - 1)) * plotWidth;
                      const y = padding.top + plotHeight - ((h.classes?.[className] ?? 0 - minY) / range) * plotHeight;
                      return (
                        <circle
                          key={i}
                          cx={x}
                          cy={y}
                          r="4"
                          fill={color}
                          stroke="white"
                          strokeWidth="2"
                        >
                          <title>{`${className} - Epoch ${h.epoch}: ${((h.classes?.[className] ?? 0) * 100).toFixed(1)}%`}</title>
                        </circle>
                      );
                    })}
                  </g>
                );
              })}

              {/* X-axis labels */}
              {history.map((h: EpochEntry, i: number) => {
                const x = padding.left + (i / (history.length - 1)) * plotWidth;
                return (
                  <g key={i}>
                    <text
                      x={x}
                      y={height - padding.bottom + 20}
                      textAnchor="middle"
                      className="text-xs fill-gray-600"
                    >
                      E{h.epoch}
                    </text>
                  </g>
                );
              })}

              {/* Axis labels */}
              <text
                x={width / 2}
                y={height - 10}
                textAnchor="middle"
                className="text-sm fill-gray-700 font-semibold"
              >
                Epochs
              </text>
              <text
                x={20}
                y={height / 2}
                textAnchor="middle"
                transform={`rotate(-90, 20, ${height / 2})`}
                className="text-sm fill-gray-700 font-semibold"
              >
                Probability
              </text>
            </svg>

            {/* Legend (hover a class to highlight) */}
            <div className="flex flex-wrap gap-4 justify-center mt-6">
              {allClasses.map((className: string, idx: number) => {
                const color = colors[idx % colors.length];
                const currentValue = (history[history.length - 1].classes?.[className] ?? 0);
                const isLegendActive = !hoveredClass || hoveredClass === className;
                return (
                  <div
                    key={className}
                    role="button"
                    tabIndex={0}
                    onMouseEnter={() => setHoveredClass(className)}
                    onMouseLeave={() => setHoveredClass(null)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        setHoveredClass(prev => (prev === className ? null : className));
                      }
                    }}
                    className={`flex items-center gap-2 cursor-pointer select-none ${isLegendActive ? '' : 'opacity-40'}`}
                  >
                    <div
                      className="w-4 h-4 rounded"
                      style={{ backgroundColor: color }}
                    ></div>
                    <span className="text-sm font-medium">{className}</span>
                    <span className="text-sm text-gray-500">
                      ({(currentValue * 100).toFixed(1)}%)
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </DialogContent>
      </Dialog>
    );
  }

  // Binary classifier line chart
  if (!classifier || !classifier.history) return null;

  const { history, score } = classifier as ClassifierEntry;
  const color = score > 0.7 ? '#10b981' : score > 0.4 ? '#f59e0b' : '#ef4444';

  const scores = history.map((h: EpochEntry) => h.score ?? 0);
  const minScore = Math.min(...scores);
  const maxScore = Math.max(...scores);
  const axisMin = 0;
  const axisMax = 1;
  const axisRange = axisMax - axisMin || 1;

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="w-[min(1200px,95vw)] max-w-none sm:!max-w-none max-h-[90vh] overflow-auto">
        <DialogHeader>
            <DialogTitle className="text-xl">
            {isMulticlass && multiclassData ? multiclassData.name : classifier.name} - Evolution Over Time
          </DialogTitle>
        </DialogHeader>

        <div className="mt-4">
          <svg width={width} height={height} className="mx-auto">
            {/* Y-axis grid lines */}
            {[0, 0.25, 0.5, 0.75, 1.0].map(t => {
              const val = axisMin + t * axisRange;
              const y = padding.top + plotHeight - ((val - axisMin) / axisRange) * plotHeight;
              return (
                <g key={`grid-y-${t}`}>
                  <line
                    x1={padding.left}
                    y1={y}
                    x2={width - padding.right}
                    y2={y}
                    stroke="#e5e7eb"
                    strokeWidth="1"
                    strokeDasharray="4"
                  />
                  <text
                    x={padding.left - 10}
                    y={y}
                    textAnchor="end"
                    dominantBaseline="middle"
                    className="text-xs fill-gray-600"
                  >
                    {(val * 100).toFixed(1)}%
                  </text>
                </g>
              );
            })}

            {/* Area fill */}
            <path
              d={`
                M ${padding.left} ${height - padding.bottom}
                ${history.map((h: EpochEntry, i: number) => {
                  const x = padding.left + (i / (history.length - 1)) * plotWidth;
                  const y = padding.top + plotHeight - (((h.score ?? 0) - axisMin) / axisRange) * plotHeight;
                  return `L ${x} ${y}`;
                }).join(' ')}
                L ${padding.left + plotWidth} ${height - padding.bottom}
                Z
              `}
              fill={color}
              opacity="0.2"
            />

            {/* Line */}
            <path
              d={history.map((h: EpochEntry, i: number) => {
                const x = padding.left + (i / (history.length - 1)) * plotWidth;
                const y = padding.top + plotHeight - (((h.score ?? 0) - axisMin) / axisRange) * plotHeight;
                return `${i === 0 ? 'M' : 'L'} ${x} ${y}`;
              }).join(' ')}
              fill="none"
              stroke={color}
              strokeWidth="3"
            />

            {/* Data points */}
            {history.map((h: EpochEntry, i: number) => {
              const x = padding.left + (i / (history.length - 1)) * plotWidth;
              const y = padding.top + plotHeight - (((h.score ?? 0) - axisMin) / axisRange) * plotHeight;
              return (
                <circle
                  key={i}
                  cx={x}
                  cy={y}
                  r="6"
                  fill={color}
                  stroke="white"
                  strokeWidth="2"
                >
                  <title>{`Epoch ${h.epoch}: ${((h.score ?? 0) * 100).toFixed(1)}%`}</title>
                </circle>
              );
            })}

            {/* X-axis labels */}
            {history.map((h: EpochEntry, i: number) => {
                const x = padding.left + (i / (history.length - 1)) * plotWidth;
                return (
                  <g key={i}>
                    <text
                      x={x}
                      y={height - padding.bottom + 20}
                      textAnchor="middle"
                      className="text-xs fill-gray-600"
                    >
                      E{h.epoch}
                    </text>
                  </g>
                );
              })}

            {/* Axis labels */}
            <text
              x={width / 2}
              y={height - 10}
              textAnchor="middle"
              className="text-sm fill-gray-700 font-semibold"
            >
              Epochs
            </text>
            <text
              x={20}
              y={height / 2}
              textAnchor="middle"
              transform={`rotate(-90, 20, ${height / 2})`}
              className="text-sm fill-gray-700 font-semibold"
            >
              Score
            </text>
          </svg>

          {/* Stats */}
          <div className="flex gap-6 justify-center mt-6 text-sm">
            <div>
              <span className="text-gray-600">Current: </span>
              <span className="font-bold" style={{ color }}>{(classifier.score * 100).toFixed(1)}%</span>
            </div>
            <div>
              <span className="text-gray-600">Min: </span>
              <span className="font-semibold">{(minScore * 100).toFixed(1)}%</span>
            </div>
            <div>
              <span className="text-gray-600">Max: </span>
              <span className="font-semibold">{(maxScore * 100).toFixed(1)}%</span>
            </div>
            <div>
              <span className="text-gray-600">Change: </span>
              <span className={`font-semibold ${
                scores[scores.length - 1] - scores[0] > 0 ? 'text-green-600' : 'text-red-600'
              }`}>
                {((scores[scores.length - 1] - scores[0]) * 100).toFixed(1)}%
              </span>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
};

const ClassifierDisplay = ({ alert }: { alert?: unknown }) => {
  const [viewMode, setViewMode] = useState<'current' | 'temporal'>('current');
  const [binaryFamily, setBinaryFamily] = useState<string>('all');
  // const [multiclassFamily, setMulticlassFamily] = useState<string>('AppleCider');
  const [dialogOpen, setDialogOpen] = useState<boolean>(false);
  const [helpDialogOpen, setHelpDialogOpen] = useState(false);
  const [selectedClassifier, setSelectedClassifier] = useState<ClassifierEntry | null>(null);
  const [selectedMulticlass, setSelectedMulticlass] = useState<MulticlassData | null>(null);

    // Map alert classifications to our data structure
      const classifierData = useMemo< { binary: BinaryFamilies; multiclass: Record<string, MulticlassData> }>(() => {
        const mapped = mapAlertClassifications(alert);
        // Merge with mock data for history (for demo purposes)
        const mockBinary = (mapped.binary as unknown) as BinaryFamilies;
        Object.keys(mockBinary).forEach((family: string) => {
          if (mapped.binary[family]) {
            mapped.binary[family] = mapped.binary[family].map((mc: ClassifierEntry) => {
              const mock = (mockBinary[family] || []).find((m: ClassifierEntry) => m.name === mc.name);
              return mock ? { ...mc, history: mock.history, isStatic: mock.isStatic } : mc;
            });
          }
        });
        return {
          binary: mapped.binary,
          multiclass: (mapped.multiclass as unknown) as Record<string, MulticlassData>, // No multiclass in alert yet
        };
      }, [alert]);

  const FAMILY_ORDER = ['drb', 'reliability', 'sgscore', 'LSPSC', 'BTSBot', 'ACAI'];
  const binaryFamilies = Object.keys(classifierData.binary).sort((a, b) => {
    const ai = FAMILY_ORDER.indexOf(a);
    const bi = FAMILY_ORDER.indexOf(b);
    if (ai === -1 && bi === -1) return a.localeCompare(b);
    if (ai === -1) return 1;
    if (bi === -1) return -1;
    return ai - bi;
  });

  const anomalies = detectAnomalies(classifierData.binary);
  const hasData = Object.values(classifierData.binary).flat().length > 0
    || Object.keys(classifierData.multiclass).length > 0;

  // Get classifiers to display based on selection
  const displayedBinaryClassifiers: ClassifierEntry[] = binaryFamily === 'all'
    ? binaryFamilies.flatMap(f => classifierData.binary[f])
    : (classifierData.binary[binaryFamily] ?? []);

  // Group families: consecutive singles are batched, multi-entry families get their own labeled group
  const binaryGroups: Array<{ label?: string; classifiers: ClassifierEntry[] }> = useMemo(() => {
    if (binaryFamily !== 'all') return [{ classifiers: displayedBinaryClassifiers }];
    const groups: Array<{ label?: string; classifiers: ClassifierEntry[] }> = [];
    let singlesBuffer: ClassifierEntry[] = [];
    for (const family of binaryFamilies) {
      const entries = classifierData.binary[family] ?? [];
      if (entries.length === 1) {
        singlesBuffer.push(...entries);
      } else {
        if (singlesBuffer.length > 0) {
          groups.push({ classifiers: singlesBuffer });
          singlesBuffer = [];
        }
        groups.push({ label: family, classifiers: entries });
      }
    }
    if (singlesBuffer.length > 0) groups.push({ classifiers: singlesBuffer });
    return groups;
  }, [binaryFamily, binaryFamilies, classifierData.binary, displayedBinaryClassifiers]);

  // Filter out static classifiers for temporal view
  const temporalBinaryClassifiers: ClassifierEntry[] = displayedBinaryClassifiers.filter((c) => !c.isStatic && c.history && c.history.length > 1);

  // Same grouping logic as binaryGroups but applied to temporal-eligible classifiers
  const temporalBinaryGroups: Array<{ label?: string; classifiers: ClassifierEntry[] }> = useMemo(() => {
    if (binaryFamily !== 'all') return [{ classifiers: temporalBinaryClassifiers }];
    const groups: Array<{ label?: string; classifiers: ClassifierEntry[] }> = [];
    let singlesBuffer: ClassifierEntry[] = [];
    for (const family of binaryFamilies) {
      const entries = (classifierData.binary[family] ?? []).filter((c) => !c.isStatic && c.history && c.history.length > 1);
      if (entries.length === 0) continue;
      if (entries.length === 1 && (classifierData.binary[family] ?? []).length === 1) {
        singlesBuffer.push(...entries);
      } else {
        if (singlesBuffer.length > 0) {
          groups.push({ classifiers: singlesBuffer });
          singlesBuffer = [];
        }
        groups.push({ label: family, classifiers: entries });
      }
    }
    if (singlesBuffer.length > 0) groups.push({ classifiers: singlesBuffer });
    return groups;
  }, [binaryFamily, binaryFamilies, classifierData.binary, temporalBinaryClassifiers]);

  const totalEpochs = Math.max(0, ...displayedBinaryClassifiers.map(c => c.history?.length ?? 0));
  const showTrend = viewMode === 'current' && totalEpochs > 1;

  return (
    <>
      <Card className="@container/card col-span-1 h-full bg-card text-card-foreground flex flex-col">
        <CardContent className="space-y-4 pt-0 flex-1 flex flex-col min-h-0">
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2 min-w-0">
              <CardTitle className="text-lg">ML Scores</CardTitle>
              <button
                onClick={() => setHelpDialogOpen(true)}
                title="Widget information"
                className="p-1 rounded hover:bg-slate-100 dark:hover:bg-slate-700"
              >
                <Info className="w-4 h-4 text-gray-500 dark:text-gray-400" />
              </button>
            </div>
            <div className="flex items-center gap-2 flex-shrink-0">
              <Select value={binaryFamily} onValueChange={setBinaryFamily}>
                <SelectTrigger className="w-32 h-7 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all" className="text-xs">All</SelectItem>
                  {binaryFamilies.map(family => (
                    <SelectItem key={family} value={family} className="text-xs">
                      {family}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          {/* Anomaly Alert */}
          {anomalies.length > 0 && (
            <Alert className="py-2">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription className="text-xs">
                {anomalies[0].message}
              </AlertDescription>
            </Alert>
          )}

          {!hasData ? (
            <div className="flex-1 flex items-center justify-center min-h-0">
              <div className="w-full h-full flex items-center justify-center text-md text-muted-foreground italic px-4 py-6 text-center bg-muted/30 rounded-lg border border-dashed border-muted-foreground/30">
                No data
              </div>
            </div>
          ) : (
            <>
              {/* View Toggle */}
              <Tabs value={viewMode} onValueChange={(v: string) => setViewMode(v as 'current' | 'temporal')} className="w-full">
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger value="current" className="text-xs">Current</TabsTrigger>
                  <TabsTrigger value="temporal" className="text-xs">Temporal</TabsTrigger>
                </TabsList>

                <TabsContent value="current" className="space-y-4 mt-1">
                  <div className="space-y-2">
                      {binaryGroups.map((group, gi) => (
                        <div key={gi}>
                          {group.label && (
                            <div className="text-xs font-medium text-gray-500 mb-1">{group.label}</div>
                          )}
                          <div className="grid grid-cols-2 gap-2">
                            {group.classifiers.map((c: ClassifierEntry) => (
                              <CompactHeatmap key={`${binaryFamily}-${c.name}`} {...c} showTrend={showTrend} />
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                </TabsContent>

                <TabsContent value="temporal" className="space-y-4 mt-1">
                  <div className="space-y-2">
                    {temporalBinaryClassifiers.length > 0 ? (
                      temporalBinaryGroups.map((group, gi) => (
                        <div key={gi}>
                          {group.label && (
                            <div className="text-xs font-medium text-gray-500 mb-1">{group.label}</div>
                          )}
                          <div className="grid grid-cols-2 gap-2">
                            {group.classifiers.map(c => (
                              <CompactSparkline
                                key={`${binaryFamily}-${c.name}`}
                                classifier={c}
                                onClick={() => {
                                  setSelectedClassifier(c);
                                  setSelectedMulticlass(null);
                                  setDialogOpen(true);
                                }}
                              />
                            ))}
                          </div>
                        </div>
                      ))
                    ) : (
                      <div className="text-xs text-muted-foreground italic py-4 text-center bg-muted/30 rounded-lg border border-dashed border-muted-foreground/30">
                        No time-variant classifiers available, for the selected classifier type(s).
                      </div>
                    )}
                  </div>
                </TabsContent>
              </Tabs>

              {/* Legend */}
              <div className="pt-2 border-t">
                <div className="flex items-center justify-between text-xs text-gray-500">
                  <div className="flex items-center gap-3">
                    <div className="flex items-center gap-1">
                      <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                      <span>High</span>
                    </div>
                    <div className="flex items-center gap-1">
                      <div className="w-2 h-2 bg-yellow-500 rounded-full"></div>
                      <span>Med</span>
                    </div>
                    <div className="flex items-center gap-1">
                      <div className="w-2 h-2 bg-red-500 rounded-full"></div>
                      <span>Low</span>
                    </div>
                  </div>
                  {showTrend && (
                    <div className="flex items-center gap-1 text-gray-400">
                      <TrendingUp className="w-3 h-3" />
                      <span>Trend</span>
                    </div>
                  )}
                </div>
              </div>
            </>
          )}
        </CardContent>
      </Card>

      {/* Time Series Dialog */}
      <TimeSeriesDialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        classifier={selectedClassifier}
        isMulticlass={!!selectedMulticlass}
        multiclassData={selectedMulticlass}
      />

      {/* Help Dialog */}
      <Dialog open={helpDialogOpen} onOpenChange={setHelpDialogOpen}>
        <DialogContent className="w-[min(1000px,95vw)] max-w-none sm:!max-w-none max-h-[90vh] overflow-auto">
          <DialogHeader>
            <DialogTitle className="text-xl">Understanding ML Classifiers</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 text-sm">
            <div>
              <h3 className="font-semibold mb-2">What This Widget Shows</h3>
              <p className="text-gray-600 dark:text-gray-300">
                This widget displays machine learning classification scores for the astronomical object. These scores represent
                the probability (0-100%) that the object belongs to specific categories based on various trained models.
                The widget shows both current scores and their evolution over time (if multiple epochs are available).
              </p>
            </div>

            <div>
              <h3 className="font-semibold mb-2">View Modes</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">Current:</span>
                  <span>Shows the latest classification scores as horizontal bars. Scores are color-coded (green for high confidence, yellow for medium, red for low).</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Temporal:</span>
                  <span>Displays how classifier scores have changed over time across multiple epochs. Click any classifier in Current view to see its detailed time series.</span>
                </div>
              </div>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Classifier Types</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">Binary:</span>
                  <span>These classifiers provide a single score indicating the likelihood of a specific property (e.g., "Real/Bogus" for artifact detection, "Hosted" for supernova-like events in host galaxies).</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Multiclass:</span>
                  <span>These classifiers output probabilities for multiple mutually exclusive categories (e.g., different supernova types, variable star classes). The sum of all classes equals 100%.</span>
                </div>
              </div>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Classifier Families</h3>
              <p className="text-gray-600 dark:text-gray-300 mb-2">
                Classifiers are organized into families (selectable via dropdown):
              </p>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">ACAI:</span>
                  <span>Alert Classification for Astrophysical Insights - classifies transients by context (hosted, nuclear, orphan, variable).</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">drb:</span>
                  <span>Deep-learning Real/Bogus classifier - distinguishes real astronomical sources from artifacts.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">sgscore/LSPSC:</span>
                  <span>Star-Galaxy Score - indicates whether nearby sources are point-like (stars) or extended (galaxies).
                  </span>
                </div>
              </div>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Visual Elements</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">Color coding:</span>
                  <span>Green (high confidence, &gt;60%), Yellow (medium, 30-60%), Red (low, &lt;30%).</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Trend arrows:</span>
                  <span>Show whether the score is increasing ↑, decreasing ↓, or stable — compared to the previous epoch.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Clock icon:</span>
                  <span>Indicates time-variant classifiers that update as new data becomes available.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Epoch badge:</span>
                  <span>Shows the number of times the object has been classified (as new observations arrive).</span>
                </div>
              </div>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Interactive Features</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">Click a classifier:</span>
                  <span>Opens a detailed time series plot showing how the score has evolved across all epochs.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Switch families:</span>
                  <span>Use the dropdown to view different classifier families (ACAI, drb, etc.).</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Toggle view mode:</span>
                  <span>Switch between Current (bar chart) and Temporal (time series overview) views.</span>
                </div>
              </div>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Anomaly Detection</h3>
              <p className="text-gray-600 dark:text-gray-300">
                When unusual patterns are detected (e.g., scores that sum to &gt;110% or significant unexpected changes),
                an alert banner appears at the top to highlight potential data quality issues.
              </p>
            </div>

            <div>
              <h3 className="font-semibold mb-2">Interpretation Tips</h3>
              <div className="space-y-2 text-gray-600 dark:text-gray-300">
                <div className="flex items-start gap-2">
                  <span className="font-medium">High drb score:</span>
                  <span>Indicates the detection is likely a real astronomical source, not an artifact.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Rising trends:</span>
                  <span>May indicate the object is becoming more consistent with a classification as more data arrives.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Multiclass consensus:</span>
                  <span>When one class has a much higher score than others, the classification is more confident.</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="font-medium">Competing scores:</span>
                  <span>Similar scores across multiple - conflicting - classes suggest the object's type is ambiguous or evolving.</span>
                </div>
              </div>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
};

export default ClassifierDisplay;
