import { useMemo, useState } from 'react';
import { TrendingUp, TrendingDown, Minus, Clock, AlertTriangle } from 'lucide-react';
import { Card, CardContent, CardTitle } from '@/components/ui/card';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { Badge } from '@/components/ui/badge';
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
type ClassifierEntry = { name: string; score: number; history: EpochEntry[]; isStatic?: boolean; separation?: number };
type BinaryFamilies = Record<string, ClassifierEntry[]>;
type MulticlassData = { name: string; classes: Record<string, number>; history: EpochEntry[] };

type MapAlertResult = { binary: BinaryFamilies; multiclass: Record<string, MulticlassData> };


// Sample data with classifier families
const mockClassifierData = {
  binary: {
    'ACAI': [
      { 
        name: 'Hosted', 
        score: 0.82, 
        history: [
          { epoch: 1, score: 0.75, date: '2024-01-15' },
          { epoch: 2, score: 0.78, date: '2024-02-01' },
          { epoch: 3, score: 0.80, date: '2024-02-15' },
          { epoch: 4, score: 0.81, date: '2024-03-01' },
          { epoch: 5, score: 0.82, date: '2024-03-15' },
          { epoch: 6, score: 0.82, date: '2024-04-01' },
        ]
      },
      { 
        name: 'Nuclear', 
        score: 0.15, 
        history: [
          { epoch: 1, score: 0.22, date: '2024-01-15' },
          { epoch: 2, score: 0.20, date: '2024-02-01' },
          { epoch: 3, score: 0.18, date: '2024-02-15' },
          { epoch: 4, score: 0.16, date: '2024-03-01' },
          { epoch: 5, score: 0.15, date: '2024-03-15' },
          { epoch: 6, score: 0.15, date: '2024-04-01' },
        ]
      },
      { 
        name: 'Variable', 
        score: 0.08, 
        history: [
          { epoch: 1, score: 0.12, date: '2024-01-15' },
          { epoch: 2, score: 0.11, date: '2024-02-01' },
          { epoch: 3, score: 0.10, date: '2024-02-15' },
          { epoch: 4, score: 0.09, date: '2024-03-01' },
          { epoch: 5, score: 0.08, date: '2024-03-15' },
          { epoch: 6, score: 0.08, date: '2024-04-01' },
        ]
      },
      { 
        name: 'Orphan', 
        score: 0.03,
        history: [
          { epoch: 1, score: 0.05, date: '2024-01-15' },
          { epoch: 2, score: 0.04, date: '2024-02-01' },
          { epoch: 3, score: 0.04, date: '2024-02-15' },
          { epoch: 4, score: 0.03, date: '2024-03-01' },
          { epoch: 5, score: 0.03, date: '2024-03-15' },
          { epoch: 6, score: 0.03, date: '2024-04-01' },
        ]
      },
    ],
    'drb': [
      { 
        name: 'Real/Bogus', 
        score: 0.98,
        history: [
          { epoch: 1, score: 0.96, date: '2024-01-15' },
          { epoch: 2, score: 0.97, date: '2024-02-01' },
          { epoch: 3, score: 0.97, date: '2024-02-15' },
          { epoch: 4, score: 0.98, date: '2024-03-01' },
          { epoch: 5, score: 0.98, date: '2024-03-15' },
          { epoch: 6, score: 0.98, date: '2024-04-01' },
        ]
      },
    ],
    'sgscore': [
      { 
        name: 'Star/Galaxy', 
        score: 0.23,
        isStatic: true, // Spatial classifier, not time-variant
        history: [
          { epoch: 1, score: 0.23, date: '2024-01-15' },
          { epoch: 2, score: 0.23, date: '2024-02-01' },
          { epoch: 3, score: 0.23, date: '2024-02-15' },
          { epoch: 4, score: 0.23, date: '2024-03-01' },
          { epoch: 5, score: 0.23, date: '2024-03-15' },
          { epoch: 6, score: 0.23, date: '2024-04-01' },
        ]
      },
    ],
  },
  multiclass: {
    'AppleCider': {
      name: 'Type Classification',
      classes: {
        'SNIa': 0.78,
        'SNII': 0.12,
        'AGN': 0.06,
        'TDE': 0.03,
        'CV': 0.01
      },
      history: [
        { 
          epoch: 1, 
          date: '2024-01-15',
          classes: { 'SNIa': 0.45, 'SNII': 0.35, 'AGN': 0.12, 'TDE': 0.05, 'CV': 0.03 }
        },
        { 
          epoch: 2, 
          date: '2024-02-01',
          classes: { 'SNIa': 0.58, 'SNII': 0.25, 'AGN': 0.10, 'TDE': 0.04, 'CV': 0.03 }
        },
        { 
          epoch: 3, 
          date: '2024-02-15',
          classes: { 'SNIa': 0.67, 'SNII': 0.18, 'AGN': 0.09, 'TDE': 0.04, 'CV': 0.02 }
        },
        { 
          epoch: 4, 
          date: '2024-03-01',
          classes: { 'SNIa': 0.73, 'SNII': 0.14, 'AGN': 0.08, 'TDE': 0.03, 'CV': 0.02 }
        },
        { 
          epoch: 5, 
          date: '2024-03-15',
          classes: { 'SNIa': 0.76, 'SNII': 0.13, 'AGN': 0.07, 'TDE': 0.03, 'CV': 0.01 }
        },
        { 
          epoch: 6, 
          date: '2024-04-01',
          classes: { 'SNIa': 0.78, 'SNII': 0.12, 'AGN': 0.06, 'TDE': 0.03, 'CV': 0.01 }
        },
      ]
    }
  }
};

// alert.classifications actually just looks like this at the moment:
// {
//   "acai_h": 0.82,
//   "acai_n": 0.15,
//   "acai_v": 0.08,
//   "acai_o": 0.03,
// }
// and drb and sgscore similarly, but from the alert.candidate field
// so let's build a mapping function to convert that into the above structure
type AlertLike = {
  classifications?: Record<string, number>;
  candidate?: { drb?: number; sgscore1?: number; distpsnr1?: number };
};

function mapAlertClassifications(alert: unknown): MapAlertResult {
  const mapped: MapAlertResult = { binary: {}, multiclass: {} };
  if (!alert) return mapped;
  const a = alert as AlertLike;

  if (a.classifications) {
    mapped.binary['ACAI'] = [
      { name: 'Hosted', score: a.classifications.acai_h ?? 0, history: [] },
      { name: 'Nuclear', score: a.classifications.acai_n ?? 0, history: [] },
      { name: 'Variable', score: a.classifications.acai_v ?? 0, history: [] },
      { name: 'Orphan', score: a.classifications.acai_o ?? 0, history: [] },
    ];
  }

  if (a.candidate?.drb !== undefined) {
    mapped.binary['drb'] = [
      { name: 'Real/Bogus', score: a.candidate.drb ?? 0, history: [] },
    ];
  }

  if (a.candidate?.sgscore1 !== undefined) {
    mapped.binary['sgscore'] = [
      {
        name: 'Star/Galaxy',
        score: a.candidate.sgscore1 ?? 0,
        isStatic: true,
        history: [],
        separation: a.candidate.distpsnr1,
      },
    ];
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

const CompactHeatmap = ({ name, score, showTrend, history, isStatic, separation }: { name: string; score: number; showTrend?: boolean; history: EpochEntry[]; isStatic?: boolean; separation?: number }) => {
  const bgColor = score > 0.7 ? 'bg-green-500' : score > 0.4 ? 'bg-yellow-500' : 'bg-red-500';
  const trend = history && history.length > 1 ? (history[history.length - 1].score ?? 0) - (history[0].score ?? 0) : 0;
  const TrendIcon = trend > 0.1 ? TrendingUp : trend < -0.1 ? TrendingDown : Minus;
  const trendColor = trend > 0.1 ? 'text-green-100' : trend < -0.1 ? 'text-red-100' : 'text-white/60';
  
  return (
    <div className={`${bgColor} text-white p-3 rounded-lg relative space-between flex flex-col h-full`}>
      <div className="flex items-start justify-between mb-1.5">
        <div className="text-sm font-semibold truncate flex-1 pr-2">{name}</div>
        {showTrend && history && history.length > 1 && !isStatic && (
          <TrendIcon className={`w-3.5 h-3.5 flex-shrink-0 ${trendColor}`} />
        )}
        {isStatic && (
            // <Tooltip content="This score comes from another catalog (by spatial matching) and does not change over time.">
            <Tooltip>
                <TooltipTrigger className="absolute top-1.5 right-1.5 px-1.5 py-0">
                    <Badge variant="secondary" className="text-xs bg-white/20 text-white border-0">
                        Static
                    </Badge>
                </TooltipTrigger>
                <TooltipContent>
                This score comes from another catalog (by spatial matching) and does not change over time.
                </TooltipContent>
            </Tooltip>
        )}
      </div>
      <div className="flex items-end justify-between">
        <div className="text-2xl font-bold tabular-nums">{(score * 100).toFixed(0)}%</div>
        {separation !== undefined && (
            <div className="text-s text-white/90 mb-0.95 text-right">
                {separation < 60 ? `${separation.toFixed(1)}″` : `${(separation / 60).toFixed(2)}′`}
            </div>
        )}
      </div>
    </div>
  );
};

const CompactSparkline = ({ classifier, onClick }: { classifier: ClassifierEntry; onClick?: () => void }) => {
  const { name, score, history } = classifier;
  const color = score > 0.7 ? '#10b981' : score > 0.4 ? '#f59e0b' : '#ef4444';
  
  const sparklineData = history.map((h: EpochEntry) => h.score ?? 0);
  const maxY = Math.max(...sparklineData);
  const minY = Math.min(...sparklineData);
  const range = maxY - minY || 0.1;
  
  const sparklinePath = sparklineData.map((value: number, i: number) => {
    const x = (i / (sparklineData.length - 1)) * 100;
    const y = 24 - ((value - minY) / range) * 20;
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
      <div className="flex items-start justify-between mb-1.5">
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
          const x = (i / (sparklineData.length - 1)) * 100;
          const y = 24 - ((value - minY) / range) * 20;
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

const MulticlassHeatmap = ({ classes }: { classes: Record<string, number> }) => {
  const cls = classes;
  const sortedClasses = Object.entries(cls).sort((a, b) => b[1] - a[1]);
  const maxScore = Math.max(...Object.values(cls));
  
  return (
    <div className="grid grid-cols-2 gap-2">
      {sortedClasses.map(([className, score]) => {
        const intensity = score / maxScore;
        const bgColor = `rgba(59, 130, 246, ${intensity * 0.7 + 0.3})`;
        const textColor = intensity > 0.5 ? 'text-white' : 'text-gray-800';
        
        return (
          <div 
            key={className}
            className={`px-2.5 py-2 rounded-lg ${textColor} transition-all`}
            style={{ backgroundColor: bgColor }}
          >
            <div className="text-xs font-medium truncate mb-0.5">{className}</div>
            <div className="text-lg font-bold tabular-nums">{(score * 100).toFixed(0)}%</div>
          </div>
        );
      })}
    </div>
  );
};

const MulticlassSparkline = ({ className, history, onClick }: { className: string; history: EpochEntry[]; onClick?: () => void }) => {
  const scores = history.map((h: EpochEntry) => (h.classes ? (h.classes[className] ?? 0) : 0));
  const currentScore = scores[scores.length - 1];
  const color = '#3b82f6';
  
  const maxY = Math.max(...scores);
  const minY = Math.min(...scores);
  const range = maxY - minY || 0.1;
  
  const sparklinePath = scores.map((value: number, i: number) => {
    const x = (i / (scores.length - 1)) * 100;
    const y = 24 - ((value - minY) / range) * 20;
    return `${i === 0 ? 'M' : 'L'} ${x} ${y}`;
  }).join(' ');
  
  const trend = scores[scores.length - 1] - scores[0];
  const TrendIcon = trend > 0.05 ? TrendingUp : trend < -0.05 ? TrendingDown : Minus;
  const trendColor = trend > 0.05 ? 'text-green-600' : trend < -0.05 ? 'text-red-600' : 'text-gray-400';
  
  return (
    <button 
      onClick={onClick}
      className="py-2 px-2.5 rounded-lg text-left transition-colors w-full border-1 hover:border-gray-300"
    >
      <div className="flex items-start justify-between mb-1.5">
        <h4 className="text-xs font-semibold text-blue-900 truncate flex-1">{className}</h4>
        <div className="flex items-center gap-1 flex-shrink-0 ml-1">
          <TrendIcon className={`w-3 h-3 ${trendColor}`} />
          <div className="text-base font-bold tabular-nums" style={{ color }}>
            {(currentScore * 100).toFixed(0)}%
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
        {scores.map((value: number, i: number) => {
          const x = (i / (scores.length - 1)) * 100;
          const y = 24 - ((value - minY) / range) * 20;
          const isLast = i === scores.length - 1;
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
    // Compute data range across all classes so the y-axis reflects actual min/max
    const allValues = history.flatMap((h: EpochEntry) => Object.values(h.classes ?? {})) as number[];
    const minY = Math.min(...allValues);
    const maxY = Math.max(...allValues);
    const range = maxY - minY || 0.01;
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
                  const y = padding.top + plotHeight - ((h.classes?.[className] ?? 0) * plotHeight);
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
                      const y = padding.top + plotHeight - ((h.classes?.[className] ?? 0) * plotHeight);
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
                    <text
                      x={x}
                      y={height - padding.bottom + 35}
                      textAnchor="middle"
                      className="text-xs fill-gray-400"
                    >
                      {h.date ? h.date.slice(5) : ''}
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
  const maxY = Math.max(...scores);
  const minY = Math.min(...scores);
  const range = maxY - minY || 0.1;
  
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
            
            {/* Area fill */}
            <path
              d={`
                M ${padding.left} ${height - padding.bottom}
                ${history.map((h: EpochEntry, i: number) => {
                  const x = padding.left + (i / (history.length - 1)) * plotWidth;
                  const y = padding.top + plotHeight - (((h.score ?? 0) - minY) / range) * plotHeight;
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
                const y = padding.top + plotHeight - (((h.score ?? 0) - minY) / range) * plotHeight;
                return `${i === 0 ? 'M' : 'L'} ${x} ${y}`;
              }).join(' ')}
              fill="none"
              stroke={color}
              strokeWidth="3"
            />
            
            {/* Data points */}
            {history.map((h: EpochEntry, i: number) => {
              const x = padding.left + (i / (history.length - 1)) * plotWidth;
              const y = padding.top + plotHeight - (((h.score ?? 0) - minY) / range) * plotHeight;
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
                    <text
                      x={x}
                      y={height - padding.bottom + 35}
                      textAnchor="middle"
                      className="text-xs fill-gray-400"
                    >
                      {h.date ? h.date.slice(5) : ''}
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
              <span className="font-semibold">{(minY * 100).toFixed(1)}%</span>
            </div>
            <div>
              <span className="text-gray-600">Max: </span>
              <span className="font-semibold">{(maxY * 100).toFixed(1)}%</span>
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
  const [multiclassFamily, setMulticlassFamily] = useState<string>('AppleCider');
  const [dialogOpen, setDialogOpen] = useState<boolean>(false);
  const [selectedClassifier, setSelectedClassifier] = useState<ClassifierEntry | null>(null);
  const [selectedMulticlass, setSelectedMulticlass] = useState<MulticlassData | null>(null);

    // Map alert classifications to our data structure
      const classifierData = useMemo< { binary: BinaryFamilies; multiclass: Record<string, MulticlassData> }>(() => {
        const mapped = mapAlertClassifications(alert);
        // Merge with mock data for history (for demo purposes)
        const mockBinary = (mockClassifierData.binary as unknown) as BinaryFamilies;
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
          multiclass: (mockClassifierData.multiclass as unknown) as Record<string, MulticlassData>, // No multiclass in alert yet
        };
      }, [alert]);
  
  const binaryFamilies = Object.keys(classifierData.binary);
  const multiclassFamilies = Object.keys(classifierData.multiclass);
  
  const anomalies = detectAnomalies(classifierData.binary);
  
  // Get classifiers to display based on selection
  const displayedBinaryClassifiers: ClassifierEntry[] = binaryFamily === 'all'
    ? Object.values(classifierData.binary).flat()
    : (classifierData.binary[binaryFamily] ?? []);

  // Filter out static classifiers for temporal view
  const temporalBinaryClassifiers: ClassifierEntry[] = displayedBinaryClassifiers.filter((c) => !c.isStatic);

  const displayedMulticlass: MulticlassData = classifierData.multiclass[multiclassFamily] ?? { name: '', classes: {}, history: [] };
  const sortedClasses = Object.entries(displayedMulticlass.classes ?? {}).sort((a, b) => b[1] - a[1]);

  const totalEpochs = displayedBinaryClassifiers[0]?.history?.length ?? 0;
  const showTrend = viewMode === 'current' && totalEpochs > 1;
  
  return (
    <>
      <Card className="@container/card col-span-1 row-span-2">
        {/* <CardHeader className="pt-0 pb-0">
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg">ML Classifiers</CardTitle>
            {totalEpochs > 0 && (
              <Badge variant="outline" className="text-xs">
                {totalEpochs} epochs
              </Badge>
            )}
          </div>
        </CardHeader> */}
        <CardContent className="space-y-4 pt-0">
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg">ML Classifiers</CardTitle>
            {totalEpochs > 0 && (
              <Badge variant="outline" className="text-xs">
                {totalEpochs} epochs
              </Badge>
            )}
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
          
          {/* View Toggle */}
          <Tabs value={viewMode} onValueChange={(v: string) => setViewMode(v as 'current' | 'temporal')} className="w-full">
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="current" className="text-xs">Current</TabsTrigger>
              <TabsTrigger value="temporal" className="text-xs">Temporal</TabsTrigger>
            </TabsList>
            
            <TabsContent value="current" className="space-y-4 mt-1">
              {/* Binary Classifiers */}
              <div className="space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <h3 className="text-sm font-semibold text-gray-700">Binary</h3>
                  <Select value={binaryFamily} onValueChange={setBinaryFamily}>
                    <SelectTrigger className="w-32 h-8 text-xs">
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
                
                <div className="grid grid-cols-2 gap-2">
                  {displayedBinaryClassifiers.map((c: ClassifierEntry) => (
                    <CompactHeatmap key={`${binaryFamily}-${c.name}`} {...c} showTrend={showTrend} />
                  ))}
                </div>
              </div>
              
              {/* Multi-class Classifiers */}
              <div className="space-y-2 pt-2 border-t">
                <div className="flex items-center justify-between gap-3">
                  <h3 className="text-sm font-semibold text-gray-700">Multi-class</h3>
                  {multiclassFamilies.length > 0 && (
                    <Select value={multiclassFamily} onValueChange={setMulticlassFamily}>
                      <SelectTrigger className="w-36 h-8 text-xs">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {multiclassFamilies.map(family => (
                          <SelectItem key={family} value={family} className="text-xs">
                            {family}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  )}
                </div>
                
                <div className="text-xs text-gray-500 mb-2">{displayedMulticlass.name}</div>
                
                <MulticlassHeatmap classes={displayedMulticlass.classes} />
              </div>
            </TabsContent>
            
            <TabsContent value="temporal" className="space-y-4 mt-3">
              {/* Binary Classifiers */}
              <div className="space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <h3 className="text-sm font-semibold text-gray-700">Binary</h3>
                  <Select value={binaryFamily} onValueChange={setBinaryFamily}>
                    <SelectTrigger className="w-32 h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all" className="text-xs">All Families</SelectItem>
                      {binaryFamilies.map(family => (
                        <SelectItem key={family} value={family} className="text-xs">
                          {family}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                
                {temporalBinaryClassifiers.length > 0 ? (
                  <div className="grid grid-cols-2 gap-2">
                    {temporalBinaryClassifiers.map(c => (
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
                ) : (
                  <div className="text-xs text-gray-500 italic py-4 text-center bg-gray-50 rounded-lg">
                    No time-variant classifiers available <br />(for the selected classifier type).
                  </div>
                )}
              </div>
              
              {/* Multi-class */}
              <div className="space-y-2 pt-2 border-t">
                <div className="flex items-center justify-between gap-3">
                  <h3 className="text-sm font-semibold text-gray-700">Multi-class</h3>
                  {multiclassFamilies.length > 0 && (
                    <Select value={multiclassFamily} onValueChange={setMulticlassFamily}>
                      <SelectTrigger className="w-36 h-8 text-xs">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {multiclassFamilies.map(family => (
                          <SelectItem key={family} value={family} className="text-xs">
                            {family}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  )}
                </div>
                
                <div className="text-xs text-gray-500 mb-2">{displayedMulticlass.name}</div>
                
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                  {sortedClasses.map(([className]) => (
                    <MulticlassSparkline 
                      key={className}
                      className={className}
                      history={displayedMulticlass.history}
                      onClick={() => {
                        setSelectedClassifier(null);
                        setSelectedMulticlass(displayedMulticlass);
                        setDialogOpen(true);
                      }}
                    />
                  ))}
                </div>
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
    </>
  );
};

export default ClassifierDisplay;