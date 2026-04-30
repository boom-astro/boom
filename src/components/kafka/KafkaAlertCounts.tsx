import type { TopicInfo } from "@/lib/api";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import { KAFKA_TOPICS, SURVEYS } from "@/lib/utils";

const NO_MATCH_COLOR = "var(--chart-4)";
const MATCH_COLOR = "var(--chart-3)";

const isMatchTopic = (name: string) => !name.split(".")[2].startsWith("no-");

export default function KafkaAlertCounts({ topics, loading, error, splitByMatch }: {
  topics: TopicInfo[];
  loading: boolean;
  error: string | null;
  splitByMatch: boolean;
}) {
  if (error) return <p className="text-sm text-destructive">{error}</p>;

  if (loading) return (
    <>{[1, 2].map((i) => <Card key={i} className="h-48 shimmer"/>)}</>
  );

  if (!topics.length) return (
    <Card>
      <CardContent className="py-8 text-center text-muted-foreground">
        No topics available.
      </CardContent>
    </Card>
  );

  const countByName = new Map(topics.map((t) => [t.name, t.n_alerts]));
  const retentionDays = topics[0]?.retention_days;
  const keyOf = (name: string) => {
    if (splitByMatch) return name;
    const [prefix, survey, , classification] = name.split(".");
    return `${prefix}.${survey}.*.${classification}`;
  };

  return (
    <>
      {SURVEYS.map((survey) => {
        const surveyTopics = KAFKA_TOPICS.filter((t) => t.split(".")[1] === survey);
        const groups = new Map<string, { matched: number; noMatch: number }>();
        for (const name of surveyTopics) {
          const k = keyOf(name);
          const count = countByName.get(name) ?? 0;
          const prev = groups.get(k) ?? { matched: 0, noMatch: 0 };
          if (isMatchTopic(name)) {
            groups.set(k, { matched: prev.matched + count, noMatch: prev.noMatch });
          } else {
            groups.set(k, { matched: prev.matched, noMatch: prev.noMatch + count });
          }
        }

        if (!groups.size) return null;
        const rows = [...groups];
        const total = rows.reduce((s, [, { matched, noMatch }]) => s + matched + noMatch, 0);

        const crossSurvey = survey === "ztf" ? "LSST" : "ZTF";

        return (
          <Card key={survey}>
            <CardHeader>
              <div className="flex items-start justify-between gap-4">
                <div>
                  <CardTitle>{survey.toUpperCase()}</CardTitle>
                  <CardDescription>
                    {surveyTopics.length} topics — {total.toLocaleString()} alerts
                    {retentionDays !== undefined && ` — ${retentionDays}-day retention`}
                  </CardDescription>
                </div>
                {!splitByMatch && (
                  <div className="flex items-center gap-3 text-xs text-muted-foreground shrink-0 pt-0.5">
                    <span className="flex items-center gap-1.5">
                      <span className="inline-block w-3 h-3 rounded-sm shrink-0" style={{ backgroundColor: NO_MATCH_COLOR }} />
                      No {crossSurvey} match
                    </span>
                    <span className="flex items-center gap-1.5">
                      <span className="inline-block w-3 h-3 rounded-sm shrink-0" style={{ backgroundColor: MATCH_COLOR }} />
                      With {crossSurvey} match
                    </span>
                  </div>
                )}
              </div>
            </CardHeader>
            <CardContent className="space-y-3">
              {rows.map(([name, { matched, noMatch }]) => {
                const n = matched + noMatch;
                const pct = total > 0 ? (n / total) * 100 : 0;
                return (
                  <div key={name} className="flex items-center gap-3">
                    <code className="text-xs text-muted-foreground w-64 shrink-0 truncate" title={name}>
                      {name}
                    </code>
                    <div className="flex-1 h-5 rounded bg-muted overflow-hidden">
                      <div className="h-full flex" style={{ width: `${pct}%` }}>
                        {splitByMatch ? (
                          <div className="h-full w-full" style={{ backgroundColor: isMatchTopic(name) ? MATCH_COLOR : NO_MATCH_COLOR }} />
                        ) : (
                          <>
                            {noMatch > 0 && (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <div
                                    className="h-full"
                                    style={{
                                      width: `${(noMatch / n) * 100}%`,
                                      backgroundColor: NO_MATCH_COLOR,
                                    }}
                                  />
                                </TooltipTrigger>
                                <TooltipContent>
                                  Without {crossSurvey} matches: {noMatch.toLocaleString()} ({((noMatch / n) * 100).toFixed(1)}%)
                                </TooltipContent>
                              </Tooltip>
                            )}
                            {matched > 0 && (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <div
                                    className="h-full"
                                    style={{
                                      width: `${(matched / n) * 100}%`,
                                      backgroundColor: MATCH_COLOR,
                                    }}
                                  />
                                </TooltipTrigger>
                                <TooltipContent>
                                  With {crossSurvey} matches: {matched.toLocaleString()} ({((matched / n) * 100).toFixed(1)}%)
                                </TooltipContent>
                              </Tooltip>
                            )}
                          </>
                        )}
                      </div>
                    </div>
                    <span className="text-xs tabular-nums w-20 text-right">
                      {n.toLocaleString()}
                    </span>
                  </div>
                );
              })}
            </CardContent>
          </Card>
        );
      })}
    </>
  );
}
