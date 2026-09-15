"use client"

// removed unused Radar imports

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  ChartConfig,
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart"

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { useEffect, useState } from "react"

import { Bar, BarChart, XAxis, YAxis, LabelList, CartesianGrid } from "recharts"


const chartConfig = {
  score: {
    label: "score",
    color: "hsl(var(--chart-1))",
  },
} satisfies ChartConfig

function classifications2chartData(
  classifications: Record<string, number>
): { model: string; score: number }[] {
  const data = Object.entries(classifications).map(([key, value]) => ({
    model: key,
    score: value,
  }));

  return data;
}

export default function Classifications({
  alert,
}: {
  alert: unknown
}) {
  const [selected, setSelected] = useState("all");
  const [chartData, setChartData] = useState<Array<{ model: string; score: number }>>([]);
  const alertObj = (alert ?? {}) as Record<string, unknown>;
  const classifications = (alertObj['classifications'] as Record<string, number> | undefined) ?? {};
  // run effect to prepare chart data
  useEffect(() => {
    let chartDataTemp = classifications2chartData(classifications);
    const candidate = (alertObj['candidate'] as Record<string, unknown> | undefined) ?? {};

    // we inject the drb from alert.candidate.drb
    chartDataTemp.push({
      model: "drb",
      score: Number(candidate['drb'] ?? 0),
    });

    // if alert.candidate.distpsnr1 < 2, add sgscore1
    if (Number(candidate['distpsnr1'] ?? 99) < 2) {
      chartDataTemp.push({
        model: "sgscore",
        score: Number(candidate['sgscore1'] ?? 0),
      });
    }

    // if selected = general, only keep drb, sgscore, btsbot
    // if selected = acai, only keep the classifiers that start with acai
    if (selected === "basic") {
      chartDataTemp = chartDataTemp.filter((item) => {
        return (
          item.model === "drb" ||
          item.model === "sgscore" ||
          item.model === "btsbot"
        );
      });
    } else if (selected === "acai") {
      chartDataTemp = chartDataTemp.filter((item) => {
        return item.model.startsWith("acai");
      });
    }

    // multiply the score by 100
    chartDataTemp = chartDataTemp.map((item) => {
      return {
        ...item,
        score: item.score * 100,
      };
    });

    // round it to 2 decimal places
    chartDataTemp = chartDataTemp.map((item) => {
      return {
        ...item,
        score: Math.round(item.score * 100) / 100,
      };
    });

    // set the chart data
    setChartData(chartDataTemp);
  }, [classifications, selected]);

  // if there are no keys, return null
  if (Object.keys(classifications).length === 0) {
    return (
      <Card>
        <CardHeader className="items-center">
          <CardTitle>Classifications</CardTitle>
        </CardHeader>
        <CardContent className="pb-0">
          <CardDescription>No classifications available</CardDescription>
        </CardContent>
      </Card>
    )

    }

  return (
    <Card className="@container/card">
      <CardHeader className="items-center flex flex-row justify-between">
        <CardTitle>Classifications</CardTitle>
        <Select onValueChange={setSelected} defaultValue="all" value={selected}>
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="Theme" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All</SelectItem>
            <SelectItem value="basic">Basic</SelectItem>
            <SelectItem value="acai">ACAI</SelectItem>
          </SelectContent>
        </Select>
      </CardHeader>
      <CardContent className="pb-0">
        <ChartContainer
          config={chartConfig}
          className="mx-auto pr-5 w-full pl-5 max-h-[250px] min-h-[250px]"
        >
          <BarChart
            accessibilityLayer
            data={chartData}
            layout="vertical"
            margin={{
              right: 60
            }}
          >
            <CartesianGrid vertical={true} horizontal={false}/>
            <XAxis type="number" dataKey="score" hide />
            <YAxis
              dataKey="model"
              type="category"
              tickLine={false}
              tickMargin={10}
              axisLine={false}
              tickFormatter={(value) => {
                return value
              }}
            />
            <ChartTooltip
              cursor={false}
              content={<ChartTooltipContent hideLabel />}
            />
            <Bar dataKey="score" fill="var(--chart-2)" radius={5}>
              <LabelList
                dataKey="score"
                position="right"
                offset={8}
                className="fill-foreground"
                fontSize={12}
                formatter={(value: number) => {
                  return `${value.toFixed(2)}%`
                }}
              />
            </Bar>
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  )
}
