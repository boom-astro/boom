"use client"
import { useEffect, useState } from "react"
import { CartesianGrid, Line, LineChart, XAxis } from "recharts"
import {
  Card,
  CardContent,
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

const chartConfig = {
    drb: {
      label: "drb",
      color: "hsl(var(--chart-1))",
    },
    acai_h: {
      label: "acai_h",
      color: "hsl(var(--chart-2))",
    },
    acai_n: {
      label: "acai_n",
      color: "hsl(var(--chart-3))",
    },
    acai_v: {
      label: "acai_v",
      color: "hsl(var(--chart-4))",
    },
  } satisfies ChartConfig

function jd2mjd(jd: number) {
    return jd - 2400000.5;
}

// Function to generate evenly spaced ticks
const generateTicks = (min: number, max: number, count: number) => {
    const ticks = [];
    const step = (max - min) / (count - 1);
    
    for (let i = 0; i < count; i++) {
        ticks.push(min + step * i);
    }
    
    return ticks;
};

// Custom formatter to limit decimal places
const formatNumber = (value: number) => {
    return value.toFixed(2);
};

function getArrayMinMax(arr: number[], field: string) {
    let min = arr[0][field];
    let max = arr[0][field];
    for (let i = 1; i < arr.length; i++) {
        if (arr[i][field] < min) {
            min = arr[i][field];
        }
        if (arr[i][field] > max) {
            max = arr[i][field];
        }
    }
    return { min, max };
}

function getChartData(
  classification_history: any,
): { mjd: number; drb: number, acai_h: number, acai_n: number, acai_v: number }[] {
    const chartData = classification_history.map((item: any) => {
      return {
        mjd: jd2mjd(item.jd),
        drb: item.drb * 100.0,
        acai_h: item.classifications.acai_h * 100.0,
        acai_n: item.classifications.acai_n * 100.0,
        acai_v: item.classifications.acai_v * 100.0,
      }
    });
    return chartData;
}

export default function ClassificationsHistory({
    alert,
  }: {
    alert: any
  }) {
    const [chartData, setChartData] = useState([]);
    const [selected, setSelected] = useState("all");

    useEffect(() => {
      const data = getChartData(alert.classification_history);
      setChartData(data);
    }, [alert]);

    let { min: min_mjd, max: max_mjd } = chartData?.length > 0 ? getArrayMinMax(chartData, "mjd") : { min: 0, max: 0 };
    const xTicks = generateTicks(min_mjd, max_mjd, 6); // 6 ticks on X-axis

  return (
    <Card className="@container/card col-span-2 lg:col-span-2">
      <CardHeader className="items-center flex flex-row justify-between">
        <CardTitle>Classifications History</CardTitle>
        <Select defaultValue="all" value={selected} onValueChange={setSelected}>
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="Model(s)" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All Models</SelectItem>
            <SelectItem value="drb">DRB</SelectItem>
            <SelectItem value="acai_h">ACAI H</SelectItem>
            <SelectItem value="acai_n">ACAI N</SelectItem>
            <SelectItem value="acai_v">ACAI V</SelectItem>
          </SelectContent>
        </Select>
      </CardHeader>
      <CardContent className="px-2 pt-4 sm:px-6 sm:pt-6">
      <ChartContainer config={chartConfig} className="mx-auto pr-5 w-full pl-5 max-h-[250px] min-h-[250px]">
          <LineChart
            accessibilityLayer
            data={chartData}
            margin={{
              left: 12,
              right: 12,
              top: 12,
            }}
          >
            <CartesianGrid vertical={true} horizontal={true}/>
            <XAxis
              dataKey="mjd"
              tickLine={false}
              axisLine={false}
              tickFormatter={formatNumber}
              ticks={xTicks}
              domain={[min_mjd - 1, max_mjd + 1]}
            />
            <ChartTooltip cursor={false} content={<ChartTooltipContent />} />
            {selected === "all" || selected === "drb" ? (
                <Line
                    dataKey="drb"
                    type="monotone"
                    stroke="var(--chart-1)"
                    strokeWidth={2}
                    dot={true}
                />
            ) : null}
            {selected === "all" || selected === "acai_h" ? (
                <Line
                    dataKey="acai_h"
                    type="monotone"
                    stroke="var(--chart-2)"
                    strokeWidth={2}
                    dot={true}
                />
            ) : null}
            {selected === "all" || selected === "acai_n" ? (
                <Line
                    dataKey="acai_n"
                    type="monotone"
                    stroke="var(--chart-3)"
                    strokeWidth={2}
                    dot={true}
                />
            ) : null}
            {selected === "all" || selected === "acai_v" ? (
                <Line
                    dataKey="acai_v"
                    type="monotone"
                    stroke="var(--chart-4)"
                    strokeWidth={2}
                    dot={true}
                />
            ) : null}
          </LineChart>
        </ChartContainer>
      </CardContent>
    </Card>
  )
}
