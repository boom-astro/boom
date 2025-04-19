import { IconTrendingDown, IconTrendingUp } from "@tabler/icons-react"

import { Badge } from "@/components/ui/badge"
import {
  Card,
  CardAction,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"

import Classifications from "@/components/Classifications";
import Header from "@/components/Header";
import Aladin from "@/components/Aladin";
import Lightcurve from "./Lightcurve";
import ClassificationsHistory from "./ClassificationsHistory";

export function SectionCards({data}: {data: any}) {
  return (
    <div className="*:data-[slot=card]:from-primary/5 *:data-[slot=card]:to-card dark:*:data-[slot=card]:bg-card grid grid-cols-1 gap-4 px-4 *:data-[slot=card]:bg-gradient-to-t *:data-[slot=card]:shadow-xs lg:px-6 @xl/main:grid-cols-2 @5xl/main:grid-cols-4">
      <Header data={data} />
      <Lightcurve data={data} />
      <Classifications alert={data} />
      <Aladin alert={data} />
      <ClassificationsHistory alert={data} />
    </div>
  )
}
