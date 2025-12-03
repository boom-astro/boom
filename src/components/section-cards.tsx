// import Classifications from "@/components/Classifications";
import Header from "@/components/Header";
import Aladin from "@/components/Aladin";
import CrossmatchCard from "@/components/CrossmatchCard";
import CentroidPlot from "@/components/CentroidPlot";
import Lightcurve from "./Lightcurve";
import ClassifierDisplay from "./ClassificationsV2";
import { ApiObject } from '@/lib/api';
// import ClassificationsHistory from "./ClassificationsHistory";

export function SectionCards({data}: {data: ApiObject | null}) {
  if (!data) {
    return (
      <div className="grid grid-cols-1 gap-4 px-4 lg:px-6">
        <div className="p-6 rounded-lg border bg-card/50">No object loaded. Use the sign in form or click "Fetch latest" to load data.</div>
      </div>
    )
  }

  return (
    <div className="*:data-[slot=card]:from-primary/5 *:data-[slot=card]:to-card dark:*:data-[slot=card]:bg-card grid grid-cols-1 gap-4 px-4 *:data-[slot=card]:bg-gradient-to-t *:data-[slot=card]:shadow-xs lg:px-6 @xl/main:grid-cols-2 @5xl/main:grid-cols-4">
      <Header data={data} />
      <Lightcurve data={data} />
      {/* <Classifications alert={data} /> */}
      <CentroidPlot />
      <Aladin alert={data} />
      <ClassifierDisplay alert={data} />
      <CrossmatchCard />
      {/* <ClassificationsHistory alert={data} /> */}
    </div>
  )
}
