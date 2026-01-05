"use client"

import { bytes2image } from '@/lib/imageProcessing';

import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
    CardFooter,
} from "@/components/ui/card"

import { toast } from "sonner"

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"

import dayjs from "dayjs";

import utc from "dayjs/plugin/utc";
import relativeTime from "dayjs/plugin/relativeTime";
import { useState, useMemo } from "react";
import { Badge } from "@/components/ui/badge"
import { Button } from './ui/button';
import { IconGalaxy, IconMeteor, IconSparkles, IconStar, IconStars } from '@tabler/icons-react';
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from "@/components/ui/dialog"


dayjs.extend(utc);
dayjs.extend(relativeTime);


type Detection = {
  jd?: number;
  magpsf?: number;
  sigmapsf?: number;
  band?: string;
  diffmaglim?: number;
};

type CandidateData = {
  object_id?: string;
  candidate?: { ra?: number; dec?: number; drb?: number; ndethist?: number };
  classifications?: Record<string, number>;
  properties?: { star?: boolean };
  cutout_science?: Uint8Array | string | ArrayBuffer | undefined;
  cutout_template?: Uint8Array | string | ArrayBuffer | undefined;
  cutout_difference?: Uint8Array | string | ArrayBuffer | undefined;
  prv_candidates?: Detection[];
  prv_nondetections?: Detection[];
};

function mjd_to_utc(mjd: number) {
  // Take a MJD string and return UTC time
  return dayjs
    .unix((mjd - 40587) * 86400.0)
    .utc()
    .format();
}

function jd_to_mjd(jd: number) {
  return jd - 2400000.5;
}

export function ClassificationBadges({
  data,
}: {
  data: CandidateData;
}) {
  const classifications = data.classifications ?? {};
  const properties = data.properties ?? {};
  return (
    <div className="flex flex-row flex-wrap gap-2">
      {(data.candidate?.drb ?? 1) < 0.2 && (
        <Badge variant="outline" className="text-sm font-semibold">
          Bogus?
        </Badge>
      )}
      {(classifications['acai_h'] ?? 0) > 0.8 && (
        <Badge variant="outline" className="text-sm font-semibold">
          Hosted
        </Badge>
      )}
      {(classifications['acai_n'] ?? 0) > 0.8 && (
        <Badge variant="outline" className="text-sm font-semibold">
          Nuclear
        </Badge>
      )}
      {((classifications['btsbot'] ?? 0) > 0.8) || ((classifications['acai_h'] ?? 0) > 0.8 && !properties?.star && ((data.candidate?.ndethist ?? 0) < 200)) && (
        <Badge variant="outline" className="text-sm font-semibold">
          SN?
        </Badge>
      )}
      {properties?.star && (
        <Badge variant="outline" className="text-sm font-semibold">
          Stellar
        </Badge>
      )}
    </div>
  )
}

function formatDetection(det: Detection | null): string {
  if (!det) return "-";
  const mag = det.magpsf;
  const sig = det.sigmapsf;
  if (mag == null || sig == null) return "-";
  return `${mag.toFixed(2)}±${sig.toFixed(2)}`;
}

export default function Header({
    data,
  }: {
    data: CandidateData;
  }) {
    const [lightboxOpen, setLightboxOpen] = useState(false);
    const [band, setBand] = useState<string>("all");

    const objectId = data.object_id ?? "";
    const ra = data.candidate?.ra?.toFixed(6) ?? "-";
    const dec = data.candidate?.dec?.toFixed(6) ?? "-";

    const prvCandidates: Detection[] = data.prv_candidates ?? [];
    const prvNonDetections: Detection[] = data.prv_nondetections ?? [];

    const filteredCandidates = useMemo(() => {
      if (!band || band === "all") return prvCandidates;
      return prvCandidates.filter((c: Detection) => ((c.band ?? "")).toLowerCase() === band.toLowerCase());
    }, [band, prvCandidates]);

    const filteredNonDetections = useMemo(() => {
      if (!band || band === "all") return prvNonDetections;
      return prvNonDetections.filter((c: Detection) => ((c.band ?? "")).toLowerCase() === band.toLowerCase());
    }, [band, prvNonDetections]);

    const nb_detections = filteredCandidates.length;
    const nb_nondetections = filteredNonDetections.length;

    // let first_det = filteredCandidates.length ? filteredCandidates.reduce((a: any, b: any) => a.jd < b.jd ? a : b) : null;
    // let last_det = filteredCandidates.length ? filteredCandidates.reduce((a: any, b: any) => a.jd > b.jd ? a : b) : null;
    const first_det = useMemo(() => {
      if (filteredCandidates.length === 0) return null;
      return filteredCandidates.reduce((a: Detection, b: Detection) => ( (a.jd ?? Infinity) < (b.jd ?? Infinity) ? a : b));
    }, [filteredCandidates]);
    const last_det = useMemo(() => {
      if (filteredCandidates.length === 0) return null;
      return filteredCandidates.reduce((a: Detection, b: Detection) => ( (a.jd ?? -Infinity) > (b.jd ?? -Infinity) ? a : b));
    }, [filteredCandidates]);
    const peak_det = useMemo(() => {
      if (filteredCandidates.length === 0) return null;
      return filteredCandidates.reduce((a: Detection, b: Detection) => ((a.magpsf ?? Infinity) < (b.magpsf ?? Infinity) ? a : b));
    }, [filteredCandidates]);
    const age = (first_det && last_det && first_det.jd != null && last_det.jd != null) ? Math.round((last_det.jd - first_det.jd) * 100) / 100 : "-";

    const scienceImage = bytes2image(data.cutout_science, "science");
    const templateImage = bytes2image(data.cutout_template, "template");
    const differenceImage = bytes2image(data.cutout_difference, "difference");

    function openLightbox() {
      setLightboxOpen(true);
    }

    return (
      <Card className="@container/card col-span-2 row-span-2">
        <CardHeader>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">{objectId}</CardTitle>
          <CardDescription>
            <ClassificationBadges data={data} />
          </CardDescription>
        </CardHeader>
        <CardContent className="pb-0 flex flex-col gap-4">
            <div className="grid grid-cols-3 gap-4">
              <div key="science" className="w-full relative group">
                <button onClick={openLightbox} className="w-full h-full text-left">
                  <img src={scienceImage ?? undefined} alt="Science" className="w-full h-auto object-cover rounded" style={{ imageRendering: 'pixelated' }}/>
                </button>
                <span className="absolute top-2 left-2 bg-black/60 text-white text-sm px-3 py-1 rounded-md backdrop-blur-sm opacity-100 group-hover:opacity-0 transition-opacity duration-150 pointer-events-none">Science</span>
              </div>
              <div key="template" className="w-full relative group">
                <button onClick={openLightbox} className="w-full h-full text-left">
                  <img src={templateImage ?? undefined} alt="Reference" className="w-full h-auto object-cover rounded" style={{ imageRendering: 'pixelated' }}/>
                </button>
                <span className="absolute top-2 left-2 bg-black/60 text-white text-sm px-3 py-1 rounded-md backdrop-blur-sm opacity-100 group-hover:opacity-0 transition-opacity duration-150 pointer-events-none">Reference</span>
              </div>
              <div key="difference" className="w-full relative group">
                <button onClick={openLightbox} className="w-full h-full text-left">
                  <img src={differenceImage ?? undefined} alt="Difference" className="w-full h-auto object-cover rounded" style={{ imageRendering: 'pixelated' }}/>
                </button>
                <span className="absolute top-2 left-2 bg-black/60 text-white text-sm px-3 py-1 rounded-md backdrop-blur-sm opacity-100 group-hover:opacity-0 transition-opacity duration-150 pointer-events-none">Difference</span>
              </div>
            </div>
            <div className="rounded-lg shadow-sm w-full border">
            <table className="min-w-full rounded-lg">
              <thead>
                <tr>
                  <th className="py-3 px-4 text-left font-medium">
                    <Select value={band} onValueChange={(v) => setBand(v)}>
                      <SelectTrigger className="w-[180px]">
                        <SelectValue placeholder="Band(s)" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All bands</SelectItem>
                        <SelectItem value="r">R-band</SelectItem>
                        <SelectItem value="g">G-band</SelectItem>
                      </SelectContent>
                    </Select>
                  </th>
                  <th className="py-3 px-4 text-left font-medium">Age</th>
                  <th className="py-3 px-4 text-left font-medium">Nb Detections</th>
                  <th className="py-3 px-4 text-left font-medium">Nb Non Detections</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                <tr>
                  <td className="py-4 px-4 font-medium">{band === "all" ? "Showing all bands" : <span className="text-orange-500">Showing {band}-band only</span>}</td>
                  <td className="py-4 px-4 font-medium">{age} days</td>
                  <td className="py-4 px-4 font-medium">{nb_detections}</td>
                  <td className="py-4 px-4 font-medium">{nb_nondetections}</td>
                </tr>
              </tbody>
            </table>
          </div>

          <div className="rounded-lg shadow-sm w-full border">
            <table className="min-w-full rounded-lg">
              <thead>
                <tr>
                  <th className="py-3 px-4 text-left font-medium">Measurement</th>
                  <th className="py-3 px-4 text-left font-medium">Time (UTC)</th>
                  <th className="py-3 px-4 text-left font-medium">Magnitude</th>
                  {(!band || band === "all") && (
                    <th className="py-3 px-4 text-left font-medium">Band</th>
                  )}
                </tr>
              </thead>
              <tbody className="divide-y">
                <tr>
                  <td className="py-4 px-4 font-medium">First Detection</td>
                  <td className="py-4 px-4">{first_det?.jd ? mjd_to_utc(jd_to_mjd(first_det.jd)).replace("T", ' ').replace("Z", "") : "-"}</td>
                  <td className="py-4 px-4">{formatDetection(first_det)}</td>
                  {(!band || band === "all") && (
                    <td className="py-4 px-4">{first_det?.band || "-"}</td>
                  )}
                </tr>
                <tr>
                  <td className="py-4 px-4 font-medium">Peak Detection</td>
                  <td className="py-4 px-4">{peak_det?.jd ? mjd_to_utc(jd_to_mjd(peak_det.jd)).replace("T", ' ').replace("Z", "") : "-"}</td>
                  <td className="py-4 px-4">{formatDetection(peak_det)}</td>
                  {(!band || band === "all") && (
                    <td className="py-4 px-4">{peak_det?.band || "-"}</td>
                  )}
                </tr>
                <tr>
                  <td className="py-4 px-4 font-medium">Last Detection</td>
                  <td className="py-4 px-4">{last_det?.jd ? mjd_to_utc(jd_to_mjd(last_det.jd)).replace("T", ' ').replace("Z", "") : "-"}</td>
                  <td className="py-4 px-4">{formatDetection(last_det)}</td>
                  {(!band || band === "all") && (
                    <td className="py-4 px-4">{last_det?.band || "-"}</td>
                  )}
                </tr>
              </tbody>
            </table>
          </div>
        </CardContent>
        <CardFooter className="flex flex-row justify-between">
          {/* next have a grid of icon buttons that link to other websites*/}
          <div className="grid grid-cols-5 gap-4 w-full">
          <Button variant="outline" className="w-full" onClick={() => window.open(`http://simbad.u-strasbg.fr/simbad/sim-coo?Coord=${ra}%20${dec}&Radius=0.08`, "_blank")}>
              <IconSparkles /> Simbad
            </Button>
            <Button variant="outline" className="w-full" onClick={() => window.open(`https://www.wis-tns.org/search?ra=${ra}&decl=${dec}&radius=5&coords_unit=arcsec`, "_blank")}>
              <IconStar /> TNS
            </Button>
            <Button variant="outline" className="w-full" onClick={() => window.open(`https://www.legacysurvey.org/viewer?ra=${ra}&dec=${dec}&layer=ls-dr10&photoz-dr9&zoom=16&mark=${ra},${dec}`, "_blank")}>
              <IconStars /> LS DR10
            </Button>
            <Button variant="outline" className="w-full" onClick={() => window.open(`https://ned.ipac.caltech.edu/cgi-bin/objsearch?search_type=Near+Position+Search&in_csys=Equatorial&in_equinox=J2000.0&ra=${ra}&dec=${dec}&radius=1.0&obj_sort=Distance+to+search+center&img_stamp=Yes`, "_blank")}>
              <IconGalaxy /> NED
            </Button>
            <Button variant="outline" className="w-full"
              onClick={() =>
                toast("Not implemented yet", {
                  description: "Crossmatching against the MPC is not implemented yet",
                  action: {
                    label: "Undo",
                    onClick: () => console.log("Undo"),
                  },
                })
              }
            >
              <IconMeteor /> MPC
            </Button>
          </div>
        </CardFooter>
        <Dialog open={lightboxOpen} onOpenChange={(v) => setLightboxOpen(v)}>
          <DialogContent className="!fixed inset-0 m-0 p-0 bg-background !max-w-none w-full h-full rounded-none overflow-hidden !top-0 !left-0 !translate-x-0 !translate-y-0 !border-none !shadow-none">
            <DialogTitle className="sr-only">Cutouts</DialogTitle>
            <div className="w-full h-full flex items-center justify-center">
              <div className="w-[95%] h-[95%] bg-background rounded-md p-6 overflow-auto">
                <div className="flex flex-col md:flex-row gap-4 h-full">
                  <div className="flex-1 flex items-center justify-center min-h-0">
                    {scienceImage ? (
                      <img src={scienceImage} alt="Science large" className="w-full h-full object-contain" style={{ imageRendering: 'pixelated' }} />
                    ) : (
                      <div className="text-muted-foreground">No science cutout</div>
                    )}
                  </div>
                  <div className="flex-1 flex items-center justify-center min-h-0">
                    {templateImage ? (
                      <img src={templateImage} alt="Reference large" className="w-full h-full object-contain" style={{ imageRendering: 'pixelated' }} />
                    ) : (
                      <div className="text-muted-foreground">No reference cutout</div>
                    )}
                  </div>
                  <div className="flex-1 flex items-center justify-center min-h-0">
                    {differenceImage ? (
                      <img src={differenceImage} alt="Difference large" className="w-full h-full object-contain" style={{ imageRendering: 'pixelated' }} />
                    ) : (
                      <div className="text-muted-foreground">No difference cutout</div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </DialogContent>
        </Dialog>
      </Card>
    )
}