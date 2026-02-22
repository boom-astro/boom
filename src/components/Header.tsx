"use client"

import { bytes2image } from '@/lib/imageProcessing';
import api, { Cutouts } from '@/lib/api';

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
import { useState, useMemo, useEffect } from "react";
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from "@/components/ui/badge"
import { Button } from './ui/button';
import { IconGalaxy, IconMeteor, IconSparkles, IconStar, IconStars, IconRotate2 } from '@tabler/icons-react';
import { Maximize2 } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from "@/components/ui/dialog"
import { Tooltip, TooltipTrigger, TooltipContent } from './ui/tooltip';
import { radec2lb } from '@/lib/utils';


dayjs.extend(utc);
dayjs.extend(relativeTime);

const colorMap = "bone";

// Simple band color map
const BAND_COLORS: Record<string, string> = {
    g: '#38b000',
    r: '#ef233c',
    i: '#fcbf49',
    z: '#f59e0b',
    default: '#6b7280',
};

function toColor(band?: string) {
    if (!band) return BAND_COLORS.default;
    const k = String(band).toLowerCase();
    return BAND_COLORS[k] ?? BAND_COLORS.default;
}


type Detection = {
  jd?: number;
  magpsf?: number;
  sigmapsf?: number;
  band?: string;
  diffmaglim?: number;
};

type CandidateData = {
  objectId?: string;
  candid?: number;
  candidate?: { ra?: number; dec?: number; drb?: number; ndethist?: number };
  classifications?: Record<string, number>;
  properties?: { star?: boolean };
  prv_candidates?: Detection[];
  prv_nondetections?: Detection[];
  survey_matches?: Record<string, { objectId?: string; distance_arcsec?: number }>;
  cross_matches?: Record<string, Array<{ ra?: number; dec?: number; score?: number; distance_arcsec?: number }>>;
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
  
  // Check LSPSC cross_matches for stellar/hosted classification
  const lspscMatches = data.cross_matches?.LSPSC ?? [];
  const hasLspscStellar = lspscMatches.some(
    (m) => (m.distance_arcsec ?? Infinity) < 1 && (m.score ?? 0) > 0.5
  );
  const hasLspscHosted = lspscMatches.some((m) => (m.score ?? 1) < 0.5);
  
  return (
    <>
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
      {(properties?.star || hasLspscStellar) && (
        <Badge variant="outline" className="text-sm font-semibold">
          Stellar?
        </Badge>
      )}
      {!hasLspscStellar && hasLspscHosted && (
        <Badge variant="outline" className="text-sm font-semibold">
          Hosted?
        </Badge>
      )}
    </>
  )
}

export function SurveyMatchesBadges({
  survey_matches,
}: {
  survey_matches: Record<string, { objectId?: string, distance_arcsec?: number }> | null | undefined;
}) {
  if (!survey_matches || Object.keys(survey_matches).length === 0) {
    return null;
  }
  return (
    <>
      {Object.entries(survey_matches).map(([survey, match]) => {
        // if match is null or objectId is missing, skip
        if (!match || !match.objectId) {
          return null;
        }
        const objectId = match.objectId ?? "unknown";
        const distance = match.distance_arcsec != null ? `${match.distance_arcsec.toFixed(2)}"` : "unknown";
        const url = `/objects/${survey}/${objectId}`;
        return (
          <Tooltip key={survey}>
            <TooltipTrigger asChild>
              <Badge
                variant="secondary"
                className="text-sm font-semibold cursor-pointer hover:underline"
                onClick={() => window.open(url, "_blank")}
              >
                {/* {survey.toUpperCase()}: {objectId} */}
                {/* only show the survey name in front, if the objectId doesn't start with the survey name */}
                {objectId.toLowerCase().startsWith(survey.toLowerCase()) ? objectId : `${survey.toUpperCase()} ${objectId}`}
              </Badge>
            </TooltipTrigger>
            <TooltipContent>
              <span>Separation: {distance}</span>
            </TooltipContent>
          </Tooltip>
        );
      })}
    </>
  );
}

export function TNSBadge({
  cross_matches,
}: {
  cross_matches: Record<string, Array<{ ra?: number; dec?: number; name?: string; name_prefix?: string; score?: number; distance_arcsec?: number, type?: string, redshift?: string }>> | null | undefined;
}) {
  const tnsMatches = cross_matches?.TNS;
  if (!tnsMatches?.length) {
    return null;
  }
  const bestMatch = tnsMatches.reduce((a, b) => ( (a.distance_arcsec ?? Infinity) < (b.distance_arcsec ?? Infinity) ? a : b));
  const name_prefix = bestMatch.name_prefix ?? "AT";
  const name = bestMatch.name ?? "unknown";
  const type = bestMatch.type;
  const distance = bestMatch.distance_arcsec != null ? `${bestMatch.distance_arcsec.toFixed(2)}"` : "unknown";
  const redshift = Number.parseFloat(bestMatch?.redshift?.toString() ?? "NaN");
  const url = `https://www.wis-tns.org/object/${name}`;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Badge
          variant="default"
          className="text-sm font-semibold cursor-pointer hover:underline flex items-center gap-1 bg-[#CA5D3B] text-white"
          onClick={() => window.open(url, "_blank")}
        >
          <img src="https://www.wis-tns.org/themes/custom/astrot/favicon.png" alt="TNS" className="w-3 h-3" />
          {name_prefix ?? ""} {name}{type ? `: ${type}` : ""}{redshift ? ` (z=${redshift.toFixed(2)})` : ""}
        </Badge>
      </TooltipTrigger>
      <TooltipContent>
        <span>Separation: {distance} {type ? `| Type: ${type}` : ""} {redshift ? `| Redshift: ${redshift}` : ""}</span>
      </TooltipContent>
    </Tooltip>
  );
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
    const [rotated, setRotated] = useState(true);
    const [cutouts, setCutouts] = useState<Cutouts | null>(null);
    const [loadingCutouts, setLoadingCutouts] = useState(true);

    const objectId = data.objectId ?? "";
    const ra = data.candidate?.ra?.toFixed(6) ?? "-";
    const dec = data.candidate?.dec?.toFixed(6) ?? "-";

    const [l, b] = radec2lb(Number(ra), Number(dec));

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

    const survey = objectId.startsWith("ZTF") ? "ztf" : "lsst";

    useEffect(() => {
      if (!data.objectId) return;
      let cancelled = false;
      const fetchCutouts = async () => {
        setLoadingCutouts(true);
        try {
          const result = await api.fetchObjCutouts(survey, data.objectId!);
          if (!cancelled) setCutouts(result);
        } catch (err) {
          console.error("Failed to load cutouts:", err);
          if (!cancelled) setCutouts({});
        } finally {
          if (!cancelled) setLoadingCutouts(false);
        }
      };
      fetchCutouts();
      return () => { cancelled = true; };
    }, [survey, data.objectId]);

    const scienceImage = cutouts ? bytes2image(cutouts.cutoutScience, survey, "science", colorMap, rotated) : null;
    const templateImage = cutouts ? bytes2image(cutouts.cutoutTemplate, survey, "template", colorMap, rotated) : null;
    const differenceImage = cutouts ? bytes2image(cutouts.cutoutDifference, survey, "difference", colorMap, rotated) : null;

    const firstTime = first_det?.jd ? mjd_to_utc(jd_to_mjd(first_det.jd)).replace("T", ' ').replace("Z", "") : "-";
    const peakTime = peak_det?.jd ? mjd_to_utc(jd_to_mjd(peak_det.jd)).replace("T", ' ').replace("Z", "") : "-";
    const lastTime = last_det?.jd ? mjd_to_utc(jd_to_mjd(last_det.jd)).replace("T", ' ').replace("Z", "") : "-";

    function openLightbox() {
      setLightboxOpen(true);
    }

    return (
      <Card className="@container/card col-span-1 @xl/main:col-span-2 gap-3 row-span-2">
        <CardHeader className="gap-0">
          <div className="flex items-start justify-between gap-2">
            <CardTitle className="text-2xl font-semibold tabular-nums md:text-3xl">{objectId}</CardTitle>
            {!objectId.startsWith("ZTF") && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => setRotated(!rotated)}
                    className={`transition-colors ${rotated ? 'bg-primary/20 text-primary hover:bg-primary/30' : 'opacity-50 hover:opacity-75'}`}
                    aria-label={rotated ? "Disable image rotation" : "Enable image rotation"}
                  >
                    <IconRotate2 size={18} />
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="bottom">
                  {rotated ? "Disable rotation" : "Enable rotation"}
                </TooltipContent>
              </Tooltip>
            )}
          </div>
          <CardDescription className="flex flex-col gap-1">
            <div className="flex flex-row flex-wrap">
              <div className='text-md md:text-sm cursor-pointer hover:text-blue-500' onClick={() => { 
                navigator.clipboard.writeText(`${ra}, ${dec}`)
                toast.success("Copied RA/Dec to clipboard");
              }}>RA: {ra}° | Dec: {dec}° &nbsp;</div>
              <div className='text-md md:text-sm cursor-pointer hover:text-blue-500' onClick={() => {
                navigator.clipboard.writeText(`l,b = ${l.toFixed(6)}, ${b.toFixed(6)}`)
                toast.success("Copied Galactic Coordinates to clipboard");
              }}>(l,b = {l.toFixed(6)}°, {b.toFixed(6)}°)</div>
            </div>
            <div className="flex flex-row flex-wrap gap-2">
              <TNSBadge cross_matches={data.cross_matches} />
              <SurveyMatchesBadges survey_matches={data.survey_matches} />
              <ClassificationBadges data={data} />
            </div>
          </CardDescription>
        </CardHeader>
        <CardContent className="pb-0 flex flex-col gap-3">
            <div className="grid grid-cols-3 gap-3">
              {loadingCutouts ? (
                <>
                  <div className="w-full flex flex-col gap-1 items-center">
                    <Skeleton className="w-full aspect-square rounded" />
                    <div className="text-xs font-medium text-muted-foreground">Science</div>
                  </div>
                  <div className="w-full flex flex-col gap-1 items-center">
                    <Skeleton className="w-full aspect-square rounded" />
                    <div className="text-xs font-medium text-muted-foreground">Reference</div>
                  </div>
                  <div className="w-full flex flex-col gap-1 items-center">
                    <Skeleton className="w-full aspect-square rounded" />
                    <div className="text-xs font-medium text-muted-foreground">Difference</div>
                  </div>
                </>
              ) : (
                <>
                  <div key="science" className="w-full flex flex-col gap-1 items-center">
                    <button onClick={openLightbox} className="w-full h-full text-left relative group">
                      <img src={scienceImage ?? undefined} alt="Science" className="w-full h-auto object-cover rounded" style={{ imageRendering: 'pixelated' }}/>
                        <Maximize2 className="absolute top-2 right-2 w-4 h-4 text-white opacity-0 group-hover:opacity-90 transition-opacity duration-150 pointer-events-none drop-shadow" />
                    </button>
                    <div className="text-xs font-medium text-muted-foreground">Science</div>
                  </div>
                  <div key="template" className="w-full flex flex-col gap-1 items-center">
                    <button onClick={openLightbox} className="w-full h-full text-left relative group">
                      <img src={templateImage ?? undefined} alt="Reference" className="w-full h-auto object-cover rounded" style={{ imageRendering: 'pixelated' }}/>
                        <Maximize2 className="absolute top-2 right-2 w-4 h-4 text-white opacity-0 group-hover:opacity-90 transition-opacity duration-150 pointer-events-none drop-shadow" />
                    </button>
                    <div className="text-xs font-medium text-muted-foreground">Reference</div>
                  </div>
                  <div key="difference" className="w-full flex flex-col gap-1 items-center">
                    <button onClick={openLightbox} className="w-full h-full text-left relative group">
                      <img src={differenceImage ?? undefined} alt="Difference" className="w-full h-auto object-cover rounded" style={{ imageRendering: 'pixelated' }}/>
                        <Maximize2 className="absolute top-2 right-2 w-4 h-4 text-white opacity-0 group-hover:opacity-90 transition-opacity duration-150 pointer-events-none drop-shadow" />
                    </button>
                    <div className="text-xs font-medium text-muted-foreground">Difference</div>
                  </div>
                </>
              )}
            </div>
            <div className="rounded-lg shadow-sm w-full border overflow-hidden">
              <div className="hidden sm:block overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr>
                      <th className="py-3 px-3 sm:px-4 text-left font-medium">
                        <Select value={band} onValueChange={(v) => setBand(v)}>
                          <SelectTrigger className="w-auto whitespace-nowrap xl:w-[180px]">
                            <SelectValue placeholder="Band(s)" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">All bands</SelectItem>
                            <SelectItem value="r">R-band</SelectItem>
                            <SelectItem value="g">G-band</SelectItem>
                          </SelectContent>
                        </Select>
                      </th>
                      <th className="py-3 px-3 sm:px-4 text-left font-medium">Age</th>
                      <th className="py-3 px-3 sm:px-4 text-left font-medium">Nb Detections</th>
                      <th className="py-3 px-3 sm:px-4 text-left font-medium">Nb Non Detections</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    <tr>
                      <td className="py-4 px-3 sm:px-4 font-medium">
                        <span className="font-medium">
                          {band === "all" ? "Showing all bands" : (
                            <>
                              Showing <span style={{ color: toColor(band) }}>{band}-band</span> only
                            </>
                          )}
                        </span>
                      </td>
                      <td className="py-4 px-3 sm:px-4 font-medium">{age} days</td>
                      <td className="py-4 px-3 sm:px-4 font-medium">{nb_detections}</td>
                      <td className="py-4 px-3 sm:px-4 font-medium">{nb_nondetections}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <div className="grid gap-3 p-4 sm:hidden text-sm">
                <Select value={band} onValueChange={(v) => setBand(v)}>
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder="Band(s)" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All bands</SelectItem>
                    <SelectItem value="r">R-band</SelectItem>
                    <SelectItem value="g">G-band</SelectItem>
                  </SelectContent>
                </Select>
                <div className="grid grid-cols-2 gap-x-4 gap-y-2">
                  <span className="text-muted-foreground">Band</span>
                  <span className="font-medium">
                    {band === "all" ? "Showing all bands" : (
                      <>
                        Showing <span style={{ color: toColor(band) }}>{band}-band</span> only
                      </>
                    )}
                  </span>
                  <span className="text-muted-foreground">Age</span>
                  <span className="font-medium">{age} days</span>
                  <span className="text-muted-foreground">Detections</span>
                  <span className="font-medium">{nb_detections}</span>
                  <span className="text-muted-foreground">Non Detections</span>
                  <span className="font-medium">{nb_nondetections}</span>
                </div>
              </div>
            </div>

          <div className="rounded-lg shadow-sm w-full border overflow-hidden">
            <div className="hidden sm:block overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr>
                    <th className="py-3 px-3 sm:px-4 text-left font-medium">Measurement</th>
                    <th className="py-3 px-3 sm:px-4 text-left font-medium">Time (UTC)</th>
                    <th className="py-3 px-3 sm:px-4 text-left font-medium">Magnitude</th>
                    {(!band || band === "all") && (
                      <th className="py-3 px-3 sm:px-4 text-left font-medium">Band</th>
                    )}
                  </tr>
                </thead>
                <tbody className="divide-y">
                  <tr>
                    <td className="py-4 px-3 sm:px-4 font-medium">First Detection</td>
                    <td className="py-4 px-3 sm:px-4">{firstTime}</td>
                    <td className="py-4 px-3 sm:px-4">{formatDetection(first_det)}</td>
                    {(!band || band === "all") && (
                      <td className="py-4 px-3 sm:px-4">{first_det?.band || "-"}</td>
                    )}
                  </tr>
                  <tr>
                    <td className="py-4 px-3 sm:px-4 font-medium">Peak Detection</td>
                    <td className="py-4 px-3 sm:px-4">{peakTime}</td>
                    <td className="py-4 px-3 sm:px-4">{formatDetection(peak_det)}</td>
                    {(!band || band === "all") && (
                      <td className="py-4 px-3 sm:px-4">{peak_det?.band || "-"}</td>
                    )}
                  </tr>
                  <tr>
                    <td className="py-4 px-3 sm:px-4 font-medium">Last Detection</td>
                    <td className="py-4 px-3 sm:px-4">{lastTime}</td>
                    <td className="py-4 px-3 sm:px-4">{formatDetection(last_det)}</td>
                    {(!band || band === "all") && (
                      <td className="py-4 px-3 sm:px-4">{last_det?.band || "-"}</td>
                    )}
                  </tr>
                </tbody>
              </table>
            </div>
            <div className="grid gap-3 p-4 sm:hidden text-sm">
              <div className="grid grid-cols-2 gap-x-4 gap-y-2">
                <span className="text-muted-foreground">First Detection</span>
                <span className="font-medium flex flex-col">
                  <span>{firstTime}</span>
                  <span className="text-muted-foreground">{formatDetection(first_det)}; {first_det?.band}-band</span>
                </span>
                <span className="text-muted-foreground">Peak Detection</span>
                <span className="font-medium flex flex-col">
                  <span>{peakTime}</span>
                  <span className="text-muted-foreground">{formatDetection(peak_det)}; {peak_det?.band}-band</span>
                </span>
                <span className="text-muted-foreground">Last Detection</span>
                <span className="font-medium flex flex-col">
                  <span>{lastTime}</span>
                  <span className="text-muted-foreground">{formatDetection(last_det)}; {last_det?.band}-band</span>
                </span>
              </div>
            </div>
          </div>
        </CardContent>
        <CardFooter className="flex flex-row justify-between">
          {/* next have a grid of icon buttons that link to other websites*/}
          <div className="grid grid-cols-5 sm:grid-cols-5 gap-2 sm:gap-4 w-full">
          <Button variant="outline" className="w-full text-xs sm:text-sm" onClick={() => window.open(`http://simbad.u-strasbg.fr/simbad/sim-coo?Coord=${ra}%20${dec}&Radius=0.08`, "_blank")}>
              <IconSparkles className="hidden sm:inline mr-1" size={16} /> Simbad
            </Button>
            <Button variant="outline" className="w-full text-xs sm:text-sm" onClick={() => window.open(`https://www.wis-tns.org/search?ra=${ra}&decl=${dec}&radius=5&coords_unit=arcsec`, "_blank")}>
              <IconStar className="hidden sm:inline mr-1" size={16} /> TNS
            </Button>
            <Button variant="outline" className="w-full text-xs sm:text-sm" onClick={() => window.open(`https://www.legacysurvey.org/viewer?ra=${ra}&dec=${dec}&layer=ls-dr10&photoz-dr9&zoom=16&mark=${ra},${dec}`, "_blank")}>
              <IconStars className="hidden sm:inline mr-1" size={16} /> LS DR10
            </Button>
            <Button variant="outline" className="w-full text-xs sm:text-sm" onClick={() => window.open(`https://ned.ipac.caltech.edu/cgi-bin/objsearch?search_type=Near+Position+Search&in_csys=Equatorial&in_equinox=J2000.0&ra=${ra}&dec=${dec}&radius=1.0&obj_sort=Distance+to+search+center&img_stamp=Yes`, "_blank")}>
              <IconGalaxy className="hidden sm:inline mr-1" size={16} /> NED
            </Button>
            <Button variant="outline" className="w-full text-xs sm:text-sm"
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
              <IconMeteor className="hidden sm:inline mr-1" size={16} /> MPC
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