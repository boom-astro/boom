import { type ReactNode } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { AvroSchema } from "@/lib/api";
import { SURVEYS, ZTF, type Survey } from "@/lib/utils";
import { SchemaViewer } from "@/components/SchemaViewer";

const getFieldDescriptions = (survey: Survey): Record<string, ReactNode> => ({
  stationary: (
    <>
      <code className="text-chart-2">True</code> if the temporal baseline (last &minus; first
      observation) &gt; 0.01&nbsp;days (~14&nbsp;min).
      Combines <code>prv_candidates</code> and <code>fp_hists</code> (forced photometry, SNR &ge; 3).
    </>
  ),
  ...survey === ZTF ? {
    rock: (
      <>
        <code className="text-chart-2">True</code> if a known Solar System object is within 12&nbsp;arcsec (<code>ssdistnr</code>) with a valid <code>ssmagnr</code>.
      </>
    ),
    star: (
      <>
        Stellar classification from PS1 star-galaxy scores (<code>sgscore1</code>, <code>distpsnr1</code>):
        <br/>- <code className="text-chart-2">True</code> if <code>sgscore1</code> &gt; 0.76 and <code>distpsnr1</code> &le; 2.0 arcsec.
        <br/>- <code className="text-chart-2">True</code> if <code>sgscore1</code> &gt; 0.2, <code>distpsnr1</code> &le; 1.0 arcsec and a strong red color (<code>srmag1 - szmag1</code> or <code>srmag1 - simag1</code> &gt; 3.0).
      </>
    ),
    near_brightstar: (
      <>
        Bright-star proximity flag combining Gaia and PS1:
        <br/>- <code className="text-chart-2">True</code> if a Gaia bright star is within 20 arcsec (<code>neargaiabright</code>) with <code>maggaiabright</code> &le; 12.0.
        <br/>- <code className="text-chart-2">True</code> if any of the top-3 PS1 matches has <code>sgscore</code> &gt; 0.49, distance &le; 20 arcsec and <code>srmag</code> &le; 15.0.
        <br/>- <code className="text-chart-2">True</code> if <code>sgscore1</code> = 0.5, <code>distpsnr1</code> &lt; 0.5 arcsec and any of <code>sgmag1</code>/<code>srmag1</code>/<code>simag1</code> &lt; 17.0.
      </>
    ),
  } : {
    rock: (
      <>
        <code className="text-chart-2">True</code> if the alert has an <code>ss_object_id</code> (Solar System object identifier).
      </>
    ),
    star: (
      <>
        Cross-match with the LSPSC catalog:
        <br/>- <code className="text-chart-2">True</code> if distance &le; 1.0 arcsec and score &gt; 0.5.
        <br/>- <code className="text-destructive">False</code> if inside the footprint but no match.
        <br/>- <code className="text-destructive">None</code> if outside the LSST footprint.
      </>
    ),
    near_brightstar: (
      <>
        Same catalog (LSPSC) with wider criteria:
        <br/>- <code className="text-chart-2">True</code> if distance &le; 20.0 arcsec, score &gt; 0.5 and white magnitude &le; 15.0 (bright star).
        <br/>- <code className="text-destructive">False</code> if inside the footprint but no bright star nearby.
        <br/>- <code className="text-destructive">None</code> if outside the LSST footprint.
      </>
    ),
  },
  photstats: "Light-curve statistics computed from the alert's (prv_candidates + fp_hists).",
  multisurvey_photstats: "Same statistics recomputed from the combined light curve when a cross-survey match exists.",
  peak_jd: "Julian Date of the brightest observation (minimum magnitude) in this band.",
  peak_mag: "Magnitude at the brightest point (lower magnitude = brighter).",
  peak_mag_err: "Photometric uncertainty on the peak magnitude.",
  dt: "Time span in days between the first and last observation.",
  rising: "Linear fit of the light curve before the peak (brightening phase). Null if fewer than 2 points, time between first and last point before the peak ≤ 0.01 days (~14 min), or insufficient signal.",
  fading: "Linear fit of the light curve after the peak (dimming phase). Null if fewer than 2 points, time between first and last point after the peak ≤ 0.01 days (~14 min), or insufficient signal.",
  rate: "Slope of the linear fit (mag/day). Negative when rising, positive when fading.",
  rate_error: "Standard deviation of the slope estimate.",
  red_chi2: "Reduced chi-squared of the fit, values near 1.0 indicate a good linear fit.",
  nb_data: "Number of data points used in the linear fit.",
})

export default function KafkaSchemas({ schemas }: { schemas: Record<string, AvroSchema> }) {
  if (!Object.keys(schemas).length) {
    return (
      <Card>
        <CardContent className="py-8 text-center text-muted-foreground">
          No schemas available.
        </CardContent>
      </Card>
    );
  }

  return (
    <Tabs defaultValue="lsst">
      <TabsList className="grid w-full grid-cols-3">
        {SURVEYS.filter((s) => schemas[s]).map((s) => (
          <TabsTrigger key={s} value={s}>{s.toUpperCase()}</TabsTrigger>
        ))}
        <TabsTrigger value="compare">Compare</TabsTrigger>
      </TabsList>
      {SURVEYS.filter((s) => schemas[s]).map((survey) => (
        <TabsContent key={survey} value={survey}>
          <SchemaViewer survey={survey} schema={schemas[survey]} field_descriptions={getFieldDescriptions(survey)}/>
        </TabsContent>
      ))}
      <TabsContent value="compare">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
          {SURVEYS.filter((s) => schemas[s]).map((survey) => (
            <SchemaViewer key={survey} survey={survey} schema={schemas[survey]} field_descriptions={getFieldDescriptions(survey)} copy_button={false}/>
          ))}
        </div>
      </TabsContent>
    </Tabs>
  );
}
