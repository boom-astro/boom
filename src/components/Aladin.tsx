import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"
import { useEffect } from "react";

export default function Aladin({
    alert,
  }: {
    alert: any
  }) {

    useEffect(() => {
        let ra = alert.candidate.ra;
        let dec = alert.candidate.dec;
        let aladin = window.A.aladin('#aladin-lite-div', { 
            survey: 'CDS/P/DESI-Legacy-Surveys/DR10/color',
            fov: 2/60,
            target: `${ra},${dec}`,
            showProjectionControl: false,
            showZoomControl: false,
            // showFullscreenControl: false,
            showLayersControl: false,
            showGotoControl: false,
            showFrame: false,
        });

        if (alert?.cross_matches?.NED_BetaV3?.length > 0) {
            let ned_catalog = A.catalog({
                name: 'NED',
                color: 'red',
                sourceSize: 10,
                raField: 'ra',
                decField: 'dec',
                labelColumn: 'objname',
                onClick: 'showPopup',
            });
            aladin.addCatalog(ned_catalog);
            let ned_sources = alert.cross_matches.NED_BetaV3.map((data: any) => {
                let ra = data.ra;
                let dec = data.dec;
                // let name = data.objname;
                delete data.coordinates;
                return A.source(ra, dec, data);
            });
            ned_catalog.addSources(ned_sources);
        }

        let overlay = A.graphicOverlay({
            color: 'white',
            lineWidth: 2,
        });
        aladin.addOverlay(overlay);
        overlay.add(A.circle(ra, dec, 2/3600));


        // let's have a listener on the container so that when one double clicks
        // we set the target back to the original target and zoom
        let container = document.getElementById('aladin-lite-container');
        if (container) {
            container.addEventListener('dblclick', (e) => {
                e.preventDefault();
                e.stopPropagation();
                aladin.gotoRaDec(ra, dec);
                aladin.setFov(2/60);
            });
        }

    }, [alert]);

    return (
    //   <Card className="@container/card">
    // we want no padding at all in that card
      <Card className="@container/card p-0 min-h-[100px]">
        {/* then the card content also has no padding, but has rounded corners */}
        <CardContent className="p-0 flex flex-row gap-4 items-center w-full h-full rounded-lg" id="aladin-lite-container">
            <div id="aladin-lite-div" className="w-full h-full rounded-lg z-10"></div>
        </CardContent>
      </Card>
    )
}