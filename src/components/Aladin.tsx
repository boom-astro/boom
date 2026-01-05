import {
    Card,
    CardContent,
} from "@/components/ui/card"
import { useEffect, useState } from "react";
import { ApiObject } from '@/lib/api';
import { loadAladinScript } from '@/lib/aladinLoader';

export default function Aladin({
        alert,
    }: {
        alert: ApiObject | null | undefined
    }) {
        const [isAladinLoaded, setIsAladinLoaded] = useState(false);

        // Load Aladin script when component mounts
        useEffect(() => {
                loadAladinScript()
                        .then(() => setIsAladinLoaded(true))
                        .catch((err) => console.error('Failed to load Aladin:', err));
        }, []);

        useEffect(() => {
                if (!alert || !isAladinLoaded || !window.A) return;
                const candidate = alert['candidate'] as Record<string, unknown> | undefined;
                const ra: number = Number(candidate?.['ra'] ?? alert['ra'] ?? alert['ra_deg']);
                const dec: number = Number(candidate?.['dec'] ?? alert['dec'] ?? alert['dec_deg']);
        const b = 10; // placeholder for galactic latitude
        const survey = (Math.abs(b) < 20) ? 'CDS/P/Pan-STARRS/DR1/color-z-zg-g' : 'CDS/P/DESI-Legacy-Surveys/DR10/color';
        const aladin = window.A.aladin('#aladin-lite-div', {
            survey: survey,
            fov: 63/3600,
            target: `${ra}, ${dec}`,
            showProjectionControl: false,
            showZoomControl: false,
            // showFullscreenControl: false,
            showLayersControl: true,
            showGotoControl: false,
            showFrame: false,
        });

        const simbad = window.A.catalogHiPS('https://hipscat.cds.unistra.fr/HiPSCatService/Simbad', {onClick: 'showTable', name: 'Simbad', sourceSize: 16, color: 'blue'});
        aladin.addCatalog(simbad);

        const crossMatches = alert ? (alert['cross_matches'] as Record<string, unknown> | undefined) : undefined;
        const nedList = crossMatches ? crossMatches['NED_BetaV3'] : undefined;
        if (Array.isArray(nedList) && nedList.length > 0) {
            const ned_catalog = window.A.catalog({
                name: 'NED',
                color: 'red',
                sourceSize: 10,
                raField: 'ra',
                decField: 'dec',
                labelColumn: 'objname',
                onClick: 'showPopup',
            });
            aladin.addCatalog(ned_catalog);
                        const matches = nedList ?? [];
                        const ned_sources: Array<unknown> = [];
                        if (Array.isArray(matches)) {
                            for (const dataRaw of matches) {
                                const data = (dataRaw ?? {}) as Record<string, unknown>;
                                const raVal = Number(data['ra']);
                                const decVal = Number(data['dec']);
                                if (Object.prototype.hasOwnProperty.call(data, 'coordinates')) {
                                    const copy = { ...data } as Record<string, unknown>;
                                    delete copy.coordinates;
                                    ned_sources.push(window.A.source(raVal, decVal, copy as Record<string, unknown>));
                                } else {
                                    ned_sources.push(window.A.source(raVal, decVal, data as Record<string, unknown>));
                                }
                            }
                        }
                        const addSourcesFn = (ned_catalog as unknown as { addSources?: (...args: unknown[]) => unknown }).addSources;
                        if (typeof addSourcesFn === 'function') {
                            addSourcesFn.call(ned_catalog, ned_sources);
                        }
        }

        const overlay = window.A.graphicOverlay({
            color: 'white',
            lineWidth: 2,
        });
        aladin.addOverlay(overlay);
        overlay.add(window.A.circle(ra, dec, 2/3600));

        // reset position and zoom level on double click
        const container = document.getElementById('aladin-lite-container');
        const dblHandler = (e: MouseEvent) => {
            if (!container) return;
            e.preventDefault();
            e.stopPropagation();
            aladin.gotoRaDec(ra, dec);
            aladin.setFov(63/3600);
        };
        if (container) {
            container.addEventListener('dblclick', dblHandler);
        }

        return () => {
            // cleanup dblclick listener
            if (container) {
                container.removeEventListener('dblclick', dblHandler);
            }
            // try to remove overlay/catalogs if API supports it (best-effort)
            try {
                if (overlay && typeof overlay.clear === 'function') overlay.clear();
            } catch {
                // ignore cleanup errors
            }
        };
    }, [alert, isAladinLoaded]);

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