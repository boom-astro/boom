# Argus

Argus alerts follow schema 3.0 (`schemas/argus/3.0`). Every packet carries an
`alertCadence` and exactly one of `eventAssociation` or `ssoAssociation`, which
put it in one of six lanes:

| Lane | Packets |
|---|---|
| `1s` | events at 1 s cadence |
| `1m` | events at 60 s cadence |
| `15m` | events from 15 min coadds |
| `1h` | events from 1 h coadds |
| `1d` | events from day and week coadds |
| `sso` | solar system objects, any cadence |

Each lane has its own Kafka topic and consumer group, and its own Valkey queue
and pool of workers at every stage, so a flood at one cadence never delays
another:

| Stage | Queue | Writes |
|---|---|---|
| alert | `ARGUS_alerts_packets_queue_<lane>` | `ARGUS_alerts`, `ARGUS_alerts_aux` |
| enrichment | `ARGUS_alerts_enrichment_queue_<lane>` | `properties` on the alert |
| filter | `ARGUS_alerts_filter_queue_<lane>` | `ARGUS_alerts_results` topic |

`workers.argus.<stage>.lanes` sets the pool size per lane, and `n_workers`
covers any lane not listed.

```sh
kafka_consumer argus   # one consumer per lane, groups <group_id>-<lane>
scheduler argus        # one pool per lane and stage
```

When every lane comes on one topic, `kafka_consumer argus --topics-override
<topic>` reads it with group `<group_id>` and sends each packet to the alert
queue of its lane.

The aux document is keyed by `eventId` or `ssoId` and holds the light curve in
`prv_candidates`, every cadence together, one point per `jd`, `filter` and
`cadence`. An alert's `_id` hashes `alertId`, `alertCadence`,
`obsWeightedMidpoint` and `filter`, since `alertId` is shared by every
detection at one position. Events are crossmatched with the
`crossmatch.argus` catalogs and get a host galaxy when one is found.

Enrichment fits the light curve at the alert's own cadence, per band
(`properties.photstats.beta` and `.rho`), and adds the detection and episode
histories and the cadences the object was seen at. On alerts brighter than
their reference, `properties.flare_fit` fits the last 256 points of the alert's
band and cadence over one day with sums of Davenport et al. (2014) flare
templates (lightcurve-fitting `fit_flares`, up to 3 components), on the
difference flux over the alert's reference flux (`psfFluxSci - psfFluxDif`).
It gives the number of components, `delta_bic`
against a flat baseline, the total equivalent duration, and the peak time,
FWHM, amplitude and equivalent duration of the latest component, times in
seconds. Filters see `kind`, `lane`,
`alertCadence`, `candidate`, `properties`, `eventAssociation` or
`ssoAssociation`, and from the aux `prv_candidates`, `cross_matches`,
`host_galaxy` and `aliases`.
