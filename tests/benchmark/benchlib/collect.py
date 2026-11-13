"""Read a finished run's results out of MongoDB and Kafka.

Per-alert stage times come from fields BOOM already writes: `created_at` is
stamped when the alert worker starts on a packet, `updated_at` when the
enrichment batch holding it starts (both JD). Output times are the Kafka
CreateTime of Babamul and filter-result messages.
"""

from __future__ import annotations

import csv
from pathlib import Path

from .kafka_io import output_survey, read_outputs


def dump_alert_timing(db, surveys: list[str], out: Path) -> dict[str, int]:
    counts = {}
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["survey", "alert_id", "object_id", "created_at_jd", "updated_at_jd", "enriched",
                    "has_classifications", "villar_reduced_chi2"])
        for s in surveys:
            pipeline = [{"$project": {
                "objectId": 1, "created_at": 1, "updated_at": 1,
                # Only enrichment writes these. The timestamps cannot tell: both are
                # whole seconds (flare::Time), so an alert enriched within the second
                # it was ingested has updated_at == created_at.
                "enriched": {"$or": [{"$ne": [{"$type": "$properties"}, "missing"]},
                                     {"$ne": [{"$type": "$enrichment_set"}, "missing"]}]},
                "has_cls": {"$ne": [{"$type": "$classifications"}, "missing"]},
                "villar": "$villar_fit.reduced_chi2",
            }}]
            n = 0
            for d in db[f"{s}_alerts"].aggregate(pipeline, allowDiskUse=True):
                w.writerow([s, d["_id"], d.get("objectId", ""), d.get("created_at", ""), d.get("updated_at", ""),
                            int(bool(d.get("enriched"))), int(bool(d.get("has_cls"))), d.get("villar", "")])
                n += 1
            counts[s] = n
    return counts


def dump_objects(db, object_ids: dict[str, list[str]], out: Path) -> None:
    """Cross-survey links and host of every replayed object, for comparing runs."""
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["survey", "object_id", "aliases", "best_host", "best_host_catalog", "best_host_d_dlr",
                    "best_host_z", "n_cross_match_catalogs"])
        for s, ids in object_ids.items():
            coll = db[f"{s}_alerts_aux"]
            for i in range(0, len(ids), 1000):
                cursor = coll.find({"_id": {"$in": ids[i:i + 1000]}},
                                   {"aliases": 1, "host_galaxy.best_host": 1, "cross_matches": 1})
                for d in cursor:
                    aliases = d.get("aliases") or {}
                    alias_str = ";".join(f"{k}:{','.join(sorted(map(str, v)))}"
                                         for k, v in sorted(aliases.items()) if v)
                    host = ((d.get("host_galaxy") or {}).get("best_host")) or {}
                    xm = d.get("cross_matches") or {}
                    w.writerow([s, d["_id"], alias_str, host.get("objname", ""), host.get("catalog", ""),
                                host.get("d_dlr", ""), host.get("z", ""),
                                sum(1 for v in xm.values() if v)])


def dump_outputs(bootstrap: str, out: Path) -> int:
    n = 0
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["topic", "survey", "kafka_ts_ms", "alert_id", "object_id", "filter_ids"])
        for topic, ts, rec in read_outputs(bootstrap):
            rec = rec or {}
            filters = "|".join(sorted(str(x.get("filter_id")) for x in rec.get("filters") or []))
            w.writerow([topic, output_survey(topic), ts, rec.get("candid", ""), rec.get("objectId", ""), filters])
            n += 1
    return n
