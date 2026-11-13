"""MongoDB helpers: preparing the run database and reading results out of it."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

from pymongo import MongoClient

from . import DB_NAME, mongo_uri
from .timeutil import unix_to_jd


def connect(port: int, db_name: str = DB_NAME, timeout_s: float = 120.0):
    client = MongoClient(mongo_uri(port), serverSelectionTimeoutMS=5000)
    deadline = time.time() + timeout_s
    while True:
        try:
            client.admin.command("ping")
            return client, client[db_name]
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(2)


def reset_survey_state(db, surveys: list[str]) -> None:
    """Drop what a previous run of these surveys would have written."""
    for s in surveys:
        db.drop_collection(f"{s}_alerts")
        db.drop_collection(f"{s}_alerts_cutouts")
    db.drop_collection("filters")


def mark_mpc_orbits_fresh(db) -> str:
    """Keep the scheduler from downloading MPCORB at startup.

    The scheduler checks `updated_at` (Unix seconds) on whichever orbit document
    it reads first, so every document gets the stamp. Returns what was found.
    """
    coll = db["MPC_orbits"]
    now = time.time()
    n = coll.estimated_document_count()
    if n <= 1:
        coll.delete_many({})
        coll.insert_one({"_id": "benchmark-stub", "updated_at": now})
        return "stub (no orbits: solar-system association finds nothing)"
    coll.update_many({}, {"$set": {"updated_at": now}})
    return f"{n} orbits"


def load_filters(directory: Path, surveys: list[str], n_filters: int | None) -> tuple[list[dict], list[str]]:
    """Filter documents for the replayed surveys, plus any warnings.

    A file holds one filter or a list of them, either as exported from a
    deployment's `filters` collection or in the short form
    `{"survey": "ZTF", "name": ..., "pipeline": [...]}`.
    """
    warnings: list[str] = []
    docs: list[dict] = []
    for path in sorted(Path(directory).glob("*.json")):
        content = json.loads(path.read_text())
        for item in content if isinstance(content, list) else [content]:
            item = _unwrap_extended_json(item)
            if str(item.get("survey", "")).upper() in surveys:
                docs.append(_as_filter_doc(item))
    docs.sort(key=lambda d: d["name"])
    if n_filters is not None:
        if n_filters > len(docs) and docs:
            warnings.append(
                f"{n_filters} filters requested but {len(docs)} available; the rest are copies, "
                "which share query plans and cached pages and so cost less than distinct filters")
            base = list(docs)
            i = 0
            while len(docs) < n_filters:
                copy = json.loads(json.dumps(base[i % len(base)]))
                copy["_id"] = str(uuid.uuid4())
                copy["name"] = f"{copy['name']}__copy{i // len(base) + 1}"
                docs.append(copy)
                i += 1
        docs = docs[:n_filters]
    return docs, warnings


def _unwrap_extended_json(obj):
    """mongoexport writes {"$oid": ...}, {"$date": ...}; flatten them to plain values."""
    if isinstance(obj, dict):
        if len(obj) == 1:
            key, value = next(iter(obj.items()))
            if key in ("$oid", "$numberLong", "$numberInt"):
                return str(value) if key == "$oid" else int(value)
            if key == "$numberDouble":
                return float(value)
        return {k: _unwrap_extended_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_unwrap_extended_json(v) for v in obj]
    return obj


def _as_filter_doc(item: dict) -> dict:
    if "fv" in item:
        doc = dict(item)
        doc["_id"] = str(doc.get("_id") or uuid.uuid4())
        doc["active"] = True
        return doc
    survey = str(item["survey"]).upper()
    now = unix_to_jd(time.time())
    return {
        "_id": str(uuid.uuid4()),
        "name": item.get("name", "benchmark-filter"),
        "description": item.get("description"),
        "survey": survey,
        "user_id": "benchmark",
        "permissions": item.get("permissions") or {survey: [1, 2, 3]},
        "active": True,
        "active_fid": "v1",
        "fv": [{"fid": "v1", "created_at": now, "pipeline": json.dumps(item["pipeline"]), "changelog": None}],
        "created_at": now,
        "updated_at": now,
    }


def insert_filters(db, docs: list[dict]) -> None:
    db.drop_collection("filters")
    if docs:
        db["filters"].insert_many(docs)
    db["filters"].create_index("filter_id")


def collection_stats(db) -> list[dict]:
    out = []
    for name in sorted(db.list_collection_names()):
        try:
            s = db.command("collStats", name)
        except Exception as e:  # views and system collections
            out.append({"collection": name, "error": str(e)})
            continue
        out.append({
            "collection": name,
            "count": s.get("count"),
            "data_size_bytes": s.get("size"),
            "storage_size_bytes": s.get("storageSize"),
            "total_index_size_bytes": s.get("totalIndexSize"),
            "total_size_bytes": s.get("totalSize"),
        })
    return out


def catalog_presence(db, catalogs: list[str]) -> dict[str, int]:
    names = set(db.list_collection_names())
    return {c: (db[c].estimated_document_count() if c in names else 0) for c in catalogs}


def not_yet_detected(cut_jd: float) -> dict:
    """Objects with no detection before `cut_jd`: at that moment they did not exist yet."""
    return {"prv_candidates": {"$not": {"$elemMatch": {"jd": {"$lt": cut_jd}}}}}


def count_premature_objects(db, survey: str, object_ids: list[str], cut_jd: float) -> int:
    """Replayed objects whose document exists although they were first detected in the replay.

    The alert worker cross-matches and associates a host only when it creates
    an object's document (src/alert/ztf.rs `upsert_aux`), so for these the
    replay silently skips both.
    """
    coll = db[f"{survey}_alerts_aux"]
    n = 0
    for i in range(0, len(object_ids), 10000):
        n += coll.count_documents({"_id": {"$in": object_ids[i:i + 10000]}, **not_yet_detected(cut_jd)})
    return n


def max_history_jd(db, survey: str, sample: int = 2000) -> float | None:
    """Latest photometry in a sample of the survey's objects, to check the snapshot cut."""
    coll = db[f"{survey}_alerts_aux"]
    pipeline = [
        {"$sample": {"size": sample}},
        {"$project": {"jd": {"$max": {"$concatArrays": [
            {"$ifNull": ["$prv_candidates.jd", []]},
            {"$ifNull": ["$fp_hists.jd", []]},
        ]}}}},
        {"$group": {"_id": None, "jd": {"$max": "$jd"}}},
    ]
    res = list(coll.aggregate(pipeline))
    return res[0]["jd"] if res and res[0].get("jd") is not None else None
