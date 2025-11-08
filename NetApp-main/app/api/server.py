import os, json, time
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from kafka import KafkaProducer
from kafka.errors import KafkaError

import numpy as np

from orchestrator.rules import decide_tier
from orchestrator.mover import ensure_buckets, put_seed_objects, move_object
from orchestrator.consistency import with_retry
from orchestrator.stream_consumer import ensure_topic
from orchestrator.predictive import TierPredictor, auto_label_records

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC_ACCESS    = os.getenv("TOPIC_ACCESS", "access-events")
MONGO_URL       = os.getenv("MONGO_URL", "mongodb://mongo:27017")

app = FastAPI(title="NetApp Data-in-Motion API")

# --- globals filled at startup ---
mongo: Optional[MongoClient] = None
db = None
coll_files = None
coll_events = None
producer: Optional[KafkaProducer] = None
predictor: TierPredictor = TierPredictor()

# --------- models ----------
class AccessEvent(BaseModel):
    file_id: str
    event: str = "read"      # read/write
    ts: float = time.time()
    client_id: Optional[str] = None
    bytes_read: Optional[int] = 0
    bytes_written: Optional[int] = 0
    latency_ms: Optional[float] = None
    temperature: Optional[float] = None
    high_temp_alert: bool = False
    egress_cost: Optional[float] = 0.0
    storage_cost_per_gb: Optional[float] = None
    cloud_region: Optional[str] = None
    sync_conflict: bool = False
    failed_read: bool = False
    network_failure: bool = False
    success: bool = True

class MoveRequest(BaseModel):
    file_id: str
    target: str  # "s3" | "azure" | "gcs"


class TrainingRecord(BaseModel):
    access_freq_per_day: float
    latency_sla_ms: float
    size_kb: float
    target_tier: str
    days_since_access: Optional[float] = None
    last_access_ts: Optional[Union[str, float]] = None
    req_count_last_1min: Optional[float] = None
    req_count_last_10min: Optional[float] = None
    req_count_last_1hr: Optional[float] = None
    bytes_read_last_10min: Optional[float] = None
    bytes_written_last_10min: Optional[float] = None
    unique_clients_last_30min: Optional[float] = None
    avg_latency_1min: Optional[float] = None
    p95_latency_5min: Optional[float] = None
    max_latency_10min: Optional[float] = None
    hour_of_day: Optional[float] = None
    day_of_week: Optional[float] = None
    ema_req_5min: Optional[float] = None
    ema_req_30min: Optional[float] = None
    growth_rate_10min: Optional[float] = None
    delta_latency_5min: Optional[float] = None
    events_per_minute: Optional[float] = None
    high_temp_alerts_last_10min: Optional[float] = None
    current_tier: Optional[Union[str, float]] = None
    num_recent_migrations: Optional[float] = None
    time_since_last_migration: Optional[float] = None
    storage_cost_per_gb: Optional[float] = None
    egress_cost_last_1hr: Optional[float] = None
    cloud_region: Optional[Union[str, float]] = None
    sync_conflicts_last_1hr: Optional[float] = None
    failed_reads_last_10min: Optional[float] = None
    network_failures_last_hour: Optional[float] = None


class TrainPredictivePayload(BaseModel):
    records: Optional[List[TrainingRecord]] = None
    auto_label: bool = True

# --------- helpers ----------
FEATURE_DEFAULTS: Dict[str, Any] = {
    "req_count_last_1min": 0.0,
    "req_count_last_10min": 0.0,
    "req_count_last_1hr": 0.0,
    "bytes_read_last_10min": 0.0,
    "bytes_written_last_10min": 0.0,
    "unique_clients_last_30min": 0.0,
    "avg_latency_1min": 0.0,
    "p95_latency_5min": 0.0,
    "max_latency_10min": 0.0,
    "hour_of_day": 0.0,
    "day_of_week": 0.0,
    "ema_req_5min": 0.0,
    "ema_req_30min": 0.0,
    "growth_rate_10min": 0.0,
    "delta_latency_5min": 0.0,
    "events_per_minute": 0.0,
    "high_temp_alerts_last_10min": 0.0,
    "current_tier": "unknown",
    "num_recent_migrations": 0.0,
    "time_since_last_migration": 1e6,
    "storage_cost_per_gb": 0.05,
    "egress_cost_last_1hr": 0.0,
    "cloud_region": "us-east-1",
    "sync_conflicts_last_1hr": 0.0,
    "failed_reads_last_10min": 0.0,
    "network_failures_last_hour": 0.0,
}

LOCATION_TO_TIER = {
    "s3": "warm",
    "azure": "hot",
    "gcs": "cold",
}


def seed_from_disk():
    """Load /data/seeds/metadata.json into Mongo and ensure objects are in S3."""
    from pathlib import Path
    import json

    seed_meta = Path("/data/seeds/metadata.json")
    if not seed_meta.exists():
        return {"seeded": 0, "note": "metadata.json not found"}

    # Tolerate UTF-8 BOM (Windows)
    try:
        text = seed_meta.read_text(encoding="utf-8-sig")
    except Exception:
        text = seed_meta.read_text()  # fallback

    meta = json.loads(text)

    with_retry(ensure_buckets, retries=10, backoff=0.5)
    with_retry(lambda: put_seed_objects("/data/seeds"), retries=10, backoff=0.5)

    cnt = 0
    for m in meta:
        base_doc = {
            **m,
            "current_location": "s3",
        }
        base_doc.setdefault(
            "current_tier",
            LOCATION_TO_TIER.get(base_doc["current_location"], FEATURE_DEFAULTS["current_tier"]),
        )
        for key, default in FEATURE_DEFAULTS.items():
            base_doc.setdefault(key, default)
        coll_files.update_one(
            {"id": m["id"]},
            {"$set": base_doc},
            upsert=True,
        )
        cnt += 1
    return {"seeded": cnt, "ok": True}


def _prepare_training_rows(payload: "TrainPredictivePayload"):
    rows = []
    if payload.records:
        for record in payload.records:
            data = record.model_dump()
            days = data.get("days_since_access")
            if days is None:
                days = predictor._days_since_access(data.get("last_access_ts"))
            row: Dict[str, float] = {}
            for name in predictor.feature_names:
                if name == "days_since_access":
                    row[name] = float(days if days is not None else 365.0)
                else:
                    row[name] = predictor.normalize_feature(name, data.get(name))
            row[predictor.label_name] = data["target_tier"]
            rows.append(row)
    elif payload.auto_label:
        files = list(coll_files.find({}, {"_id": 0}))
        if not files:
            raise ValueError("no metadata available for auto labelling")
        rows = auto_label_records(
            files,
            lambda doc: decide_tier(doc.get("access_freq_per_day", 0), doc.get("latency_sla_ms", 9999)),
        )
    else:
        raise ValueError("no training data supplied")
    return rows


def _update_usage_metrics(file_id: str) -> None:
    if coll_events is None or coll_files is None:
        return

    now = time.time()
    events = list(
        coll_events.find(
            {
                "type": "access",
                "file_id": file_id,
                "ts": {"$gte": now - 3600},
            },
            {"_id": 0},
        )
    )
    events.sort(key=lambda e: e.get("ts", 0.0))

    def _events_since(seconds: float) -> List[Dict[str, Any]]:
        cutoff = now - seconds
        return [e for e in events if e.get("ts", 0.0) >= cutoff]

    def _events_between(start: float, end: float) -> List[Dict[str, Any]]:
        return [e for e in events if start <= e.get("ts", 0.0) < end]

    def _latencies(subset: List[Dict[str, Any]]) -> List[float]:
        return [float(e.get("latency_ms")) for e in subset if e.get("latency_ms") is not None]

    def _sum_field(subset: List[Dict[str, Any]], field: str) -> float:
        total = 0.0
        for e in subset:
            val = e.get(field)
            if val is None:
                continue
            try:
                total += float(val)
            except (TypeError, ValueError):
                continue
        return total

    def _count_flag(subset: List[Dict[str, Any]], field: str) -> float:
        return float(sum(1 for e in subset if e.get(field)))

    def _ema(window_minutes: int) -> float:
        bucket_events = _events_since(window_minutes * 60.0)
        if not bucket_events:
            return 0.0
        alpha = 2.0 / (window_minutes + 1)
        ema = 0.0
        for minute in range(window_minutes):
            start = now - 60.0 * (window_minutes - minute)
            end = start + 60.0
            count = float(len(_events_between(start, end)))
            ema = alpha * count + (1 - alpha) * ema
        return ema

    last_minute = _events_since(60.0)
    last_5 = _events_since(300.0)
    last_10 = _events_since(600.0)
    last_30 = _events_since(1800.0)
    last_hour = events

    prev_10_window_start = now - 1200.0
    prev_10 = [e for e in events if prev_10_window_start <= e.get("ts", 0.0) < now - 600.0]
    prev_5 = [e for e in events if now - 600.0 <= e.get("ts", 0.0) < now - 300.0]

    lat_last_minute = _latencies(last_minute)
    lat_last_5 = _latencies(last_5)
    lat_prev_5 = _latencies(prev_5)
    lat_last_10 = _latencies(last_10)
    lat_last_5min = lat_last_5

    req_count_last_1min = float(len(last_minute))
    req_count_last_10min = float(len(last_10))
    req_count_last_1hr = float(len(last_hour))

    bytes_read_last_10min = _sum_field(last_10, "bytes_read")
    bytes_written_last_10min = _sum_field(last_10, "bytes_written")
    unique_clients_last_30min = float(len({e.get("client_id") for e in last_30 if e.get("client_id")}))
    avg_latency_1min = float(fmean(lat_last_minute)) if lat_last_minute else 0.0
    p95_latency_5min = float(np.percentile(lat_last_5, 95)) if lat_last_5 else 0.0
    max_latency_10min = float(max(lat_last_10)) if lat_last_10 else 0.0

    last_ts = events[-1].get("ts") if events else now
    if last_ts is None:
        last_ts = now
    dt_last = datetime.fromtimestamp(float(last_ts), tz=timezone.utc)
    hour_of_day = float(dt_last.hour)
    day_of_week = float(dt_last.weekday())

    ema_req_5min = _ema(5)
    ema_req_30min = _ema(30)
    growth_rate_10min = req_count_last_10min - float(len(prev_10))
    avg_prev_5 = float(fmean(lat_prev_5)) if lat_prev_5 else 0.0
    avg_last_5 = float(fmean(lat_last_5min)) if lat_last_5min else 0.0
    delta_latency_5min = avg_last_5 - avg_prev_5
    events_per_minute = req_count_last_1min
    high_temp_alerts_last_10min = _count_flag(last_10, "high_temp_alert")
    egress_cost_last_1hr = _sum_field(last_hour, "egress_cost")
    sync_conflicts_last_1hr = _count_flag(last_hour, "sync_conflict")
    failed_reads_last_10min = _count_flag(last_10, "failed_read")
    network_failures_last_hour = _count_flag(last_hour, "network_failure")

    moves = list(
        coll_events.find(
            {
                "type": "move",
                "file_id": file_id,
            },
            {"_id": 0},
        )
    )
    num_recent_migrations = float(
        sum(1 for m in moves if m.get("ts") and m["ts"] >= now - 86400.0)
    )
    if moves:
        last_move_ts = max(m.get("ts", 0.0) for m in moves if m.get("ts") is not None)
        if last_move_ts:
            time_since_last_migration = float(max((now - float(last_move_ts)) / 60.0, 0.0))
        else:
            time_since_last_migration = FEATURE_DEFAULTS["time_since_last_migration"]
    else:
        time_since_last_migration = FEATURE_DEFAULTS["time_since_last_migration"]

    existing = coll_files.find_one(
        {"id": file_id},
        {
            "_id": 0,
            "current_tier": 1,
            "current_location": 1,
            "storage_cost_per_gb": 1,
            "cloud_region": 1,
            "latency_sla_ms": 1,
            "access_freq_per_day": 1,
        },
    )

    inferred_tier = None
    if existing:
        tier_value = existing.get("current_tier")
        if isinstance(tier_value, str) and tier_value.lower() != "unknown":
            inferred_tier = tier_value
        else:
            loc = str(existing.get("current_location", "")).lower()
            inferred_tier = LOCATION_TO_TIER.get(loc)
        if not inferred_tier:
            inferred_tier = decide_tier(
                existing.get("access_freq_per_day", 0.0),
                existing.get("latency_sla_ms", 9999.0),
            )
    if not inferred_tier:
        inferred_tier = FEATURE_DEFAULTS["current_tier"]

    updates = {
        "req_count_last_1min": req_count_last_1min,
        "req_count_last_10min": req_count_last_10min,
        "req_count_last_1hr": req_count_last_1hr,
        "bytes_read_last_10min": bytes_read_last_10min,
        "bytes_written_last_10min": bytes_written_last_10min,
        "unique_clients_last_30min": unique_clients_last_30min,
        "avg_latency_1min": avg_latency_1min,
        "p95_latency_5min": p95_latency_5min,
        "max_latency_10min": max_latency_10min,
        "hour_of_day": hour_of_day,
        "day_of_week": day_of_week,
        "ema_req_5min": ema_req_5min,
        "ema_req_30min": ema_req_30min,
        "growth_rate_10min": growth_rate_10min,
        "delta_latency_5min": delta_latency_5min,
        "events_per_minute": events_per_minute,
        "high_temp_alerts_last_10min": high_temp_alerts_last_10min,
        "num_recent_migrations": num_recent_migrations,
        "time_since_last_migration": time_since_last_migration,
        "egress_cost_last_1hr": egress_cost_last_1hr,
        "sync_conflicts_last_1hr": sync_conflicts_last_1hr,
        "failed_reads_last_10min": failed_reads_last_10min,
        "network_failures_last_hour": network_failures_last_hour,
        "current_tier": inferred_tier,
    }

    if not existing or existing.get("storage_cost_per_gb") is None:
        updates["storage_cost_per_gb"] = FEATURE_DEFAULTS["storage_cost_per_gb"]
    if not existing or not existing.get("cloud_region"):
        updates["cloud_region"] = FEATURE_DEFAULTS["cloud_region"]

    coll_files.update_one({"id": file_id}, {"$set": updates}, upsert=True)

# --------- lifecycle ----------
@app.on_event("startup")
def on_startup():
    """
    Robust startup with retries. Keep /health responsive even if deps are warming up.
    """
    global mongo, db, coll_files, coll_events, producer

    # 1) Mongo
    def _mongo():
        global mongo, db, coll_files, coll_events
        mongo = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
        _ = mongo.admin.command("ping")
        db = mongo["netapp"]
        coll_files = db["files"]
        coll_events = db["events"]
    with_retry(_mongo, retries=10, backoff=0.5)

    # 2) Storage emulators (ensure + seed objects)
    with_retry(ensure_buckets, retries=10, backoff=0.5)
    with_retry(lambda: put_seed_objects("/data/seeds"), retries=10, backoff=0.5)

    # 3) Seed metadata idempotently (ignore errors)
    try:
        seed_from_disk()
    except Exception:
        pass

    # 4) Load predictive model if present
    try:
        predictor.load()
    except Exception:
        # Do not fail startup if the local model file is missing or corrupted.
        predictor.model = None

    # 5) Kafka/Redpanda (best-effort)
    def _producer():
        global producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=3000,
            api_version_auto_timeout_ms=3000,
        )
    try:
        with_retry(_producer, retries=10, backoff=0.5)
        ensure_topic(KAFKA_BOOTSTRAP, TOPIC_ACCESS)  # best-effort
    except Exception:
        producer = None  # allow API to run even without producer

# --------- endpoints ----------
@app.get("/health")
def health():
    return {"status": "ok", "app": "api"}

@app.get("/files")
def list_files():
    if coll_files is None:
        raise HTTPException(503, "db not ready")
    return list(coll_files.find({}, {"_id": 0}))

@app.get("/policy/{file_id}")
def policy(file_id: str):
    if coll_files is None:
        raise HTTPException(503, "db not ready")
    f = coll_files.find_one({"id": file_id}, {"_id":0})
    if not f:
        raise HTTPException(404, "file not found")
    features = predictor.build_features(f)
    if predictor.ready:
        tier = predictor.predict(features)
        source = "predictive"
    else:
        tier = decide_tier(f.get("access_freq_per_day",0), f.get("latency_sla_ms",9999))
        source = "rule"
    return {"file_id": file_id, "recommendation": tier, "source": source, "features": features}


@app.post("/predictive/train")
def train_predictive(payload: TrainPredictivePayload):
    if coll_files is None:
        raise HTTPException(503, "db not ready")
    try:
        rows = _prepare_training_rows(payload)
        metrics = predictor.train(rows)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return metrics

@app.post("/ingest_event")
def ingest_event(ev: AccessEvent):
    if coll_files is None:
        raise HTTPException(503, "db not ready")
    if producer is None:
        # still allow policy/metadata pathways to work
        event_doc = {"type": "access", **ev.model_dump()}
        event_doc.setdefault("ts", time.time())
        coll_events.insert_one({**event_doc, "note": "no_stream"})
        set_updates: Dict[str, Any] = {
            "last_access_ts": datetime.fromtimestamp(event_doc["ts"], tz=timezone.utc).isoformat(),
        }
        if ev.storage_cost_per_gb is not None:
            set_updates["storage_cost_per_gb"] = float(ev.storage_cost_per_gb)
        if ev.cloud_region:
            set_updates["cloud_region"] = ev.cloud_region
        coll_files.update_one(
            {"id": ev.file_id},
            {
                "$inc": {"access_freq_per_day": 1},
                "$set": set_updates,
                "$setOnInsert": {
                    "storage_cost_per_gb": FEATURE_DEFAULTS["storage_cost_per_gb"],
                    "cloud_region": FEATURE_DEFAULTS["cloud_region"],
                    "current_tier": FEATURE_DEFAULTS["current_tier"],
                    "current_location": "s3",
                },
            },
            upsert=True,
        )
        _update_usage_metrics(ev.file_id)
        return {"queued": False, "note": "streaming backend not ready"}
    doc = ev.model_dump()
    try:
        producer.send(TOPIC_ACCESS, doc)
        producer.flush(2)
    except KafkaError as e:
        raise HTTPException(503, f"kafka error: {e}")
    event_doc = {"type": "access", **doc}
    event_doc.setdefault("ts", time.time())
    coll_events.insert_one(event_doc)
    set_updates: Dict[str, Any] = {
        "last_access_ts": datetime.fromtimestamp(event_doc["ts"], tz=timezone.utc).isoformat(),
    }
    if ev.storage_cost_per_gb is not None:
        set_updates["storage_cost_per_gb"] = float(ev.storage_cost_per_gb)
    if ev.cloud_region:
        set_updates["cloud_region"] = ev.cloud_region
    coll_files.update_one(
        {"id": ev.file_id},
        {
            "$inc": {"access_freq_per_day": 1},
            "$set": set_updates,
            "$setOnInsert": {
                "storage_cost_per_gb": FEATURE_DEFAULTS["storage_cost_per_gb"],
                "cloud_region": FEATURE_DEFAULTS["cloud_region"],
                "current_tier": FEATURE_DEFAULTS["current_tier"],
                "current_location": "s3",
            },
        },
        upsert=True,
    )
    _update_usage_metrics(ev.file_id)
    return {"queued": True}

from fastapi import Body

@app.post("/move")
def move(req: MoveRequest = Body(...)):
    if coll_files is None:
        raise HTTPException(503, "db not ready")
    f = coll_files.find_one({"id": req.file_id})
    if not f:
        raise HTTPException(404, "file not found")

    src = f.get("current_location", "s3")
    try:
        with_retry(lambda: move_object(req.file_id, src, req.target))
        set_fields: Dict[str, Any] = {"current_location": req.target}
        tier_from_location = LOCATION_TO_TIER.get(req.target.lower())
        if tier_from_location:
            set_fields["current_tier"] = tier_from_location
        coll_files.update_one({"id": req.file_id}, {"$set": set_fields})
        coll_events.insert_one(
            {
                "type": "move",
                "file_id": req.file_id,
                "src": src,
                "target": req.target,
                "tier": set_fields.get("current_tier"),
                "ts": time.time(),
            }
        )
        _update_usage_metrics(req.file_id)
        return {"moved": True, "from": src, "to": req.target}
    except Exception as e:
        # return the underlying error so we see exactly what's failing
        err = f"{type(e).__name__}: {e}"
        coll_events.insert_one({"type":"move_error","file_id":req.file_id,"src":src,"target":req.target,"error":err,"ts":time.time()})
        raise HTTPException(500, f"move failed {src}->{req.target}: {err}")

@app.post("/seed")
def seed():
    if coll_files is None:
        raise HTTPException(503, "db not ready")
    return seed_from_disk()

import hashlib
def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]

@app.post("/storage_test")
def storage_test():
    results = {}
    payload = b"diag-" + str(time.time()).encode()

    # S3
    try:
        from storage_clients.s3_client import S3Client
        from orchestrator.mover import S3_BUCKET
        s3 = S3Client()
        key = "diag_s3.txt"
        s3.put_object(S3_BUCKET, key, payload)
        rb = s3.get_object(S3_BUCKET, key)
        results["s3"] = {"ok": rb == payload, "sha": _sha(rb or b""), "bucket": S3_BUCKET}
        s3.delete_object(S3_BUCKET, key)
    except Exception as e:
        results["s3"] = {"ok": False, "error": str(e)}

    # Azure
    try:
        from storage_clients.azure_client import AzureClient
        az = AzureClient()
        cont = "netapp-blob"
        key = "diag_az.txt"
        az.ensure_container(cont)
        az.put_blob(cont, key, payload)
        rb = az.get_blob(cont, key)
        results["azure"] = {"ok": rb == payload, "sha": _sha(rb or b""), "container": cont}
        az.delete_blob(cont, key)
    except Exception as e:
        results["azure"] = {"ok": False, "error": str(e)}

    # GCS
    try:
        from storage_clients.gcs_client import GCSClient
        from orchestrator.mover import GCS_BUCKET
        gcs = GCSClient()
        key = "diag_gcs.txt"
        gcs.ensure_bucket(GCS_BUCKET)
        gcs.put_object(GCS_BUCKET, key, payload)
        rb = gcs.get_object(GCS_BUCKET, key)
        results["gcs"] = {"ok": rb == payload, "sha": _sha(rb or b""), "bucket": GCS_BUCKET}
        gcs.delete_object(GCS_BUCKET, key)
    except Exception as e:
        results["gcs"] = {"ok": False, "error": str(e)}

    return results
