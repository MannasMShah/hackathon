import json
import logging
import os
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
from typing import Any, Dict, List, Optional, Union

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pymongo import MongoClient
from kafka import KafkaProducer
from kafka.errors import KafkaError

import numpy as np

from orchestrator.rules import decide_tier
from orchestrator.mover import ensure_buckets, put_seed_objects, move_object
from orchestrator.consistency import (
    ConsistencyManager,
    parse_replica_env,
    with_retry,
)
from orchestrator.stream_consumer import ensure_topic
from orchestrator.predictive import TierPredictor, auto_label_records

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC_ACCESS = os.getenv("TOPIC_ACCESS", "access-events")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
STREAM_API_BASE = os.getenv("STREAM_API", "http://stream-api:8001")
REPLICA_ENDPOINTS = parse_replica_env(os.getenv("REPLICA_ENDPOINTS"))
SIMULATION_ENABLED = os.getenv("ENABLE_SYNTHETIC_LOAD", "1").lower() not in {"0", "false"}
SIMULATION_THROTTLE = float(os.getenv("SYNTHETIC_LOAD_INTERVAL", "2.5"))
SIZE_KB_PER_GB = 1024 * 1024

ALERT_COST_THRESHOLD = float(os.getenv("ALERT_COST_THRESHOLD", "0.08"))
ALERT_LATENCY_P95_MS = float(os.getenv("ALERT_LATENCY_P95_MS", "180"))
ALERT_STREAM_RATE = float(os.getenv("ALERT_STREAM_EVENTS_PER_MIN", "15"))
ALERT_CONFIDENCE_THRESHOLD = float(os.getenv("ALERT_CONFIDENCE_THRESHOLD", "0.9"))
STREAM_LOW_ACTIVITY_THRESHOLD = float(os.getenv("ALERT_STREAM_LOW_ACTIVITY", "5"))

logger = logging.getLogger("netapp.api")

app = FastAPI(title="NetApp Data-in-Motion API")

# --- globals filled at startup ---
mongo: Optional[MongoClient] = None
db = None
coll_files = None
coll_events = None
coll_sync = None
producer: Optional[KafkaProducer] = None
predictor: TierPredictor = TierPredictor()
consistency_mgr: Optional[ConsistencyManager] = None
_simulator_stop = threading.Event()
_simulator_thread: Optional[threading.Thread] = None
_simulator_event_counter = 0

# --------- models ----------
class AccessEvent(BaseModel):
    file_id: str
    event: str = "read"      # read/write
    ts: float = Field(default_factory=lambda: time.time())
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
    source: Optional[str] = "api"

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


class SimulationBurst(BaseModel):
    events: int = 500
    file_ids: Optional[List[str]] = None
    include_moves: bool = True
    stream_events: bool = True
    pace_ms: int = 0

# --------- helpers ----------
FEATURE_DEFAULTS: Dict[str, Any] = {
    "size_kb": 0.0,
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
    "predicted_tier": "unknown",
    "prediction_confidence": 0.0,
    "prediction_source": "rule",
    "storage_gb_estimate": 0.0,
    "estimated_monthly_cost": 0.0,
    "active_alerts": [],
    "policy_triggers": [],
    "last_alert_eval_ts": None,
}

LOCATION_TO_TIER = {
    "s3": "warm",
    "azure": "hot",
    "gcs": "cold",
}

TIER_TO_LOCATION = {tier: location for location, tier in LOCATION_TO_TIER.items()}


def _stream_event_payload(file_id: str, ev: AccessEvent, counter: int) -> Dict[str, Any]:
    base = abs(hash(file_id)) % 10_000
    return {
        "event_id": counter,
        "device_id": base,
        "temperature": float(ev.temperature if ev.temperature is not None else random.uniform(50.0, 90.0)),
        "bytes": int((ev.bytes_read or 0) + (ev.bytes_written or 0)),
        "timestamp": float(ev.ts or time.time()),
    }


def _post_stream_event(session: requests.Session, payload: Dict[str, Any]) -> None:
    if not STREAM_API_BASE:
        return
    try:
        session.post(f"{STREAM_API_BASE}/stream/event", json=payload, timeout=1.5)
    except Exception:
        # Keep simulation resilient even if the stream API is warming up.
        pass


def _build_stream_snapshot(limit: int = 240) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "throughput_per_min": 0.0,
        "active_clients": 0,
        "events": [],
        "total_events": 0,
    }
    if coll_events is None:
        return snapshot

    now = time.time()
    cursor = (
        coll_events.find({"type": "access"}, {"_id": 0})
        .sort("ts", -1)
        .limit(limit)
    )
    events = list(cursor)
    events.reverse()
    for evt in events:
        if isinstance(evt, dict) and "timestamp" not in evt and evt.get("ts") is not None:
            try:
                evt["timestamp"] = float(evt["ts"])
            except (TypeError, ValueError):
                continue
    snapshot["events"] = events
    snapshot["total_events"] = int(coll_events.count_documents({"type": "access"}))
    if events:
        recent = [evt for evt in events if now - float(evt.get("ts", 0.0) or 0.0) <= 60.0]
        if recent:
            oldest = min(float(evt.get("ts", now)) for evt in recent)
            window = max(now - oldest, 1.0)
            snapshot["throughput_per_min"] = float(len(recent)) * 60.0 / window
        clients = {evt.get("client_id") for evt in events if evt.get("client_id")}
        snapshot["active_clients"] = len(clients)
    return snapshot


def _alert_signature(alert: Dict[str, Any]) -> tuple:
    if not isinstance(alert, dict):
        return ()
    return (
        str(alert.get("type") or ""),
        str(alert.get("reason") or ""),
        str(alert.get("severity") or ""),
    )


def _policy_signature(policy: Dict[str, Any]) -> tuple:
    if not isinstance(policy, dict):
        return ()
    return (
        str(policy.get("action") or ""),
        str(policy.get("target_tier") or ""),
        str(policy.get("reason") or ""),
    )


def _evaluate_alerts(
    existing_alerts: Optional[List[Dict[str, Any]]],
    existing_policies: Optional[List[Dict[str, Any]]],
    feature_source: Dict[str, Any],
    predicted_tier: Optional[str],
    prediction_confidence: Optional[float],
    inferred_tier: str,
    estimated_cost: float,
) -> Dict[str, List[Dict[str, Any]]]:
    now_iso = datetime.now(timezone.utc).isoformat()
    current_tier = (inferred_tier or "unknown").lower()
    predicted_tier_normalised = (predicted_tier or "").lower()
    confidence = float(prediction_confidence or 0.0)

    events_per_minute = float(feature_source.get("events_per_minute", 0.0) or 0.0)
    p95_latency = float(feature_source.get("p95_latency_5min", 0.0) or 0.0)
    avg_latency = float(feature_source.get("avg_latency_1min", 0.0) or 0.0)
    cost_estimate = float(estimated_cost)

    previous_alerts = {
        _alert_signature(alert): alert
        for alert in (existing_alerts or [])
        if isinstance(alert, dict)
    }
    previous_policies = {
        _policy_signature(policy): policy
        for policy in (existing_policies or [])
        if isinstance(policy, dict)
    }

    alerts: List[Dict[str, Any]] = []
    policies: List[Dict[str, Any]] = []

    def _carry_alert(alert: Dict[str, Any]) -> Dict[str, Any]:
        alert.setdefault("triggered_at", now_iso)
        sig = _alert_signature(alert)
        if sig in previous_alerts:
            alert.setdefault("triggered_at", previous_alerts[sig].get("triggered_at", now_iso))
        return alert

    def _carry_policy(policy: Dict[str, Any]) -> Dict[str, Any]:
        policy.setdefault("triggered_at", now_iso)
        sig = _policy_signature(policy)
        if sig in previous_policies:
            existing = previous_policies[sig]
            policy.setdefault("triggered_at", existing.get("triggered_at", now_iso))
            if policy.get("confidence") in (None, 0.0):
                policy["confidence"] = existing.get("confidence")
        return policy

    # Latency breach alert → promote to hot tier
    if p95_latency >= ALERT_LATENCY_P95_MS:
        alerts.append(
            _carry_alert(
                {
                    "type": "latency_sla",
                    "severity": "critical",
                    "reason": "latency_sla_breach",
                    "message": (
                        f"p95 latency {p95_latency:.1f} ms exceeded SLA ({ALERT_LATENCY_P95_MS:.0f} ms)"
                    ),
                    "metric": {
                        "p95_latency_5min": p95_latency,
                        "avg_latency_1min": avg_latency,
                    },
                }
            )
        )
        if current_tier != "hot":
            target_tier = "hot"
            policies.append(
                _carry_policy(
                    {
                        "type": "latency_remediation",
                        "action": "promote_tier",
                        "target_tier": target_tier,
                        "target_location": TIER_TO_LOCATION.get(target_tier),
                        "reason": "latency_sla_breach",
                        "confidence": max(confidence, 0.95 if predicted_tier_normalised == "hot" else 0.85),
                        "status": "pending",
                    }
                )
            )

    # Streaming hotspot alert → consider promoting tier
    if events_per_minute >= ALERT_STREAM_RATE:
        alerts.append(
            _carry_alert(
                {
                    "type": "stream_hotspot",
                    "severity": "warning",
                    "reason": "stream_hotspot",
                    "message": f"Kafka throughput {events_per_minute:.1f} events/min crosses {ALERT_STREAM_RATE}",
                    "metric": {"events_per_minute": events_per_minute},
                }
            )
        )
        if current_tier != "hot":
            target_tier = "hot" if predicted_tier_normalised == "hot" else "warm"
            policies.append(
                _carry_policy(
                    {
                        "type": "stream_hotspot",
                        "action": "promote_tier",
                        "target_tier": target_tier,
                        "target_location": TIER_TO_LOCATION.get(target_tier),
                        "reason": "stream_hotspot",
                        "confidence": max(confidence, 0.9 if target_tier == "hot" else 0.8),
                        "status": "pending",
                    }
                )
            )

    # Cost alert when spend is high but usage is low → demote tier
    if cost_estimate >= ALERT_COST_THRESHOLD and events_per_minute <= STREAM_LOW_ACTIVITY_THRESHOLD:
        alerts.append(
            _carry_alert(
                {
                    "type": "cost_overrun",
                    "severity": "warning",
                    "reason": "cost_efficiency",
                    "message": (
                        f"Estimated monthly cost ₹{cost_estimate:.2f} with low activity ({events_per_minute:.1f}/min)"
                    ),
                    "metric": {
                        "estimated_monthly_cost": cost_estimate,
                        "events_per_minute": events_per_minute,
                    },
                }
            )
        )
        if current_tier not in {"cold"}:
            target_tier = "cold"
            policies.append(
                _carry_policy(
                    {
                        "type": "cost_optimisation",
                        "action": "demote_tier",
                        "target_tier": target_tier,
                        "target_location": TIER_TO_LOCATION.get(target_tier),
                        "reason": "cost_efficiency",
                        "confidence": max(confidence, 0.75),
                        "status": "pending",
                    }
                )
            )

    # High-confidence ML recommendation → actionable policy trigger
    if (
        predicted_tier_normalised
        and predicted_tier_normalised != current_tier
        and confidence >= ALERT_CONFIDENCE_THRESHOLD
    ):
        alerts.append(
            _carry_alert(
                {
                    "type": "ml_recommendation",
                    "severity": "info",
                    "reason": "ml_high_confidence",
                    "message": (
                        f"Predictive model suggests {predicted_tier_normalised.upper()} with {confidence * 100:.1f}% confidence"
                    ),
                    "metric": {
                        "prediction_confidence": confidence,
                        "predicted_tier": predicted_tier_normalised,
                    },
                }
            )
        )
        policies.append(
            _carry_policy(
                {
                    "type": "ml_recommendation",
                    "action": "apply_prediction",
                    "target_tier": predicted_tier_normalised,
                    "target_location": TIER_TO_LOCATION.get(predicted_tier_normalised),
                    "reason": "ml_high_confidence",
                    "confidence": confidence,
                    "status": "pending",
                }
            )
        )

    return {"alerts": alerts, "policies": policies}


def _record_alert_events(
    file_id: str,
    previous_alerts: Optional[List[Dict[str, Any]]],
    new_alerts: Optional[List[Dict[str, Any]]],
    previous_policies: Optional[List[Dict[str, Any]]],
    new_policies: Optional[List[Dict[str, Any]]],
) -> None:
    if coll_events is None:
        return

    now = time.time()

    prev_alert_map = {
        _alert_signature(alert): alert
        for alert in (previous_alerts or [])
        if isinstance(alert, dict)
    }
    new_alert_map = {
        _alert_signature(alert): alert
        for alert in (new_alerts or [])
        if isinstance(alert, dict)
    }

    for signature, alert in new_alert_map.items():
        if signature not in prev_alert_map:
            coll_events.insert_one(
                {
                    "type": "alert",
                    "file_id": file_id,
                    "ts": now,
                    **alert,
                }
            )

    for signature, alert in prev_alert_map.items():
        if signature not in new_alert_map:
            coll_events.insert_one(
                {
                    "type": "alert_resolved",
                    "file_id": file_id,
                    "ts": now,
                    "alert_type": alert.get("type"),
                    "reason": alert.get("reason"),
                }
            )

    prev_policy_map = {
        _policy_signature(policy): policy
        for policy in (previous_policies or [])
        if isinstance(policy, dict)
    }
    new_policy_map = {
        _policy_signature(policy): policy
        for policy in (new_policies or [])
        if isinstance(policy, dict)
    }

    for signature, policy in new_policy_map.items():
        if signature not in prev_policy_map:
            coll_events.insert_one(
                {
                    "type": "policy_triggered",
                    "file_id": file_id,
                    "ts": now,
                    **policy,
                }
            )

    for signature, policy in prev_policy_map.items():
        if signature not in new_policy_map:
            coll_events.insert_one(
                {
                    "type": "policy_cleared",
                    "file_id": file_id,
                    "ts": now,
                    "action": policy.get("action"),
                    "reason": policy.get("reason"),
                }
            )

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
        base_doc.setdefault("version", 1)
        base_doc.setdefault(
            "sync_state",
            {
                "status": "seeded",
                "last_synced": datetime.now(timezone.utc).isoformat(),
                "replicas": [],
                "last_error": None,
            },
        )
        coll_files.update_one(
            {"id": m["id"]},
            {"$set": base_doc},
            upsert=True,
        )
        try:
            _update_usage_metrics(m["id"])
        except Exception:
            pass
        if consistency_mgr is not None:
            try:
                consistency_mgr.mark_seed_synced(m["id"])
            except Exception:
                # keep seeding resilient; consistency manager will reconcile later
                pass
        cnt += 1
    return {"seeded": cnt, "ok": True}


def _simulate_load_loop():
    global _simulator_event_counter
    if coll_files is None:
        return

    session = requests.Session()
    rng = random.Random()
    backoff = 1.0
    while not _simulator_stop.is_set():
        try:
            files = list(
                coll_files.find(
                    {},
                    {
                        "_id": 0,
                        "id": 1,
                        "size_kb": 1,
                        "cloud_region": 1,
                        "current_location": 1,
                        "current_tier": 1,
                        "storage_cost_per_gb": 1,
                    },
                )
            )
            if not files:
                time.sleep(5.0)
                continue

            burst = rng.randint(25, 60)
            for _ in range(burst):
                if _simulator_stop.is_set():
                    break
                record = rng.choice(files)
                file_id = record.get("id")
                if not file_id:
                    continue
                event_type = "write" if rng.random() < 0.35 else "read"
                base_bytes = rng.randint(5_000, 200_000)
                latency = rng.uniform(10.0, 180.0)
                access = AccessEvent(
                    file_id=file_id,
                    event=event_type,
                    ts=time.time(),
                    client_id=f"svc-{rng.randint(1, 200)}",
                    bytes_read=base_bytes if event_type == "read" else 0,
                    bytes_written=base_bytes if event_type == "write" else 0,
                    latency_ms=latency,
                    temperature=rng.uniform(45.0, 95.0),
                    high_temp_alert=rng.random() < 0.05,
                    egress_cost=rng.uniform(0.0, 0.75),
                    storage_cost_per_gb=record.get("storage_cost_per_gb", 0.05),
                    cloud_region=record.get("cloud_region", "us-east-1"),
                    sync_conflict=rng.random() < 0.02,
                    failed_read=rng.random() < 0.01,
                    network_failure=False,
                    success=True,
                    source="synthetic",
                )
                try:
                    ingest_event(access)
                except HTTPException:
                    pass
                _simulator_event_counter += 1
                payload = _stream_event_payload(file_id, access, _simulator_event_counter)
                if SIMULATION_ENABLED:
                    _post_stream_event(session, payload)
                if SIMULATION_THROTTLE > 0:
                    time.sleep(SIMULATION_THROTTLE / max(burst, 1))

            if rng.random() < 0.25:
                record = rng.choice(files)
                file_id = record.get("id")
                if file_id:
                    targets = [loc for loc in LOCATION_TO_TIER.keys() if loc != record.get("current_location")]
                    if targets:
                        try:
                            move(MoveRequest(file_id=file_id, target=rng.choice(targets)))
                        except HTTPException:
                            pass

            backoff = 1.0
        except Exception as exc:
            logger.debug("synthetic load loop error: %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)


def _start_simulator() -> None:
    global _simulator_thread
    if not SIMULATION_ENABLED:
        return
    if _simulator_thread is not None and _simulator_thread.is_alive():
        return
    _simulator_stop.clear()
    _simulator_thread = threading.Thread(target=_simulate_load_loop, name="synthetic-load", daemon=True)
    _simulator_thread.start()


def _stop_simulator() -> None:
    global _simulator_thread
    _simulator_stop.set()
    if _simulator_thread and _simulator_thread.is_alive():
        _simulator_thread.join(timeout=2.0)
    _simulator_thread = None


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


def _bootstrap_predictor_from_rules() -> None:
    """Train the predictor from existing metadata using the rule engine labels."""

    if coll_files is None:
        return

    files = list(coll_files.find({}, {"_id": 0}))
    if not files:
        return

    rows = auto_label_records(
        files,
        lambda doc: decide_tier(
            doc.get("access_freq_per_day", 0.0),
            doc.get("latency_sla_ms", 9999.0),
        ),
    )
    if not rows:
        return

    labels = {
        row.get(predictor.label_name)
        for row in rows
        if row.get(predictor.label_name) is not None
    }
    if len(labels) <= 1:
        # Not enough diversity for a useful model—skip bootstrapping.
        return

    predictor.train(rows)


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
            "size_kb": 1,
            "last_access_ts": 1,
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

    feature_source: Dict[str, Any] = dict(existing or {})
    feature_source.update(updates)
    if feature_source.get("size_kb") in (None, ""):
        feature_source["size_kb"] = FEATURE_DEFAULTS["size_kb"]
    if feature_source.get("storage_cost_per_gb") in (None, ""):
        feature_source["storage_cost_per_gb"] = FEATURE_DEFAULTS["storage_cost_per_gb"]

    predicted_tier: Optional[str] = None
    prediction_confidence: Optional[float] = None
    prediction_source = "predictive" if predictor.ready else "rule"
    try:
        features = predictor.build_features(feature_source)
        if predictor.ready:
            predicted_tier, prediction_confidence = predictor.predict_with_confidence(features)
            prediction_source = predictor.model_type or "predictive"
    except Exception as exc:
        logger.debug("predictive inference failed for %s: %s", file_id, exc)

    if not predicted_tier:
        predicted_tier = decide_tier(
            feature_source.get("access_freq_per_day", 0.0),
            feature_source.get("latency_sla_ms", 9999.0),
        )
        if prediction_confidence is None:
            if predicted_tier == "hot":
                prediction_confidence = 0.75
            elif predicted_tier == "warm":
                prediction_confidence = 0.65
            else:
                prediction_confidence = 0.55
        prediction_source = "rule"

    storage_gb_estimate = float(feature_source.get("size_kb", 0.0)) / SIZE_KB_PER_GB
    estimated_cost = storage_gb_estimate * float(
        feature_source.get("storage_cost_per_gb", FEATURE_DEFAULTS["storage_cost_per_gb"])
    )

    updates.update(
        {
            "predicted_tier": predicted_tier,
            "prediction_confidence": float(prediction_confidence or 0.0),
            "prediction_source": prediction_source,
            "storage_gb_estimate": storage_gb_estimate,
            "estimated_monthly_cost": estimated_cost,
        }
    )

    previous_alerts = list(existing.get("active_alerts", [])) if existing else []
    previous_policies = list(existing.get("policy_triggers", [])) if existing else []
    evaluation = _evaluate_alerts(
        previous_alerts,
        previous_policies,
        feature_source,
        predicted_tier,
        prediction_confidence,
        inferred_tier,
        estimated_cost,
    )
    alerts = evaluation.get("alerts", [])
    policies = evaluation.get("policies", [])

    updates["active_alerts"] = alerts
    updates["policy_triggers"] = policies
    updates["last_alert_eval_ts"] = datetime.now(timezone.utc).isoformat()

    try:
        _record_alert_events(
            file_id,
            previous_alerts,
            alerts,
            previous_policies,
            policies,
        )
    except Exception as exc:
        logger.debug("failed to record alert history for %s: %s", file_id, exc)

    if not existing or existing.get("storage_cost_per_gb") is None:
        updates["storage_cost_per_gb"] = FEATURE_DEFAULTS["storage_cost_per_gb"]
    if not existing or not existing.get("cloud_region"):
        updates["cloud_region"] = FEATURE_DEFAULTS["cloud_region"]

    if consistency_mgr is None:
        coll_files.update_one({"id": file_id}, {"$set": updates}, upsert=True)
        return

    try:
        consistency_mgr.safe_update(
            file_id,
            lambda doc, data=updates: {"set": data},
            reason="metrics_refresh",
        )
    except Exception:
        # fall back to a basic update but keep the API responsive
        coll_files.update_one({"id": file_id}, {"$set": updates}, upsert=True)

# --------- lifecycle ----------
@app.on_event("startup")
def on_startup():
    """
    Robust startup with retries. Keep /health responsive even if deps are warming up.
    """
    global mongo, db, coll_files, coll_events, coll_sync, producer, consistency_mgr

    # 1) Mongo
    def _mongo():
        global mongo, db, coll_files, coll_events, coll_sync
        mongo = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
        _ = mongo.admin.command("ping")
        db = mongo["netapp"]
        coll_files = db["files"]
        coll_events = db["events"]
        coll_sync = db["sync_queue"]
    with_retry(_mongo, retries=10, backoff=0.5)

    # 1b) Initialize consistency manager once Mongo is ready
    consistency_mgr = ConsistencyManager(
        coll_files,
        coll_events,
        coll_sync,
        FEATURE_DEFAULTS,
        replica_endpoints=REPLICA_ENDPOINTS,
    )
    consistency_mgr.ensure_indexes()

    # 2) Storage emulators (ensure + seed objects)
    with_retry(ensure_buckets, retries=10, backoff=0.5)
    with_retry(lambda: put_seed_objects("/data/seeds"), retries=10, backoff=0.5)

    # 3) Seed metadata idempotently (ignore errors)
    try:
        seed_from_disk()
    except Exception:
        pass

    # 3b) Reconcile any pending replica sync work
    try:
        consistency_mgr.reconcile_pending()
    except Exception:
        pass

    # 4) Load predictive model if present
    try:
        predictor.load()
    except Exception:
        # Do not fail startup if the local model file is missing or corrupted.
        predictor.model = None

    # 4b) If no persisted model exists yet, bootstrap one from the rule engine so
    # the predictive path is active out of the box.
    if not predictor.ready:
        try:
            _bootstrap_predictor_from_rules()
        except Exception:
            # Keep startup resilient—prediction will fall back to the rule engine
            # if bootstrapping fails for any reason.
            pass

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

    # 6) Begin synthetic streaming if enabled so dashboards have live data.
    try:
        _start_simulator()
    except Exception as exc:
        logger.debug("failed to start simulator: %s", exc)


# --------- shutdown ----------
@app.on_event("shutdown")
def on_shutdown():
    try:
        _stop_simulator()
    except Exception as exc:
        logger.debug("failed to stop simulator cleanly: %s", exc)


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
        tier, confidence = predictor.predict_with_confidence(features)
        source = "predictive"
    else:
        tier = decide_tier(f.get("access_freq_per_day",0), f.get("latency_sla_ms",9999))
        source = "rule"
        confidence = None
    return {
        "file_id": file_id,
        "recommendation": tier,
        "source": source,
        "features": features,
        "confidence": confidence,
        "model_type": getattr(predictor, "model_type", "unknown"),
    }


@app.get("/streaming/metrics")
def streaming_metrics(limit: int = 240):
    if coll_events is None:
        raise HTTPException(503, "db not ready")
    snapshot = _build_stream_snapshot(limit)
    snapshot["producer_ready"] = producer is not None
    snapshot["kafka_bootstrap"] = KAFKA_BOOTSTRAP
    snapshot["topic"] = TOPIC_ACCESS
    snapshot.setdefault("active_devices", snapshot.get("active_clients", 0))
    return snapshot


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
    size_increment_kb = float(ev.bytes_written or 0.0) / 1024.0
    if producer is None:
        # still allow policy/metadata pathways to work
        event_doc = {"type": "access", **ev.model_dump()}
        event_doc.setdefault("ts", time.time())
        event_doc["network_failure"] = True
        coll_events.insert_one({**event_doc, "note": "no_stream"})
        set_updates: Dict[str, Any] = {
            "last_access_ts": datetime.fromtimestamp(event_doc["ts"], tz=timezone.utc).isoformat(),
        }
        if ev.storage_cost_per_gb is not None:
            set_updates["storage_cost_per_gb"] = float(ev.storage_cost_per_gb)
        if ev.cloud_region:
            set_updates["cloud_region"] = ev.cloud_region
        if consistency_mgr is not None:
            try:
                consistency_mgr.record_failure(ev.file_id, "kafka_unavailable", "producer not ready")
            except Exception:
                pass

        def _mutate(doc: Dict[str, Any]):
            payload = dict(set_updates)
            if doc.get("storage_cost_per_gb") is None and "storage_cost_per_gb" not in payload:
                payload["storage_cost_per_gb"] = FEATURE_DEFAULTS["storage_cost_per_gb"]
            if not doc.get("cloud_region") and "cloud_region" not in payload:
                payload["cloud_region"] = FEATURE_DEFAULTS["cloud_region"]
            inc_payload = {"access_freq_per_day": 1}
            if size_increment_kb > 0.0:
                inc_payload["size_kb"] = size_increment_kb
            return {"set": payload, "inc": inc_payload}

        if consistency_mgr is None:
            coll_files.update_one(
                {"id": ev.file_id},
                {
                    "$inc": {
                        **({"size_kb": size_increment_kb} if size_increment_kb > 0.0 else {}),
                        "access_freq_per_day": 1,
                    },
                    "$set": set_updates,
                    "$setOnInsert": {
                        "size_kb": max(size_increment_kb, FEATURE_DEFAULTS["size_kb"]),
                        "storage_cost_per_gb": FEATURE_DEFAULTS["storage_cost_per_gb"],
                        "cloud_region": FEATURE_DEFAULTS["cloud_region"],
                        "current_tier": FEATURE_DEFAULTS["current_tier"],
                        "current_location": "s3",
                    },
                },
                upsert=True,
            )
        else:
            try:
                consistency_mgr.safe_update(ev.file_id, _mutate, reason="access_event")
            except Exception:
                coll_files.update_one(
                    {"id": ev.file_id},
                    {
                        "$inc": {
                            **({"size_kb": size_increment_kb} if size_increment_kb > 0.0 else {}),
                            "access_freq_per_day": 1,
                        },
                        "$set": set_updates,
                        "$setOnInsert": {
                            "size_kb": max(size_increment_kb, FEATURE_DEFAULTS["size_kb"]),
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
        if consistency_mgr is not None:
            try:
                consistency_mgr.record_failure(ev.file_id, "kafka_error", str(e))
            except Exception:
                pass
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

    def _mutate_success(doc: Dict[str, Any]):
        payload = dict(set_updates)
        if doc.get("storage_cost_per_gb") is None and "storage_cost_per_gb" not in payload:
            payload["storage_cost_per_gb"] = FEATURE_DEFAULTS["storage_cost_per_gb"]
        if not doc.get("cloud_region") and "cloud_region" not in payload:
            payload["cloud_region"] = FEATURE_DEFAULTS["cloud_region"]
        inc_payload = {"access_freq_per_day": 1}
        if size_increment_kb > 0.0:
            inc_payload["size_kb"] = size_increment_kb
        return {"set": payload, "inc": inc_payload}

    if consistency_mgr is None:
        coll_files.update_one(
            {"id": ev.file_id},
            {
                "$inc": {
                    **({"size_kb": size_increment_kb} if size_increment_kb > 0.0 else {}),
                    "access_freq_per_day": 1,
                },
                "$set": set_updates,
                "$setOnInsert": {
                    "size_kb": max(size_increment_kb, FEATURE_DEFAULTS["size_kb"]),
                    "storage_cost_per_gb": FEATURE_DEFAULTS["storage_cost_per_gb"],
                    "cloud_region": FEATURE_DEFAULTS["cloud_region"],
                    "current_tier": FEATURE_DEFAULTS["current_tier"],
                    "current_location": "s3",
                },
            },
            upsert=True,
        )
    else:
        try:
            consistency_mgr.safe_update(ev.file_id, _mutate_success, reason="access_event")
        except Exception:
                coll_files.update_one(
                    {"id": ev.file_id},
                    {
                        "$inc": {
                            **({"size_kb": size_increment_kb} if size_increment_kb > 0.0 else {}),
                            "access_freq_per_day": 1,
                        },
                        "$set": set_updates,
                        "$setOnInsert": {
                            "size_kb": max(size_increment_kb, FEATURE_DEFAULTS["size_kb"]),
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
        if consistency_mgr is None:
            coll_files.update_one({"id": req.file_id}, {"$set": set_fields})
        else:

            def _mutate(doc: Dict[str, Any]):
                payload = dict(set_fields)
                payload["time_since_last_migration"] = 0.0
                return {"set": payload, "inc": {"num_recent_migrations": 1}}

            try:
                consistency_mgr.safe_update(req.file_id, _mutate, reason="migration")
            except Exception:
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
        if consistency_mgr is not None:
            try:
                consistency_mgr.record_failure(req.file_id, "move_error", err)
            except Exception:
                pass
        raise HTTPException(500, f"move failed {src}->{req.target}: {err}")

@app.post("/seed")
def seed():
    if coll_files is None:
        raise HTTPException(503, "db not ready")
    return seed_from_disk()


@app.post("/simulate/burst")
def simulate_burst(payload: SimulationBurst):
    if coll_files is None:
        raise HTTPException(503, "db not ready")

    query: Dict[str, Any] = {}
    if payload.file_ids:
        query["id"] = {"$in": payload.file_ids}
    candidates = list(
        coll_files.find(
            query,
            {
                "_id": 0,
                "id": 1,
                "cloud_region": 1,
                "current_location": 1,
                "storage_cost_per_gb": 1,
            },
        )
    )
    if not candidates:
        raise HTTPException(404, "no files available for simulation")

    rng = random.Random()
    produced = 0
    migrations = 0
    session = requests.Session() if payload.stream_events else None
    global _simulator_event_counter

    for _ in range(max(0, payload.events)):
        record = rng.choice(candidates)
        file_id = record["id"]
        event_type = "write" if rng.random() < 0.3 else "read"
        base_bytes = rng.randint(5_000, 500_000)
        event = AccessEvent(
            file_id=file_id,
            event=event_type,
            ts=time.time(),
            client_id=f"burst-{rng.randint(1, 400)}",
            bytes_read=base_bytes if event_type == "read" else 0,
            bytes_written=base_bytes if event_type == "write" else 0,
            latency_ms=rng.uniform(12.0, 220.0),
            temperature=rng.uniform(50.0, 98.0),
            high_temp_alert=rng.random() < 0.08,
            egress_cost=rng.uniform(0.0, 1.0),
            storage_cost_per_gb=record.get("storage_cost_per_gb", 0.05),
            cloud_region=record.get("cloud_region", "us-east-1"),
            sync_conflict=rng.random() < 0.03,
            failed_read=rng.random() < 0.015,
            network_failure=False,
            success=True,
            source="burst",
        )
        try:
            ingest_event(event)
        except HTTPException:
            pass
        produced += 1
        _simulator_event_counter += 1
        if payload.stream_events and session is not None:
            _post_stream_event(session, _stream_event_payload(file_id, event, _simulator_event_counter))

        if payload.include_moves and rng.random() < 0.1:
            targets = [loc for loc in LOCATION_TO_TIER.keys() if loc != record.get("current_location")]
            if targets:
                try:
                    move(MoveRequest(file_id=file_id, target=rng.choice(targets)))
                    migrations += 1
                except HTTPException:
                    pass

        if payload.pace_ms > 0:
            time.sleep(payload.pace_ms / 1000.0)

    return {
        "events_generated": produced,
        "migrations_triggered": migrations,
        "stream_enqueued": bool(payload.stream_events),
    }

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


@app.get("/consistency/status")
def consistency_status():
    if coll_files is None:
        raise HTTPException(503, "db not ready")
    if consistency_mgr is None:
        return {"status": "disabled", "replicas": []}
    return consistency_mgr.status()


@app.post("/consistency/resync")
def consistency_resync():
    if coll_files is None:
        raise HTTPException(503, "db not ready")
    if consistency_mgr is None:
        raise HTTPException(503, "consistency manager not ready")
    return consistency_mgr.reconcile_pending()
