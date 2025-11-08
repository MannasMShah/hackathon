import json
import logging
import os
import random
import threading
import time
from collections import Counter
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
SIMULATION_THROTTLE = float(os.getenv("SYNTHETIC_LOAD_INTERVAL", "0.75"))
DATASET_POLICY_MODE = os.getenv("ENABLE_DATASET_POLICY_TRIGGERS", "0").lower() in {"1", "true", "yes"}
SIZE_KB_PER_GB = 1024 * 1024

ALERT_COST_THRESHOLD = float(os.getenv("ALERT_COST_THRESHOLD", "0.08"))
ALERT_LATENCY_P95_MS = float(os.getenv("ALERT_LATENCY_P95_MS", "180"))
ALERT_STREAM_RATE = float(os.getenv("ALERT_STREAM_EVENTS_PER_MIN", "15"))
ALERT_CONFIDENCE_THRESHOLD = float(os.getenv("ALERT_CONFIDENCE_THRESHOLD", "0.9"))
STREAM_LOW_ACTIVITY_THRESHOLD = float(os.getenv("ALERT_STREAM_LOW_ACTIVITY", "5"))
ALERT_EGRESS_COST_SPIKE = float(os.getenv("ALERT_EGRESS_COST_SPIKE", "50"))
ALERT_CROSS_CLOUD_EGRESS_COUNT = float(os.getenv("ALERT_CROSS_CLOUD_EGRESS_COUNT", "30"))
ALERT_COST_ANOMALY_MULTIPLIER = float(os.getenv("ALERT_COST_ANOMALY_MULTIPLIER", "1.8"))
ALERT_LATENCY_MAX_MS = float(os.getenv("ALERT_LATENCY_MAX_MS", "400"))
ALERT_LATENCY_DELTA_MS = float(os.getenv("ALERT_LATENCY_DELTA_MS", "120"))
ALERT_BURST_RATIO = float(os.getenv("ALERT_BURST_RATIO", "1.5"))
ALERT_GROWTH_RATE_SPIKE = float(os.getenv("ALERT_GROWTH_RATE_SPIKE", "120"))
ALERT_GROWTH_RATE_DROP = float(os.getenv("ALERT_GROWTH_RATE_DROP", "80"))
ALERT_TEMP_ALERT_COUNT = float(os.getenv("ALERT_TEMP_ALERT_COUNT", "3"))
ALERT_UNIQUE_CLIENT_SURGE_RATIO = float(os.getenv("ALERT_UNIQUE_CLIENT_SURGE_RATIO", "1.5"))
ALERT_WRITE_RATIO_THRESHOLD = float(os.getenv("ALERT_WRITE_RATIO_THRESHOLD", "0.7"))
ALERT_MOVE_FAILURE_THRESHOLD = float(os.getenv("ALERT_MOVE_FAILURE_THRESHOLD", "2"))
ALERT_COOLING_EMA_THRESHOLD = float(os.getenv("ALERT_COOLING_EMA_THRESHOLD", "4"))

GLOBAL_POLICY_DOC_ID = "global_policy_state"
GLOBAL_POLICY_COST_THRESHOLD = float(os.getenv("GLOBAL_POLICY_COST_THRESHOLD", "6.0"))
GLOBAL_POLICY_LATENCY_THRESHOLD = float(os.getenv("GLOBAL_POLICY_LATENCY_THRESHOLD", "220.0"))
GLOBAL_POLICY_STREAM_THRESHOLD = float(os.getenv("GLOBAL_POLICY_STREAM_THRESHOLD", "120.0"))
GLOBAL_POLICY_EGRESS_THRESHOLD = float(os.getenv("GLOBAL_POLICY_EGRESS_THRESHOLD", "250.0"))

logger = logging.getLogger("netapp.api")

app = FastAPI(title="NetApp Data-in-Motion API")

# --- globals filled at startup ---
mongo: Optional[MongoClient] = None
db = None
coll_files = None
coll_events = None
coll_sync = None
coll_global_state = None
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
    client_region: Optional[str] = None
    bytes_read: Optional[int] = 0
    bytes_written: Optional[int] = 0
    latency_ms: Optional[float] = None
    temperature: Optional[float] = None
    high_temp_alert: bool = False
    egress_cost: Optional[float] = 0.0
    egress_cloud: Optional[str] = None
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
    "egress_to_s3_last_1hr": 0.0,
    "egress_to_azure_last_1hr": 0.0,
    "egress_to_gcs_last_1hr": 0.0,
    "write_ratio_last_10min": 0.0,
    "burst_score_10min": 0.0,
    "burst_ratio_last_minute": 0.0,
    "client_region_diversity_last_30min": 0.0,
    "recent_move_failures": 0.0,
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

USAGE_METRIC_FIELDS = [
    "req_count_last_1min",
    "req_count_last_10min",
    "req_count_last_1hr",
    "bytes_read_last_10min",
    "bytes_written_last_10min",
    "unique_clients_last_30min",
    "avg_latency_1min",
    "p95_latency_5min",
    "max_latency_10min",
    "ema_req_5min",
    "ema_req_30min",
    "growth_rate_10min",
    "delta_latency_5min",
    "events_per_minute",
    "high_temp_alerts_last_10min",
    "egress_cost_last_1hr",
    "egress_to_s3_last_1hr",
    "egress_to_azure_last_1hr",
    "egress_to_gcs_last_1hr",
    "write_ratio_last_10min",
    "burst_score_10min",
    "burst_ratio_last_minute",
    "client_region_diversity_last_30min",
    "recent_move_failures",
    "num_recent_migrations",
    "time_since_last_migration",
    "sync_conflicts_last_1hr",
    "failed_reads_last_10min",
    "network_failures_last_hour",
]

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
    existing_doc: Optional[Dict[str, Any]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    now_iso = datetime.now(timezone.utc).isoformat()
    current_tier = (inferred_tier or "unknown").lower()
    predicted_tier_normalised = (predicted_tier or "").lower()
    confidence = float(prediction_confidence or 0.0)

    events_per_minute = float(feature_source.get("events_per_minute", 0.0) or 0.0)
    p95_latency = float(feature_source.get("p95_latency_5min", 0.0) or 0.0)
    avg_latency = float(feature_source.get("avg_latency_1min", 0.0) or 0.0)
    cost_estimate = float(estimated_cost)
    egress_cost_last_1hr = float(feature_source.get("egress_cost_last_1hr", 0.0) or 0.0)
    max_latency = float(feature_source.get("max_latency_10min", 0.0) or 0.0)
    growth_rate = float(feature_source.get("growth_rate_10min", 0.0) or 0.0)
    delta_latency = float(feature_source.get("delta_latency_5min", 0.0) or 0.0)
    write_ratio = float(feature_source.get("write_ratio_last_10min", 0.0) or 0.0)
    burst_score = float(feature_source.get("burst_score_10min", 0.0) or 0.0)
    burst_ratio = float(feature_source.get("burst_ratio_last_minute", 0.0) or 0.0)
    client_diversity = float(feature_source.get("client_region_diversity_last_30min", 0.0) or 0.0)
    recent_move_failures = float(feature_source.get("recent_move_failures", 0.0) or 0.0)
    unique_clients = float(feature_source.get("unique_clients_last_30min", 0.0) or 0.0)
    req_count_last_10min = float(feature_source.get("req_count_last_10min", 0.0) or 0.0)
    ema_req_30 = float(feature_source.get("ema_req_30min", 0.0) or 0.0)
    egress_to_s3 = float(feature_source.get("egress_to_s3_last_1hr", 0.0) or 0.0)
    egress_to_azure = float(feature_source.get("egress_to_azure_last_1hr", 0.0) or 0.0)
    egress_to_gcs = float(feature_source.get("egress_to_gcs_last_1hr", 0.0) or 0.0)

    previous_doc = existing_doc or {}
    prev_cost = float(previous_doc.get("estimated_monthly_cost") or 0.0)
    prev_unique_clients = float(previous_doc.get("unique_clients_last_30min") or 0.0)
    prev_events_per_minute = float(previous_doc.get("events_per_minute") or 0.0)
    prev_req_count_10 = float(previous_doc.get("req_count_last_10min") or 0.0)
    prev_write_ratio = float(previous_doc.get("write_ratio_last_10min") or 0.0)
    prev_burst_ratio = float(previous_doc.get("burst_ratio_last_minute") or 0.0)
    prev_temp_alerts = float(previous_doc.get("high_temp_alerts_last_10min") or 0.0)

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

    if max_latency >= ALERT_LATENCY_MAX_MS:
        alerts.append(
            _carry_alert(
                {
                    "type": "latency_peak",
                    "severity": "critical",
                    "reason": "max_latency_spike",
                    "message": (
                        f"Max latency {max_latency:.1f} ms breached {ALERT_LATENCY_MAX_MS:.0f} ms"
                    ),
                    "metric": {"max_latency_10min": max_latency},
                }
            )
        )
        if current_tier != "hot":
            policies.append(
                _carry_policy(
                    {
                        "type": "latency_peak",
                        "action": "promote_tier",
                        "target_tier": "hot",
                        "target_location": TIER_TO_LOCATION.get("hot"),
                        "reason": "max_latency_spike",
                        "confidence": max(confidence, 0.9),
                        "status": "pending",
                    }
                )
            )

    if delta_latency >= ALERT_LATENCY_DELTA_MS:
        alerts.append(
            _carry_alert(
                {
                    "type": "latency_trend",
                    "severity": "warning",
                    "reason": "latency_delta_spike",
                    "message": (
                        f"Latency increased by {delta_latency:.1f} ms over last 5 minutes"
                    ),
                    "metric": {"delta_latency_5min": delta_latency},
                }
            )
        )
        if current_tier != "hot":
            policies.append(
                _carry_policy(
                    {
                        "type": "latency_trend",
                        "action": "promote_tier",
                        "target_tier": "hot",
                        "target_location": TIER_TO_LOCATION.get("hot"),
                        "reason": "latency_delta_spike",
                        "confidence": max(confidence, 0.88),
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

    egress_breakdown = {
        "azure": egress_to_azure,
        "s3": egress_to_s3,
        "gcs": egress_to_gcs,
    }
    dominant_egress_cloud = None
    dominant_egress_value = 0.0
    if any(val > 0.0 for val in egress_breakdown.values()):
        dominant_egress_cloud, dominant_egress_value = max(
            egress_breakdown.items(), key=lambda item: item[1]
        )

    if egress_cost_last_1hr >= ALERT_EGRESS_COST_SPIKE:
        alerts.append(
            _carry_alert(
                {
                    "type": "egress_cost_spike",
                    "severity": "warning",
                    "reason": "egress_cost_spike",
                    "message": (
                        f"Egress spend ₹{egress_cost_last_1hr:.2f} in last hour exceeds ₹{ALERT_EGRESS_COST_SPIKE:.2f}"
                    ),
                    "metric": {"egress_cost_last_1hr": egress_cost_last_1hr},
                }
            )
        )
        if current_tier not in {"cold"}:
            target_tier = (
                predicted_tier_normalised
                if predicted_tier_normalised in {"warm", "cold"}
                else "warm"
            )
            policies.append(
                _carry_policy(
                    {
                        "type": "egress_cost_spike",
                        "action": "optimize_cost",
                        "target_tier": target_tier,
                        "target_location": TIER_TO_LOCATION.get(target_tier),
                        "reason": "egress_cost_spike",
                        "confidence": max(confidence, 0.82),
                        "status": "pending",
                    }
                )
            )

    if dominant_egress_cloud and dominant_egress_value >= ALERT_CROSS_CLOUD_EGRESS_COUNT:
        alerts.append(
            _carry_alert(
                {
                    "type": "cross_cloud_egress",
                    "severity": "warning",
                    "reason": "cross_cloud_traffic",
                    "message": (
                        f"{dominant_egress_value:.0f} egress events targeting {dominant_egress_cloud.upper()}"
                    ),
                    "metric": {
                        "egress_events": dominant_egress_value,
                        "target_cloud": dominant_egress_cloud,
                    },
                }
            )
        )
        policies.append(
            _carry_policy(
                {
                    "type": "cross_cloud_alignment",
                    "action": "replicate_near_clients",
                    "target_tier": LOCATION_TO_TIER.get(dominant_egress_cloud, current_tier),
                    "target_location": dominant_egress_cloud,
                    "reason": "cross_cloud_traffic",
                    "confidence": max(confidence, 0.78),
                    "status": "pending",
                }
            )
        )

    if prev_cost > 0.0 and cost_estimate >= prev_cost * ALERT_COST_ANOMALY_MULTIPLIER:
        alerts.append(
            _carry_alert(
                {
                    "type": "cost_anomaly",
                    "severity": "warning",
                    "reason": "cost_jump",
                    "message": (
                        f"Estimated cost jumped from ₹{prev_cost:.2f} to ₹{cost_estimate:.2f}"
                    ),
                    "metric": {
                        "previous_cost": prev_cost,
                        "current_cost": cost_estimate,
                    },
                }
            )
        )
        if current_tier not in {"cold"}:
            target_tier = (
                predicted_tier_normalised
                if predicted_tier_normalised in {"warm", "cold"}
                else "cold"
            )
            policies.append(
                _carry_policy(
                    {
                        "type": "cost_anomaly",
                        "action": "demote_tier",
                        "target_tier": target_tier,
                        "target_location": TIER_TO_LOCATION.get(target_tier),
                        "reason": "cost_jump",
                        "confidence": max(confidence, 0.8),
                        "status": "pending",
                    }
            )
        )

    if burst_ratio >= ALERT_BURST_RATIO and burst_ratio > max(prev_burst_ratio, 0.0):
        alerts.append(
            _carry_alert(
                {
                    "type": "stream_burst",
                    "severity": "warning",
                    "reason": "burst_ratio_spike",
                    "message": (
                        f"Per-minute throughput surged {burst_ratio:.2f}x over baseline"
                    ),
                    "metric": {
                        "burst_ratio_last_minute": burst_ratio,
                        "burst_score_10min": burst_score,
                    },
                }
            )
        )
        target_tier = "hot" if predicted_tier_normalised == "hot" else "warm"
        policies.append(
            _carry_policy(
                {
                    "type": "stream_burst",
                    "action": "promote_tier",
                    "target_tier": target_tier,
                    "target_location": TIER_TO_LOCATION.get(target_tier),
                    "reason": "burst_ratio_spike",
                    "confidence": max(confidence, 0.86 if target_tier == "hot" else 0.8),
                    "status": "pending",
                }
            )
        )

    if growth_rate >= ALERT_GROWTH_RATE_SPIKE and req_count_last_10min > prev_req_count_10:
        alerts.append(
            _carry_alert(
                {
                    "type": "access_surge",
                    "severity": "warning",
                    "reason": "growth_rate_spike",
                    "message": (
                        f"10-minute request volume jumped by {growth_rate:.0f} ops"
                    ),
                    "metric": {"growth_rate_10min": growth_rate},
                }
            )
        )
        if current_tier != "hot":
            policies.append(
                _carry_policy(
                    {
                        "type": "access_surge",
                        "action": "promote_tier",
                        "target_tier": "hot",
                        "target_location": TIER_TO_LOCATION.get("hot"),
                        "reason": "growth_rate_spike",
                        "confidence": max(confidence, 0.87),
                        "status": "pending",
                    }
                )
            )

    if (
        prev_unique_clients > 0.0
        and unique_clients >= prev_unique_clients * ALERT_UNIQUE_CLIENT_SURGE_RATIO
    ):
        alerts.append(
            _carry_alert(
                {
                    "type": "client_surge",
                    "severity": "info",
                    "reason": "unique_clients_spike",
                    "message": (
                        f"Unique clients jumped from {prev_unique_clients:.0f} to {unique_clients:.0f}"
                    ),
                    "metric": {
                        "previous_unique_clients": prev_unique_clients,
                        "unique_clients_last_30min": unique_clients,
                        "client_region_diversity_last_30min": client_diversity,
                    },
                }
            )
        )
        policies.append(
            _carry_policy(
                {
                    "type": "client_surge",
                    "action": "replicate_near_clients",
                    "target_tier": LOCATION_TO_TIER.get(dominant_egress_cloud, current_tier)
                    if dominant_egress_cloud
                    else current_tier,
                    "target_location": dominant_egress_cloud if dominant_egress_cloud else None,
                    "reason": "unique_clients_spike",
                    "confidence": max(confidence, 0.75),
                    "status": "pending",
                }
            )
        )

    if (
        high_temp_alerts_last_10min >= ALERT_TEMP_ALERT_COUNT
        and high_temp_alerts_last_10min > prev_temp_alerts
    ):
        alerts.append(
            _carry_alert(
                {
                    "type": "temperature_anomaly",
                    "severity": "warning",
                    "reason": "temperature_spike",
                    "message": (
                        f"{high_temp_alerts_last_10min:.0f} high-temp alerts fired in 10 minutes"
                    ),
                    "metric": {"high_temp_alerts_last_10min": high_temp_alerts_last_10min},
                }
            )
        )
        if current_tier != "hot":
            policies.append(
                _carry_policy(
                    {
                        "type": "temperature_anomaly",
                        "action": "promote_tier",
                        "target_tier": "hot",
                        "target_location": TIER_TO_LOCATION.get("hot"),
                        "reason": "temperature_spike",
                        "confidence": max(confidence, 0.84),
                        "status": "pending",
                    }
                )
            )

    if write_ratio >= ALERT_WRITE_RATIO_THRESHOLD and write_ratio > prev_write_ratio:
        alerts.append(
            _carry_alert(
                {
                    "type": "write_heavy_shift",
                    "severity": "info",
                    "reason": "write_ratio_increase",
                    "message": (
                        f"Writes now {write_ratio:.0%} of operations in last 10 minutes"
                    ),
                    "metric": {"write_ratio_last_10min": write_ratio},
                }
            )
        )
        policies.append(
            _carry_policy(
                {
                    "type": "write_heavy_shift",
                    "action": "promote_tier",
                    "target_tier": "hot",
                    "target_location": TIER_TO_LOCATION.get("hot"),
                    "reason": "write_ratio_increase",
                    "confidence": max(confidence, 0.83),
                    "status": "pending",
                }
            )
        )

    if (
        growth_rate <= -ALERT_GROWTH_RATE_DROP
        and ema_req_30 <= ALERT_COOLING_EMA_THRESHOLD
        and events_per_minute <= STREAM_LOW_ACTIVITY_THRESHOLD
    ):
        alerts.append(
            _carry_alert(
                {
                    "type": "cooling_trend",
                    "severity": "info",
                    "reason": "cooling_pattern",
                    "message": "Workload cooling detected; usage trending down",
                    "metric": {
                        "growth_rate_10min": growth_rate,
                        "ema_req_30min": ema_req_30,
                    },
                }
            )
        )
        if current_tier not in {"cold"}:
            target_tier = "cold" if predicted_tier_normalised == "cold" else "warm"
            policies.append(
                _carry_policy(
                    {
                        "type": "cooling_trend",
                        "action": "demote_tier",
                        "target_tier": target_tier,
                        "target_location": TIER_TO_LOCATION.get(target_tier),
                        "reason": "cooling_pattern",
                        "confidence": max(confidence, 0.8),
                        "status": "pending",
                    }
                )
            )

    if recent_move_failures >= ALERT_MOVE_FAILURE_THRESHOLD:
        alerts.append(
            _carry_alert(
                {
                    "type": "migration_failure",
                    "severity": "warning",
                    "reason": "migration_failures",
                    "message": (
                        f"{recent_move_failures:.0f} migration failures recorded in last hour"
                    ),
                    "metric": {"recent_move_failures": recent_move_failures},
                }
            )
        )
        policies.append(
            _carry_policy(
                {
                    "type": "migration_failure",
                    "action": "hold_migrations",
                    "target_tier": current_tier,
                    "target_location": TIER_TO_LOCATION.get(current_tier),
                    "reason": "migration_failures",
                    "confidence": 0.7,
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

    if not DATASET_POLICY_MODE:
        policies = []

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


def _aggregate_database_metrics() -> Dict[str, Any]:
    if coll_files is None:
        return {
            "dataset_count": 0,
            "storage_gb_total": 0.0,
            "estimated_monthly_cost": 0.0,
            "avg_p95_latency": 0.0,
            "max_p95_latency": 0.0,
            "total_events_per_minute": 0.0,
            "total_egress_cost_last_1hr": 0.0,
            "cross_cloud_egress": 0.0,
            "active_alerts": 0,
            "tier_counts": {"hot": 0, "warm": 0, "cold": 0},
        }

    totals = {
        "dataset_count": 0,
        "storage_gb_total": 0.0,
        "estimated_monthly_cost": 0.0,
        "avg_p95_latency": 0.0,
        "max_p95_latency": 0.0,
        "total_events_per_minute": 0.0,
        "total_egress_cost_last_1hr": 0.0,
        "cross_cloud_egress": 0.0,
        "active_alerts": 0,
        "tier_counts": {"hot": 0, "warm": 0, "cold": 0},
    }

    latency_total = 0.0
    latency_count = 0

    cursor = coll_files.find(
        {},
        {
            "storage_gb_estimate": 1,
            "size_kb": 1,
            "estimated_monthly_cost": 1,
            "p95_latency_5min": 1,
            "max_latency_10min": 1,
            "events_per_minute": 1,
            "egress_cost_last_1hr": 1,
            "egress_to_s3_last_1hr": 1,
            "egress_to_azure_last_1hr": 1,
            "egress_to_gcs_last_1hr": 1,
            "active_alerts": 1,
            "current_tier": 1,
        },
    )

    for doc in cursor:
        totals["dataset_count"] += 1

        storage_gb = 0.0
        storage_val = doc.get("storage_gb_estimate")
        try:
            if storage_val is not None:
                storage_gb = float(storage_val)
        except (TypeError, ValueError):
            storage_gb = 0.0
        if storage_gb <= 0.0:
            try:
                storage_gb = float(doc.get("size_kb", 0.0)) / SIZE_KB_PER_GB
            except (TypeError, ValueError):
                storage_gb = 0.0
        totals["storage_gb_total"] += max(storage_gb, 0.0)

        try:
            totals["estimated_monthly_cost"] += float(doc.get("estimated_monthly_cost", 0.0) or 0.0)
        except (TypeError, ValueError):
            pass

        try:
            latency = float(doc.get("p95_latency_5min", 0.0) or 0.0)
        except (TypeError, ValueError):
            latency = 0.0
        if latency > 0.0:
            latency_total += latency
            latency_count += 1
            totals["max_p95_latency"] = max(totals["max_p95_latency"], latency)

        try:
            totals["total_events_per_minute"] += float(doc.get("events_per_minute", 0.0) or 0.0)
        except (TypeError, ValueError):
            pass

        try:
            totals["total_egress_cost_last_1hr"] += float(doc.get("egress_cost_last_1hr", 0.0) or 0.0)
        except (TypeError, ValueError):
            pass

        cross_cloud = 0.0
        for key in ("egress_to_s3_last_1hr", "egress_to_azure_last_1hr", "egress_to_gcs_last_1hr"):
            try:
                cross_cloud += float(doc.get(key, 0.0) or 0.0)
            except (TypeError, ValueError):
                pass
        totals["cross_cloud_egress"] += cross_cloud

        if isinstance(doc.get("active_alerts"), list):
            totals["active_alerts"] += len(doc["active_alerts"])

        tier = str(doc.get("current_tier") or "").lower()
        if tier in totals["tier_counts"]:
            totals["tier_counts"][tier] += 1

    if latency_count > 0:
        totals["avg_p95_latency"] = latency_total / latency_count

    totals["storage_gb_total"] = round(totals["storage_gb_total"], 4)
    totals["estimated_monthly_cost"] = round(totals["estimated_monthly_cost"], 4)
    totals["avg_p95_latency"] = round(totals["avg_p95_latency"], 2)
    totals["max_p95_latency"] = round(totals["max_p95_latency"], 2)
    totals["total_events_per_minute"] = round(totals["total_events_per_minute"], 2)
    totals["total_egress_cost_last_1hr"] = round(totals["total_egress_cost_last_1hr"], 2)
    totals["cross_cloud_egress"] = round(totals["cross_cloud_egress"], 2)

    return totals


def _refresh_global_policy_state() -> None:
    if coll_global_state is None:
        return

    totals = _aggregate_database_metrics()
    now_iso = datetime.now(timezone.utc).isoformat()

    existing = coll_global_state.find_one({"_id": GLOBAL_POLICY_DOC_ID}) or {}
    existing_alerts = existing.get("alerts") or []
    existing_policies = existing.get("policy_triggers") or []

    prev_alert_map = {
        _alert_signature(alert): alert for alert in existing_alerts if isinstance(alert, dict)
    }
    prev_policy_map = {
        _policy_signature(policy): policy for policy in existing_policies if isinstance(policy, dict)
    }

    alerts: List[Dict[str, Any]] = []
    policies: List[Dict[str, Any]] = []

    def _carry_alert(alert: Dict[str, Any]) -> Dict[str, Any]:
        alert.setdefault("triggered_at", now_iso)
        signature = _alert_signature(alert)
        if signature in prev_alert_map:
            alert.setdefault("triggered_at", prev_alert_map[signature].get("triggered_at", now_iso))
        alert.setdefault("scope", "database")
        return alert

    def _carry_policy(policy: Dict[str, Any]) -> Dict[str, Any]:
        policy.setdefault("triggered_at", now_iso)
        policy.setdefault("scope", "database")
        signature = _policy_signature(policy)
        if signature in prev_policy_map:
            existing_policy = prev_policy_map[signature]
            policy.setdefault("triggered_at", existing_policy.get("triggered_at", now_iso))
            if policy.get("confidence") in (None, 0.0):
                policy["confidence"] = existing_policy.get("confidence")
        return policy

    total_cost = totals.get("estimated_monthly_cost", 0.0) or 0.0
    if total_cost >= GLOBAL_POLICY_COST_THRESHOLD:
        alerts.append(
            _carry_alert(
                {
                    "type": "global_cost",
                    "severity": "warning",
                    "reason": "database_cost_budget",
                    "message": (
                        f"Aggregate monthly cost ₹{total_cost:.2f} exceeds ₹{GLOBAL_POLICY_COST_THRESHOLD:.2f}"
                    ),
                    "metric": {"estimated_monthly_cost": total_cost},
                }
            )
        )
        policies.append(
            _carry_policy(
                {
                    "type": "global_cost",
                    "action": "optimise_spend",
                    "target": "database",
                    "reason": "aggregate_cost_budget",
                    "confidence": 0.82,
                    "metric": {"estimated_monthly_cost": total_cost},
                }
            )
        )

    avg_latency = totals.get("avg_p95_latency", 0.0) or 0.0
    if avg_latency >= GLOBAL_POLICY_LATENCY_THRESHOLD:
        alerts.append(
            _carry_alert(
                {
                    "type": "global_latency",
                    "severity": "critical",
                    "reason": "p95_latency_breach",
                    "message": (
                        f"Average p95 latency {avg_latency:.1f} ms crosses {GLOBAL_POLICY_LATENCY_THRESHOLD:.1f} ms"
                    ),
                    "metric": {"avg_p95_latency": avg_latency},
                }
            )
        )
        policies.append(
            _carry_policy(
                {
                    "type": "global_latency",
                    "action": "promote_hot_tier",
                    "target": "database",
                    "reason": "p95_latency_breach",
                    "confidence": 0.86,
                    "metric": {"avg_p95_latency": avg_latency},
                }
            )
        )

    throughput = totals.get("total_events_per_minute", 0.0) or 0.0
    if throughput >= GLOBAL_POLICY_STREAM_THRESHOLD:
        alerts.append(
            _carry_alert(
                {
                    "type": "global_streaming",
                    "severity": "info",
                    "reason": "kafka_throughput_spike",
                    "message": (
                        f"Kafka throughput {throughput:.1f}/min exceeds {GLOBAL_POLICY_STREAM_THRESHOLD:.1f}/min"
                    ),
                    "metric": {"total_events_per_minute": throughput},
                }
            )
        )
        policies.append(
            _carry_policy(
                {
                    "type": "global_streaming",
                    "action": "scale_stream_consumers",
                    "target": "database",
                    "reason": "kafka_throughput_spike",
                    "confidence": 0.8,
                    "metric": {"total_events_per_minute": throughput},
                }
            )
        )

    cross_cloud = totals.get("cross_cloud_egress", 0.0) or 0.0
    if cross_cloud >= GLOBAL_POLICY_EGRESS_THRESHOLD:
        alerts.append(
            _carry_alert(
                {
                    "type": "global_egress",
                    "severity": "warning",
                    "reason": "cross_cloud_traffic",
                    "message": (
                        f"Cross-cloud egress {cross_cloud:.1f} GB crosses {GLOBAL_POLICY_EGRESS_THRESHOLD:.1f} GB"
                    ),
                    "metric": {"cross_cloud_egress": cross_cloud},
                }
            )
        )
        policies.append(
            _carry_policy(
                {
                    "type": "global_egress",
                    "action": "rebalance_regions",
                    "target": "database",
                    "reason": "cross_cloud_traffic",
                    "confidence": 0.78,
                    "metric": {"cross_cloud_egress": cross_cloud},
                }
            )
        )

    state = {
        "_id": GLOBAL_POLICY_DOC_ID,
        "totals": totals,
        "alerts": alerts,
        "policy_triggers": policies,
        "updated_at": now_iso,
    }

    coll_global_state.replace_one({"_id": GLOBAL_POLICY_DOC_ID}, state, upsert=True)


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
        existing_doc = coll_files.find_one({"id": m.get("id")}, {"_id": 0}) if coll_files else None
        base_doc = dict(m)
        loc_value = str(base_doc.get("current_location") or "").lower()
        tier_value = str(base_doc.get("current_tier") or "").lower()
        if loc_value not in LOCATION_TO_TIER:
            if tier_value in TIER_TO_LOCATION:
                loc_value = TIER_TO_LOCATION[tier_value] or "s3"
            else:
                loc_value = random.choice(list(LOCATION_TO_TIER.keys()))
        base_doc["current_location"] = loc_value
        if not tier_value or tier_value == "unknown":
            base_doc["current_tier"] = LOCATION_TO_TIER.get(
                loc_value, FEATURE_DEFAULTS["current_tier"]
            )
        for key, default in FEATURE_DEFAULTS.items():
            base_doc.setdefault(key, default)

        try:
            size_gb = float(base_doc.get("size_kb", 0.0)) / SIZE_KB_PER_GB
        except (TypeError, ValueError):
            size_gb = 0.0
        if size_gb <= 0.0 and base_doc.get("size_kb") not in (None, ""):
            size_gb = 0.001
        cost_rate_raw = base_doc.get("storage_cost_per_gb", FEATURE_DEFAULTS["storage_cost_per_gb"])
        try:
            cost_rate = float(cost_rate_raw)
        except (TypeError, ValueError):
            cost_rate = FEATURE_DEFAULTS["storage_cost_per_gb"]
        if cost_rate <= 0.0:
            cost_rate = FEATURE_DEFAULTS["storage_cost_per_gb"] or 0.05
        estimated_cost = round(size_gb * cost_rate, 4) if size_gb > 0.0 else 0.0
        base_doc["storage_gb_estimate"] = round(size_gb, 4)
        base_doc["estimated_monthly_cost"] = estimated_cost
        if not base_doc.get("predicted_tier") or str(base_doc.get("predicted_tier")).lower() in {"", "unknown"}:
            base_doc["predicted_tier"] = base_doc.get("current_tier", FEATURE_DEFAULTS["current_tier"])
        if not base_doc.get("prediction_confidence"):
            base_doc["prediction_confidence"] = 0.7
        base_doc.setdefault("prediction_source", "seed_rule")
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

        try:
            evaluation = _evaluate_alerts(
                existing_doc.get("active_alerts") if existing_doc else None,
                existing_doc.get("policy_triggers") if existing_doc else None,
                base_doc,
                base_doc.get("predicted_tier"),
                base_doc.get("prediction_confidence"),
                base_doc.get("current_tier", FEATURE_DEFAULTS["current_tier"]),
                base_doc.get("estimated_monthly_cost", 0.0),
                existing_doc,
            )
            base_doc["active_alerts"] = evaluation.get("alerts", [])
            base_doc["policy_triggers"] = (
                evaluation.get("policies", []) if DATASET_POLICY_MODE else []
            )
            base_doc["last_alert_eval_ts"] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            logger.debug("seed alert evaluation failed for %s: %s", m.get("id"), exc)

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

            burst = rng.randint(160, 320)
            for _ in range(burst):
                if _simulator_stop.is_set():
                    break
                record = rng.choice(files)
                file_id = record.get("id")
                if not file_id:
                    continue
                event_type = "write" if rng.random() < 0.35 else "read"
                base_bytes = rng.randint(20_000, 500_000)
                latency = rng.uniform(18.0, 260.0)
                access = AccessEvent(
                    file_id=file_id,
                    event=event_type,
                    ts=time.time(),
                    client_id=f"svc-{rng.randint(1, 200)}",
                    client_region=rng.choice(
                        ["us-east-1", "eu-west-1", "ap-south-1", "us-west-2"]
                    ),
                    bytes_read=base_bytes if event_type == "read" else 0,
                    bytes_written=base_bytes if event_type == "write" else 0,
                    latency_ms=latency,
                    temperature=rng.uniform(45.0, 95.0),
                    high_temp_alert=rng.random() < 0.05,
                    egress_cost=rng.uniform(0.0, 0.75),
                    egress_cloud=rng.choice(
                        [
                            loc
                            for loc in LOCATION_TO_TIER.keys()
                            if loc != record.get("current_location")
                        ]
                        or list(LOCATION_TO_TIER.keys())
                    ),
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

    # Refresh documents so the newly trained model populates predictions and
    # alert fields immediately instead of waiting for fresh traffic.
    for row in rows:
        file_id = row.get("file_id")
        if not file_id:
            continue
        try:
            _update_usage_metrics(str(file_id))
        except Exception as exc:
            logger.debug("post-bootstrap metrics refresh failed for %s: %s", file_id, exc)


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
    had_events = bool(events)

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
    egress_targets = Counter(
        str(e.get("egress_cloud", "")).lower() for e in last_hour if e.get("egress_cloud")
    )
    egress_to_s3_last_1hr = float(egress_targets.get("s3", 0.0))
    egress_to_azure_last_1hr = float(egress_targets.get("azure", 0.0))
    egress_to_gcs_last_1hr = float(egress_targets.get("gcs", 0.0))
    writes_last_10 = [e for e in last_10 if str(e.get("event", "")).lower() == "write"]
    total_last_10 = float(len(last_10))
    write_ratio_last_10min = float(len(writes_last_10)) / total_last_10 if total_last_10 else 0.0
    prev_10_count = float(len(prev_10))
    burst_score_10min = req_count_last_10min / max(prev_10_count, 1.0)
    prev_minute_rate = float(len(prev_5)) / 5.0 if prev_5 else 0.0
    burst_ratio_last_minute = (
        req_count_last_1min / max(prev_minute_rate, 1.0)
        if prev_minute_rate
        else req_count_last_1min
    )
    client_regions = {
        str(e.get("client_region", "")).lower()
        for e in last_30
        if e.get("client_region")
    }
    client_region_diversity = float(len(client_regions))
    recent_move_failures = float(
        coll_events.count_documents(
            {
                "type": "move_error",
                "file_id": file_id,
                "ts": {"$gte": now - 3600.0},
            }
        )
    )

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

    existing = coll_files.find_one({"id": file_id}, {"_id": 0})

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
        "egress_to_s3_last_1hr": egress_to_s3_last_1hr,
        "egress_to_azure_last_1hr": egress_to_azure_last_1hr,
        "egress_to_gcs_last_1hr": egress_to_gcs_last_1hr,
        "write_ratio_last_10min": write_ratio_last_10min,
        "burst_score_10min": burst_score_10min,
        "burst_ratio_last_minute": burst_ratio_last_minute,
        "client_region_diversity_last_30min": client_region_diversity,
        "recent_move_failures": recent_move_failures,
        "sync_conflicts_last_1hr": sync_conflicts_last_1hr,
        "failed_reads_last_10min": failed_reads_last_10min,
        "network_failures_last_hour": network_failures_last_hour,
        "current_tier": inferred_tier,
    }

    if not had_events and existing:
        for field in USAGE_METRIC_FIELDS:
            if field == "current_tier":
                continue
            value = existing.get(field)
            if value not in (None, ""):
                updates[field] = value

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

    if not predicted_tier or str(predicted_tier).lower() in {"", "unknown"}:
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

    tier_normalised = str(predicted_tier or "").lower()
    if tier_normalised in {"", "unknown"}:
        tier_normalised = str(inferred_tier or FEATURE_DEFAULTS["current_tier"]).lower()
        predicted_tier = tier_normalised

    # Normalise the confidence so the UI never shows implausible 0% or 100% values.
    try:
        prediction_confidence = float(prediction_confidence) if prediction_confidence is not None else 0.0
    except (TypeError, ValueError):
        prediction_confidence = 0.0
    if prediction_confidence <= 0.0:
        prediction_confidence = 0.55
    prediction_confidence = max(0.55, min(prediction_confidence, 0.97))

    raw_size = feature_source.get("size_kb")
    storage_gb_estimate = 0.0
    try:
        if raw_size is not None:
            storage_gb_estimate = float(raw_size) / SIZE_KB_PER_GB
    except (TypeError, ValueError):
        storage_gb_estimate = 0.0
    if storage_gb_estimate <= 0.0 and existing:
        try:
            storage_gb_estimate = float(existing.get("size_kb", 0.0)) / SIZE_KB_PER_GB
        except (TypeError, ValueError):
            storage_gb_estimate = 0.0
    if storage_gb_estimate <= 0.0 and existing:
        prev_storage = existing.get("storage_gb_estimate")
        try:
            if prev_storage:
                storage_gb_estimate = float(prev_storage)
        except (TypeError, ValueError):
            pass
    if storage_gb_estimate <= 0.0 and feature_source.get("size_kb") not in (None, ""):
        storage_gb_estimate = 0.001

    cost_rate_raw = feature_source.get("storage_cost_per_gb", FEATURE_DEFAULTS["storage_cost_per_gb"])
    try:
        cost_rate = float(cost_rate_raw)
    except (TypeError, ValueError):
        cost_rate = FEATURE_DEFAULTS["storage_cost_per_gb"]
    if cost_rate <= 0.0:
        cost_rate = FEATURE_DEFAULTS["storage_cost_per_gb"] or 0.05

    estimated_cost = storage_gb_estimate * cost_rate
    if storage_gb_estimate > 0.0 and estimated_cost <= 0.0:
        estimated_cost = storage_gb_estimate * max(cost_rate, FEATURE_DEFAULTS["storage_cost_per_gb"] or 0.05)
    if estimated_cost <= 0.0 and existing:
        prev_cost = existing.get("estimated_monthly_cost")
        try:
            if prev_cost:
                estimated_cost = float(prev_cost)
        except (TypeError, ValueError):
            pass

    storage_gb_estimate = round(storage_gb_estimate, 4)
    estimated_cost = round(estimated_cost, 4)

    if existing and (
        not isinstance(predicted_tier, str)
        or predicted_tier.strip() == ""
        or predicted_tier.lower() == "unknown"
    ):
        prev_pred = existing.get("predicted_tier")
        if isinstance(prev_pred, str) and prev_pred.strip():
            predicted_tier = prev_pred

    if existing:
        try:
            prev_conf = float(existing.get("prediction_confidence", 0.0) or 0.0)
        except (TypeError, ValueError):
            prev_conf = 0.0
        if prev_conf > 0.0:
            prediction_confidence = max(prediction_confidence, max(0.55, min(prev_conf, 0.97)))

    updates.update(
        {
            "predicted_tier": predicted_tier,
            "prediction_confidence": prediction_confidence,
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
        existing,
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
        try:
            _refresh_global_policy_state()
        except Exception as exc:
            logger.debug("global policy refresh failed (no consistency mgr) for %s: %s", file_id, exc)
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

    try:
        _refresh_global_policy_state()
    except Exception as exc:
        logger.debug("global policy refresh failed for %s: %s", file_id, exc)

# --------- lifecycle ----------
@app.on_event("startup")
def on_startup():
    """
    Robust startup with retries. Keep /health responsive even if deps are warming up.
    """
    global mongo, db, coll_files, coll_events, coll_sync, coll_global_state, producer, consistency_mgr

    # 1) Mongo
    def _mongo():
        global mongo, db, coll_files, coll_events, coll_sync, coll_global_state
        mongo = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
        _ = mongo.admin.command("ping")
        db = mongo["netapp"]
        coll_files = db["files"]
        coll_events = db["events"]
        coll_sync = db["sync_queue"]
        coll_global_state = db["global_state"]
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

    try:
        _refresh_global_policy_state()
    except Exception as exc:
        logger.debug("initial global policy refresh failed: %s", exc)


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


@app.get("/policy/global")
def global_policy():
    if coll_global_state is None:
        raise HTTPException(503, "db not ready")

    try:
        _refresh_global_policy_state()
    except Exception:
        pass

    state = coll_global_state.find_one({"_id": GLOBAL_POLICY_DOC_ID}, {"_id": 0})
    if not state:
        totals = _aggregate_database_metrics()
        return {
            "totals": totals,
            "policy_triggers": [],
            "alerts": [],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    state.setdefault("totals", _aggregate_database_metrics())
    state.setdefault("policy_triggers", [])
    state.setdefault("alerts", [])
    return state


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
            client_region=rng.choice(["us-east-1", "eu-west-1", "ap-south-1", "us-west-2"]),
            bytes_read=base_bytes if event_type == "read" else 0,
            bytes_written=base_bytes if event_type == "write" else 0,
            latency_ms=rng.uniform(12.0, 220.0),
            temperature=rng.uniform(50.0, 98.0),
            high_temp_alert=rng.random() < 0.08,
            egress_cost=rng.uniform(0.0, 1.0),
            egress_cloud=rng.choice(
                [
                    loc
                    for loc in LOCATION_TO_TIER.keys()
                    if loc != record.get("current_location")
                ]
                or list(LOCATION_TO_TIER.keys())
            ),
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
