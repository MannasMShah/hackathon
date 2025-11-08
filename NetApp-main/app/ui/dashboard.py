from __future__ import annotations

import os
import time
from collections import Counter
from typing import Dict, List, Tuple
import pandas as pd
import requests
import streamlit as st


API = "http://api:8000"
STREAM_API = os.getenv("STREAM_API", "http://stream-api:8001")
SIZE_KB_PER_GB = 1024 * 1024
TIER_TO_LOCATION = {"hot": "azure", "warm": "s3", "cold": "gcs"}


st.set_page_config(page_title="NetApp Data-in-Motion", layout="wide")
st.title("📊 NetApp Data-in-Motion — Mission Control")


@st.cache_data(ttl=5.0)
def fetch_files_payload() -> List[Dict[str, object]]:
    response = requests.get(f"{API}/files", timeout=5)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, list):
        return payload
    return []


@st.cache_data(ttl=10.0)
def fetch_health() -> Dict[str, object]:
    response = requests.get(f"{API}/health", timeout=5)
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict):
        return data
    return {"status": "unknown"}


@st.cache_data(ttl=15.0)
def fetch_policy(file_id: str) -> Dict[str, object]:
    response = requests.get(f"{API}/policy/{file_id}", timeout=5)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        return payload
    return {"file_id": file_id, "recommendation": "unknown", "source": "n/a", "features": {}}


@st.cache_data(ttl=5.0)
def fetch_stream_metrics(limit: int = 240) -> Dict[str, object]:
    metrics: Dict[str, object] = {
        "reachable": False,
        "throughput_per_min": 0.0,
        "active_devices": 0,
        "events": [],
        "total_events": 0,
        "producer_ready": False,
        "stream_api_online": False,
        "status": "offline",
    }

    try:
        snapshot = requests.get(
            f"{API}/streaming/metrics",
            params={"limit": limit},
            timeout=5,
        )
        snapshot.raise_for_status()
        payload = snapshot.json()
        if isinstance(payload, dict):
            metrics["throughput_per_min"] = float(payload.get("throughput_per_min", 0.0) or 0.0)
            metrics["active_devices"] = int(payload.get("active_devices", 0) or 0)
            metrics["events"] = payload.get("events", []) or []
            metrics["total_events"] = int(payload.get("total_events", metrics["total_events"]) or 0)
            metrics["producer_ready"] = bool(payload.get("producer_ready"))
            metrics["kafka_bootstrap"] = payload.get("kafka_bootstrap")
            metrics["topic"] = payload.get("topic")
            metrics["reachable"] = True
            metrics["status"] = "api"
    except Exception:
        pass

    try:
        resp = requests.get(
            f"{STREAM_API}/stream/peek",
            params={"n": limit},
            timeout=4,
        )
        resp.raise_for_status()
        events = resp.json()
        if isinstance(events, list):
            metrics["events"] = events
            now = time.time()
            recent = [
                evt for evt in events if isinstance(evt, dict) and now - float(evt.get("timestamp", 0.0) or 0.0) <= 60.0
            ]
            if recent:
                oldest = min(float(evt.get("timestamp", now)) for evt in recent)
                window = max(now - oldest, 1.0)
                metrics["throughput_per_min"] = float(len(recent)) * 60.0 / window
            metrics["active_devices"] = len({evt.get("device_id") for evt in events if isinstance(evt, dict)})
            metrics["reachable"] = True
            metrics["stream_api_online"] = True
            metrics["status"] = "stream_api"
    except Exception:
        pass

    if metrics["status"] == "offline" and metrics.get("events"):
        metrics["status"] = "fallback"

    return metrics


def refresh_all_caches() -> None:
    fetch_files_payload.clear()
    fetch_health.clear()
    fetch_policy.clear()


def safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def storage_gb(row: Dict[str, object]) -> float:
    estimate = row.get("storage_gb_estimate")
    try:
        if estimate is not None:
            return float(estimate)
    except (TypeError, ValueError):
        pass
    size_kb = safe_float(row.get("size_kb"))
    return size_kb / SIZE_KB_PER_GB


def estimated_cost(row: Dict[str, object]) -> float:
    explicit = row.get("estimated_monthly_cost")
    try:
        if explicit is not None:
            return float(explicit)
    except (TypeError, ValueError):
        pass
    size_gb = storage_gb(row)
    return size_gb * safe_float(row.get("storage_cost_per_gb"))


def tier_palette(tier: str) -> str:
    if not isinstance(tier, str):
        return "⚪"
    tier_lower = tier.lower()
    if tier_lower == "hot":
        return "🔥"
    if tier_lower == "warm":
        return "🌤️"
    if tier_lower == "cold":
        return "🧊"
    return "⚪"


def series_or_zero(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column].fillna(0).astype(float)
    return pd.Series([0.0] * len(df), index=df.index if not df.empty else None)


def summarise_alerts(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    infos: List[str] = []
    if df.empty:
        return warnings, infos

    seen_warnings: set = set()
    seen_infos: set = set()

    latency_series = series_or_zero(df, "p95_latency_5min")
    hot_latency = df[latency_series > 150]
    for _, row in hot_latency.iterrows():
        msg = f"Latency spike — {row['id']} p95 latency {row.get('p95_latency_5min', 0):.1f} ms"
        if msg not in seen_warnings:
            warnings.append(msg)
            seen_warnings.add(msg)

    high_temp_series = series_or_zero(df, "high_temp_alerts_last_10min")
    high_temp = df[high_temp_series > 0]
    for _, row in high_temp.iterrows():
        msg = (
            f"Sensor alert — {row['id']} reported {int(row.get('high_temp_alerts_last_10min', 0))} high-temp events"
        )
        if msg not in seen_warnings:
            warnings.append(msg)
            seen_warnings.add(msg)

    failed_reads_series = series_or_zero(df, "failed_reads_last_10min")
    failed_reads = df[failed_reads_series > 0]
    for _, row in failed_reads.iterrows():
        msg = f"Read failures — {row['id']} saw {int(row.get('failed_reads_last_10min', 0))} errors"
        if msg not in seen_warnings:
            warnings.append(msg)
            seen_warnings.add(msg)

    if "events_per_minute" in df.columns:
        top_streams = df.sort_values("events_per_minute", ascending=False).head(3)
        for _, row in top_streams.iterrows():
            rate = safe_float(row.get("events_per_minute"))
            if rate > 0:
                msg = f"Kafka hot path — {row['id']} running {rate:.1f} events/min"
                if msg not in seen_infos:
                    infos.append(msg)
                    seen_infos.add(msg)

    if "active_alerts" in df.columns:
        for _, row in df.iterrows():
            alerts = row.get("active_alerts") or []
            for alert in alerts:
                if not isinstance(alert, dict):
                    continue
                message = alert.get("message") or alert.get("reason")
                if not message:
                    continue
                severity = str(alert.get("severity") or "info").lower()
                prefix = "⚠️" if severity in {"critical", "warning"} else "ℹ️"
                formatted = f"{prefix} {row['id']}: {message}"
                if severity in {"critical", "warning"}:
                    if formatted not in seen_warnings:
                        warnings.append(formatted)
                        seen_warnings.add(formatted)
                else:
                    if formatted not in seen_infos:
                        infos.append(formatted)
                        seen_infos.add(formatted)

    if "policy_triggers" in df.columns:
        for _, row in df.iterrows():
            policies = row.get("policy_triggers") or []
            for policy in policies:
                if not isinstance(policy, dict):
                    continue
                message = policy.get("reason") or policy.get("action")
                if not message:
                    continue
                action = policy.get("action") or "policy"
                confidence = policy.get("confidence")
                suffix = (
                    f" (confidence {float(confidence) * 100:.1f}%)"
                    if confidence not in (None, "")
                    else ""
                )
                formatted = f"ℹ️ {row['id']}: {action} — {message}{suffix}"
                if formatted not in seen_infos:
                    infos.append(formatted)
                    seen_infos.add(formatted)

    return warnings, infos


def render_health_status() -> None:
    try:
        health = fetch_health()
        status = health.get("status", "unknown")
        color = "green" if status == "ok" else "yellow" if status == "degraded" else "red"
        with st.container(border=True):
            st.markdown("**System health**")
            st.markdown(f"`{status.upper()}`")
            if color != "green":
                st.write(health)
    except Exception as exc:
        st.error(f"Health check failed: {exc}")


# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------
st.sidebar.header("Dashboard Controls")
if st.sidebar.button("🔄 Refresh data", use_container_width=True):
    refresh_all_caches()
    st.toast("Data refreshed", icon="✅")

st.sidebar.caption(
    "This dashboard fuses metadata, predictive insights, and live streaming metrics for the NetApp Data-in-Motion platform."
)


# ---------------------------------------------------------------------------
# Load metadata & derived metrics
# ---------------------------------------------------------------------------
files_payload: List[Dict[str, object]] = []
stream_metrics: Dict[str, object] = {"reachable": False, "throughput_per_min": 0.0, "active_devices": 0, "events": [], "total_events": 0}
try:
    files_payload = fetch_files_payload()
except Exception as exc:
    st.error(f"Failed to load metadata from API: {exc}")

try:
    stream_metrics = fetch_stream_metrics()
except Exception as exc:
    st.warning(f"Streaming API unreachable: {exc}")

df = pd.DataFrame(files_payload)
if not df.empty:
    numeric_cols = df.select_dtypes(include=["number"]).columns
    if len(numeric_cols) > 0:
        df.loc[:, numeric_cols] = df.loc[:, numeric_cols].fillna(0)


# ---------------------------------------------------------------------------
# Global overview
# ---------------------------------------------------------------------------
with st.container(border=True):
    st.subheader("1️⃣ Global Overview")
    if df.empty:
        st.info("No dataset metadata available yet. Trigger a seed run or ingest events to populate the control plane.")
    else:
        total_datasets = len(df)
        tier_counts = Counter(str(t).lower() for t in df.get("current_tier", []))
        hot = tier_counts.get("hot", 0)
        warm = tier_counts.get("warm", 0)
        cold = tier_counts.get("cold", 0)
        storage_total = sum(storage_gb(rec) for rec in files_payload)
        est_cost = sum(estimated_cost(rec) for rec in files_payload)
        events_series = series_or_zero(df, "events_per_minute")
        kafka_throughput = float(stream_metrics.get("throughput_per_min", 0.0))
        active_streams = int(stream_metrics.get("active_devices", 0))
        migrations_today = int(series_or_zero(df, "num_recent_migrations").sum())
        total_alerts = sum(
            len(record.get("active_alerts") or [])
            for record in files_payload
            if isinstance(record, dict)
        )
        total_policy_triggers = sum(
            len(record.get("policy_triggers") or [])
            for record in files_payload
            if isinstance(record, dict)
        )

        metrics_row = st.columns(6)
        metrics_row[0].metric("Datasets", total_datasets)
        metrics_row[1].metric("Tier mix", f"🔥 {hot} · 🌤️ {warm} · 🧊 {cold}")
        metrics_row[2].metric("Storage footprint", f"{storage_total:.2f} GB")
        metrics_row[3].metric("Cost (est/month)", f"₹{est_cost:,.2f}")
        metrics_row[4].metric("Active devices", active_streams)
        metrics_row[5].metric("Kafka throughput", f"{kafka_throughput:.1f} msg/min")

        meta_row = st.columns(2)
        with meta_row[0]:
            render_health_status()
        with meta_row[1]:
            st.markdown("**Ongoing migrations (24h)**")
            st.metric("Recent moves", migrations_today)
            signal_cols = st.columns(2)
            signal_cols[0].metric("Active alerts", total_alerts)
            signal_cols[1].metric("Policy triggers", total_policy_triggers)
            st.caption(f"Stream events seen: {int(stream_metrics.get('total_events', 0))}")
        st.caption("Tier defaults: Azure = HOT, S3 = WARM, GCS = COLD. Confidence ≥95% triggers eligible bulk moves.")


# ---------------------------------------------------------------------------
# Tabs for detailed exploration
# ---------------------------------------------------------------------------
overview_tab, datasets_tab, streaming_tab, ml_tab, migrations_tab = st.tabs(
    [
        "📦 Storage Snapshot",
        "📚 Datasets",
        "📡 Streaming",
        "🧠 ML Insights",
        "🚚 Migrations & Alerts",
    ]
)


with overview_tab:
    st.subheader("Storage & Cost Distribution")
    if df.empty:
        st.info("No records to visualise yet.")
    else:
        grouped = df.groupby("current_tier").agg(
            storage_gb=("size_kb", lambda s: float(s.fillna(0).sum()) / SIZE_KB_PER_GB),
            avg_latency=("p95_latency_5min", "mean"),
            avg_cost=("storage_cost_per_gb", "mean"),
            datasets=("id", "count"),
        )
        st.dataframe(grouped, use_container_width=True)

        region_group = df.groupby("cloud_region").agg(
            storage_gb=("size_kb", lambda s: float(s.fillna(0).sum()) / SIZE_KB_PER_GB),
            egress_last_hr=("egress_cost_last_1hr", "sum"),
            datasets=("id", "count"),
        )
        st.markdown("**Storage by region**")
        st.dataframe(region_group, use_container_width=True)


with datasets_tab:
    st.subheader("Interactive Dataset Explorer")
    if df.empty:
        st.info("No datasets available.")
    else:
        filters_col1, filters_col2, filters_col3 = st.columns(3)
        tier_filter = filters_col1.multiselect(
            "Filter tiers", options=sorted({str(t) for t in df.get("current_tier", [])}), default=[]
        )
        region_filter = filters_col2.multiselect(
            "Filter regions", options=sorted({str(r) for r in df.get("cloud_region", [])}), default=[]
        )
        activity_filter = filters_col3.selectbox(
            "Activity level", options=["All", "Hot (>100/min)", "Warm (>10/min)", "Quiet"], index=0
        )

        filtered_df = df.copy()
        if tier_filter and "current_tier" in filtered_df:
            filtered_df = filtered_df[filtered_df["current_tier"].astype(str).isin(tier_filter)]
        if region_filter and "cloud_region" in filtered_df:
            filtered_df = filtered_df[filtered_df["cloud_region"].astype(str).isin(region_filter)]
        if activity_filter != "All" and "req_count_last_1min" in filtered_df.columns:
            if activity_filter.startswith("Hot"):
                filtered_df = filtered_df[filtered_df["req_count_last_1min"].astype(float) > 100]
            elif activity_filter.startswith("Warm"):
                filtered_df = filtered_df[(filtered_df["req_count_last_1min"].astype(float) > 10) & (filtered_df["req_count_last_1min"].astype(float) <= 100)]
            else:
                filtered_df = filtered_df[filtered_df["req_count_last_1min"].astype(float) <= 10]

        display_cols = [
            "id",
            "current_tier",
            "current_location",
            "size_kb",
            "req_count_last_1min",
            "req_count_last_10min",
            "p95_latency_5min",
            "predicted_tier",
            "prediction_confidence",
        ]
        if "predicted_tier" not in filtered_df.columns:
            filtered_df = filtered_df.assign(predicted_tier="—")
        available_cols = [c for c in display_cols if c in filtered_df.columns]
        display_df = filtered_df[available_cols].copy()
        if "size_kb" in display_df.columns:
            display_df["storage_gb"] = display_df["size_kb"].apply(lambda v: safe_float(v) / SIZE_KB_PER_GB)
            display_df.drop(columns=["size_kb"], inplace=True)
        if "prediction_confidence" in display_df.columns:
            display_df["prediction_confidence"] = display_df["prediction_confidence"].apply(
                lambda v: f"{safe_float(v) * 100:.1f}%" if v is not None else "—"
            )
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
        )

        high_confidence_moves: List[Tuple[str, str, str, float]] = []
        for record in files_payload:
            if not isinstance(record, dict):
                continue
            dataset_id = record.get("id")
            tier = str(record.get("predicted_tier") or "").lower()
            confidence = safe_float(record.get("prediction_confidence"))
            if not dataset_id or not tier:
                continue
            target_location = TIER_TO_LOCATION.get(tier)
            current_location = str(record.get("current_location") or "").lower()
            if target_location and confidence >= 0.95 and current_location != target_location:
                high_confidence_moves.append((dataset_id, tier, target_location, confidence))

        bulk_cols = st.columns([1, 2, 2])
        bulk_cols[0].metric("Ready to move", len(high_confidence_moves))
        bulk_cols[1].markdown(
            "High-confidence recommendations (≥95%) will promote or demote data to the tier's default cloud: "
            "Azure → HOT, S3 → WARM, GCS → COLD."
        )
        if bulk_cols[2].button(
            "⚡ Apply high-confidence moves",
            disabled=not high_confidence_moves,
        ):
            successes = 0
            failures: List[Tuple[str, Exception]] = []
            with st.spinner("Applying predictive tier changes..."):
                for dataset_id, tier, target_location, confidence in high_confidence_moves:
                    try:
                        response = requests.post(
                            f"{API}/move",
                            json={"file_id": dataset_id, "target": target_location},
                            timeout=10,
                        )
                        response.raise_for_status()
                        successes += 1
                    except Exception as exc:
                        failures.append((dataset_id, exc))
            if successes:
                st.success(f"Triggered {successes} predictive migration(s) with ≥95% confidence")
                refresh_all_caches()
            for dataset_id, exc in failures:
                st.error(f"Failed to move {dataset_id}: {exc}")

        st.markdown("### Dataset drill-down")
        dataset_id = st.selectbox("Select dataset", options=filtered_df["id"].tolist())
        selected_row = df[df["id"] == dataset_id].iloc[0].to_dict()

        with st.container(border=True):
            cols = st.columns(4)
            cols[0].metric("Current tier", f"{tier_palette(selected_row.get('current_tier'))} {selected_row.get('current_tier','?')}")
            cols[1].metric("Location", str(selected_row.get("current_location", "—")))
            cols[2].metric("Size", f"{storage_gb(selected_row):.3f} GB")
            cols[3].metric("Access/min", f"{safe_float(selected_row.get('req_count_last_1min')):.1f}")

            cols = st.columns(4)
            cols[0].metric("p95 latency (5m)", f"{safe_float(selected_row.get('p95_latency_5min')):.1f} ms")
            cols[1].metric("EMA (30m)", f"{safe_float(selected_row.get('ema_req_30min')):.1f}")
            cols[2].metric("Events/min", f"{safe_float(selected_row.get('events_per_minute')):.1f}")
            cols[3].metric("High-temp alerts", int(safe_float(selected_row.get('high_temp_alerts_last_10min'))))

        alerts_block = selected_row.get("active_alerts") or []
        policies_block = selected_row.get("policy_triggers") or []
        with st.container(border=True):
            st.markdown("**Automated alerts**")
            if alerts_block:
                for alert in alerts_block:
                    if not isinstance(alert, dict):
                        continue
                    message = alert.get("message") or alert.get("reason") or "Alert triggered"
                    severity = str(alert.get("severity") or "info").lower()
                    timestamp = alert.get("triggered_at")
                    suffix = f" · since {timestamp}" if timestamp else ""
                    if severity in {"critical", "warning"}:
                        st.warning(f"{message}{suffix}")
                    else:
                        st.info(f"{message}{suffix}")
            else:
                st.caption("No automated alerts for this dataset.")

            st.markdown("**Policy triggers**")
            if policies_block:
                for policy in policies_block:
                    if not isinstance(policy, dict):
                        continue
                    action = policy.get("action") or "policy"
                    reason = policy.get("reason") or "auto"
                    confidence = policy.get("confidence")
                    target = policy.get("target_tier") or "—"
                    target_location = policy.get("target_location") or "—"
                    confidence_label = (
                        f"confidence {float(confidence) * 100:.1f}%"
                        if confidence not in (None, "")
                        else "confidence n/a"
                    )
                    st.info(
                        f"{action} → {target.upper()} ({target_location}) — {reason} ({confidence_label})"
                    )
            else:
                st.caption("No automated policy triggers for this dataset.")

        action_cols = st.columns([1, 1, 1, 1])
        if action_cols[0].button("🔁 Simulate Access", key=f"simulate-{dataset_id}"):
            try:
                response = requests.post(
                    f"{API}/ingest_event",
                    json={"file_id": dataset_id},
                    timeout=5,
                )
                response.raise_for_status()
                st.success("Access event ingested")
                refresh_all_caches()
            except Exception as exc:
                st.error(f"Failed to simulate event: {exc}")

        move_target = action_cols[1].selectbox(
            "Move target", options=["s3", "azure", "gcs"], key=f"target-{dataset_id}"
        )
        if action_cols[2].button("🚚 Trigger Move", key=f"move-{dataset_id}"):
            try:
                response = requests.post(
                    f"{API}/move",
                    json={"file_id": dataset_id, "target": move_target},
                    timeout=8,
                )
                response.raise_for_status()
                st.success("Migration triggered")
                refresh_all_caches()
            except Exception as exc:
                st.error(f"Failed to move file: {exc}")

        if action_cols[3].button("📄 Show raw features", key=f"features-{dataset_id}"):
            st.json(selected_row)


with streaming_tab:
    st.subheader("Streaming Telemetry & Kafka Feed")
    throughput = float(stream_metrics.get("throughput_per_min", 0.0))
    active_devices = int(stream_metrics.get("active_devices", 0))
    total_events = int(stream_metrics.get("total_events", 0))
    status = stream_metrics.get("status", "offline")
    status_label = {
        "stream_api": "Online",
        "api": "Fallback (API proxy)",
        "fallback": "Offline (using cached events)",
    }.get(status, "Offline")

    metrics_cols = st.columns(5)
    metrics_cols[0].metric("Kafka throughput", f"{throughput:.1f} msg/min")
    metrics_cols[1].metric("Active devices", active_devices)
    metrics_cols[2].metric("Stream events", total_events)
    producer_status = "Ready" if stream_metrics.get("producer_ready") else "Offline"
    metrics_cols[3].metric("Producer", producer_status)
    metrics_cols[4].metric("Stream feed", status_label)
    if stream_metrics.get("kafka_bootstrap"):
        st.caption(
            f"Kafka bootstrap: `{stream_metrics.get('kafka_bootstrap')}` · Topic: `{stream_metrics.get('topic', 'access-events')}`"
        )

    if status == "fallback":
        st.info("Stream API offline — displaying cached events from the control-plane metrics endpoint.")
    elif status == "offline":
        st.warning("No streaming data available yet. Trigger the simulator or post events to populate telemetry.")

    events = stream_metrics.get("events", []) if stream_metrics else []
    if events:
        events_df = pd.DataFrame(events)
        if "timestamp" in events_df.columns:
            events_df["datetime"] = pd.to_datetime(events_df["timestamp"], unit="s", errors="coerce")
            events_df = events_df.sort_values("datetime")
            events_df.set_index("datetime", inplace=True)

        chart_cols = st.columns(3)
        if "temperature" in events_df.columns:
            chart_cols[0].line_chart(events_df[["temperature"]])
            chart_cols[0].caption("Sensor temperature stream")
        if "bytes" in events_df.columns:
            chart_cols[1].area_chart(events_df[["bytes"]])
            chart_cols[1].caption("Payload size per event")
        density_series = events_df.assign(events=1)[["events"]].rolling(window=10, min_periods=1).sum()
        chart_cols[2].area_chart(density_series)
        chart_cols[2].caption("Event density (rolling 10)")

        st.markdown("### Live Kafka feed (most recent events)")
        pretty_cols = [col for col in ["event_id", "device_id", "temperature", "bytes"] if col in events_df.columns]
        if pretty_cols:
            st.dataframe(events_df[pretty_cols].tail(50), use_container_width=True)
    else:
        st.info("Kafka producer not sending events yet. Start the streaming stack to populate live metrics.")

    st.markdown("### Metadata-linked feature telemetry")
    if df.empty:
        st.info("Dataset metadata will appear once control-plane events are ingested.")
    else:
        feature_cols = [
            col
            for col in [
                "req_count_last_1min",
                "req_count_last_10min",
                "req_count_last_1hr",
                "avg_latency_1min",
                "p95_latency_5min",
                "ema_req_5min",
                "ema_req_30min",
            ]
            if col in df.columns
        ]
        if feature_cols:
            st.line_chart(df.set_index("id")[feature_cols])

        st.markdown("**Live feature snapshot**")
        feed_cols = [
            "id",
            "req_count_last_1min",
            "avg_latency_1min",
            "high_temp_alerts_last_10min",
            "failed_reads_last_10min",
            "network_failures_last_hour",
        ]
        feed_cols = [c for c in feed_cols if c in df.columns]
        if feed_cols:
            feed_df = df[feed_cols].copy()
            feed_df.rename(
                columns={
                    "req_count_last_1min": "req/min",
                    "avg_latency_1min": "latency ms",
                    "high_temp_alerts_last_10min": "high-temp (10m)",
                    "failed_reads_last_10min": "failed reads",
                    "network_failures_last_hour": "network failures",
                },
                inplace=True,
            )
            st.dataframe(feed_df.sort_values("req/min", ascending=False), use_container_width=True, hide_index=True)


with ml_tab:
    st.subheader("Predictive Recommendations")
    if df.empty:
        st.info("Metadata not available. Train the predictive model once data lands.")
    else:
        policy_rows: List[Dict[str, object]] = []
        for dataset_id in df["id"].tolist():
            try:
                policy_rows.append(fetch_policy(dataset_id))
            except Exception as exc:
                policy_rows.append({"file_id": dataset_id, "recommendation": "error", "error": str(exc)})

        policy_df = pd.DataFrame(policy_rows)
        st.markdown("**Current model outputs**")
        if not policy_df.empty:
            display_cols = ["file_id", "recommendation", "source"]
            if "confidence" in policy_df.columns:
                policy_df["confidence_pct"] = policy_df["confidence"].map(lambda v: f"{float(v)*100:.1f}%" if v is not None else "–")
                display_cols.append("confidence_pct")
            if "model_type" in policy_df.columns:
                display_cols.append("model_type")
            st.dataframe(policy_df[display_cols], use_container_width=True, hide_index=True)

        st.markdown("**Feature intensity (per-tier averages)**")
        feature_cols = [
            "req_count_last_10min",
            "ema_req_30min",
            "bytes_read_last_10min",
            "p95_latency_5min",
            "hour_of_day",
        ]
        feature_df = df[["current_tier", *feature_cols]].copy()
        feature_summary = feature_df.groupby("current_tier").mean(numeric_only=True)
        st.bar_chart(feature_summary.transpose())

        st.markdown("**What-if analyzer**")
        with st.expander("Simulate demand spike"):
            scenario_dataset = st.selectbox("Dataset", options=df["id"].tolist(), key="scenario-dataset")
            scenario_row = df[df["id"] == scenario_dataset].iloc[0]
            base_freq = float(scenario_row.get("access_freq_per_day", 0.0))
            base_latency = float(scenario_row.get("latency_sla_ms", 0.0))
            freq_multiplier = st.slider("Frequency multiplier", min_value=0.1, max_value=5.0, value=1.0, step=0.1)
            latency_delta = st.slider("Latency change (ms)", min_value=-200, max_value=200, value=0, step=10)

            projected_freq = base_freq * freq_multiplier
            projected_latency = max(base_latency + latency_delta, 1.0)

            # Simple heuristic fallback mirroring decide_tier
            if projected_freq >= 100 or projected_latency <= 25:
                projected_tier = "hot"
            elif projected_freq >= 10 or projected_latency <= 120:
                projected_tier = "warm"
            else:
                projected_tier = "cold"

            st.metric("Projected tier", f"{projected_tier.upper()}")
            st.caption("Predictions fall back to heuristic thresholds when the ML model is not trained.")


with migrations_tab:
    st.subheader("Migration Activity & Alerts")
    if df.empty:
        st.info("No datasets to analyse.")
    else:
        migration_df = df[[
            "id",
            "current_location",
            "current_tier",
            "num_recent_migrations",
            "time_since_last_migration",
            "egress_cost_last_1hr",
        ]].copy()
        migration_df.rename(
            columns={
                "num_recent_migrations": "migrations (24h)",
                "time_since_last_migration": "minutes since last move",
                "egress_cost_last_1hr": "egress cost (₹, 1h)",
            },
            inplace=True,
        )
        st.dataframe(migration_df.sort_values("migrations (24h)", ascending=False), use_container_width=True, hide_index=True)

        st.markdown("**Migration trend proxy**")
        trend_df = migration_df[["id", "minutes since last move", "migrations (24h)"]].set_index("id")
        st.area_chart(trend_df)

        policy_rows: List[Dict[str, object]] = []
        for record in files_payload:
            if not isinstance(record, dict):
                continue
            dataset = record.get("id")
            for policy in record.get("policy_triggers") or []:
                if not isinstance(policy, dict):
                    continue
                policy_rows.append(
                    {
                        "dataset": dataset,
                        "action": policy.get("action"),
                        "target_tier": policy.get("target_tier"),
                        "target_location": policy.get("target_location"),
                        "reason": policy.get("reason"),
                        "confidence": policy.get("confidence"),
                    }
                )

        st.markdown("**Automated policy triggers**")
        if policy_rows:
            policy_df = pd.DataFrame(policy_rows)
            if "confidence" in policy_df.columns:
                policy_df["confidence"] = policy_df["confidence"].map(
                    lambda v: f"{float(v) * 100:.1f}%" if v not in (None, "") else "—"
                )
            st.dataframe(policy_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No active automated triggers right now.")

        warning_msgs, info_msgs = summarise_alerts(df)
        st.markdown("**Active alerts**")
        if warning_msgs:
            for msg in warning_msgs:
                st.warning(msg)
        else:
            st.success("No critical alerts at the moment.")

        if info_msgs:
            st.markdown("**Informational signals**")
            for msg in info_msgs:
                st.info(msg)

        st.markdown("**Raw telemetry snapshot**")
        st.json(df.to_dict(orient="records"))


st.caption(
    "Refresh the dashboard after running workloads to keep predictive insights, Kafka metrics, and migration history up to date."
)
