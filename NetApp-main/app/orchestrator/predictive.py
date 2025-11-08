"""Predictive tier recommendation using a lightweight gradient boosting model.

This module keeps the implementation intentionally simple so it can run entirely
on a developer laptop without a GPU or massive dependencies.  It wraps
scikit-learn's ``HistGradientBoostingClassifier`` which is fast on CPU and
handles heterogeneous tabular data well.
"""

from __future__ import annotations

import pickle
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier


MODEL_PATH = Path("model_store/tier_predictor.pkl")


@dataclass
class TierPredictor:
    """Wrapper around a gradient boosted tree classifier.

    The classifier predicts the storage tier (``hot``, ``warm`` or ``cold``)
    from a handful of metadata-derived features.  The goal is to make the model
    easy to retrain locally as new telemetry is collected.
    """

    feature_names: Sequence[str] = field(
        default_factory=lambda: (
            "access_freq_per_day",
            "latency_sla_ms",
            "size_kb",
            "days_since_access",
            "req_count_last_1min",
            "req_count_last_10min",
            "req_count_last_1hr",
            "bytes_read_last_10min",
            "bytes_written_last_10min",
            "unique_clients_last_30min",
            "avg_latency_1min",
            "p95_latency_5min",
            "max_latency_10min",
            "hour_of_day",
            "day_of_week",
            "ema_req_5min",
            "ema_req_30min",
            "growth_rate_10min",
            "delta_latency_5min",
            "events_per_minute",
            "high_temp_alerts_last_10min",
            "current_tier",
            "num_recent_migrations",
            "time_since_last_migration",
            "storage_cost_per_gb",
            "egress_cost_last_1hr",
            "cloud_region",
            "sync_conflicts_last_1hr",
            "failed_reads_last_10min",
            "network_failures_last_hour",
        )
    )
    model: HistGradientBoostingClassifier | None = None
    label_name: str = "target_tier"

    def load(self, path: Path = MODEL_PATH) -> bool:
        """Load a previously trained model from ``path`` if present."""
        if not path.exists():
            return False
        with path.open("rb") as fh:
            payload = pickle.load(fh)
        if not isinstance(payload, dict) or "model" not in payload:
            return False
        self.model = payload["model"]
        self.feature_names = tuple(payload.get("feature_names", self.feature_names))
        return True

    def save(self, path: Path = MODEL_PATH) -> None:
        """Persist the trained model and metadata to disk."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as fh:
            pickle.dump({"model": self.model, "feature_names": tuple(self.feature_names)}, fh)

    @property
    def ready(self) -> bool:
        return self.model is not None

    def train(self, records: Iterable[Dict[str, object]]) -> Dict[str, object]:
        """Train the classifier from an iterable of records.

        Each record must contain the columns listed in ``feature_names`` as well
        as ``target_tier``.  Returns a dictionary with metrics that can be
        surfaced back to the API client.
        """

        df = pd.DataFrame(list(records))
        if df.empty:
            raise ValueError("no training data supplied")

        missing_cols = [c for c in (*self.feature_names, self.label_name) if c not in df.columns]
        if missing_cols:
            raise ValueError(f"training data missing columns: {missing_cols}")

        # Drop rows with missing labels or features.
        df = df.dropna(subset=[*self.feature_names, self.label_name])
        if df.empty:
            raise ValueError("training data became empty after dropping NA values")

        X = df.loc[:, self.feature_names].astype(float)
        y = df[self.label_name].astype(str)

        clf = HistGradientBoostingClassifier(
            max_depth=3,
            learning_rate=0.1,
            max_iter=200,
            min_samples_leaf=5,
        )
        clf.fit(X, y)
        self.model = clf
        self.save()

        # Simple training accuracy metric for visibility.
        preds = clf.predict(X)
        accuracy = float(np.mean(preds == y))
        return {
            "trained": True,
            "samples": int(len(df)),
            "classes": sorted({*y}),
            "training_accuracy": round(accuracy, 4),
        }

    # ------------------------------------------------------------------
    # Feature helpers
    # ------------------------------------------------------------------
    def build_features(self, file_doc: Dict[str, object]) -> Dict[str, float]:
        """Extract the feature vector for a document from MongoDB."""
        features: Dict[str, float] = {}
        for name in self.feature_names:
            if name == "days_since_access":
                raw = file_doc.get("last_access_ts")
            else:
                raw = file_doc.get(name)
            features[name] = self.normalize_feature(name, raw)
        return features

    def predict(self, features: Dict[str, float]) -> str:
        """Predict the tier for the supplied feature dict."""
        if not self.ready:
            raise RuntimeError("predictor has not been trained")
        ordered = [[features.get(name, 0.0) for name in self.feature_names]]
        pred = self.model.predict(ordered)
        return str(pred[0])

    @staticmethod
    def _days_since_access(last_access_ts: object) -> float:
        if not last_access_ts:
            return 365.0
        if isinstance(last_access_ts, (int, float)):
            dt = datetime.fromtimestamp(float(last_access_ts), tz=timezone.utc)
        elif isinstance(last_access_ts, str):
            ts = last_access_ts.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(ts)
            except ValueError:
                return 365.0
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        elif isinstance(last_access_ts, datetime):
            dt = last_access_ts.astimezone(timezone.utc)
        else:
            return 365.0
        now = datetime.now(timezone.utc)
        delta = now - dt
        return max(delta.total_seconds() / 86400.0, 0.0)

    def normalize_feature(self, name: str, value: object) -> float:
        if name == "days_since_access":
            return float(self._days_since_access(value))
        if name == "current_tier":
            return float(self._tier_to_numeric(value))
        if name == "cloud_region":
            return float(self._region_to_numeric(value))
        if value is None:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _tier_to_numeric(tier: object) -> float:
        if isinstance(tier, (int, float)):
            return float(tier)
        if isinstance(tier, str):
            tier_lower = tier.lower()
            if tier_lower == "cold":
                return 0.0
            if tier_lower == "warm":
                return 1.0
            if tier_lower == "hot":
                return 2.0
        return -1.0

    @staticmethod
    def _region_to_numeric(region: object) -> float:
        if isinstance(region, (int, float)):
            return float(region)
        if isinstance(region, str) and region:
            return float(zlib.crc32(region.strip().lower().encode("utf-8")) % 1000)
        return 0.0


def auto_label_records(files: Iterable[Dict[str, object]], labeler) -> List[Dict[str, object]]:
    """Generate training rows from file documents using ``labeler`` for targets."""
    predictor = TierPredictor()
    rows: List[Dict[str, object]] = []
    for doc in files:
        features = predictor.build_features(doc)
        label = labeler(doc)
        rows.append({**features, predictor.label_name: label, "file_id": doc.get("id")})
    return rows
