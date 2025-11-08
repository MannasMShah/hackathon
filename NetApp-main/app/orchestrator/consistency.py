import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional

import requests


def with_retry(fn, retries=5, backoff=0.5):
    for i in range(retries):
        try:
            return fn()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(backoff * (2 ** i))


class ConsistencyManager:
    """Coordinate optimistic updates and replica synchronization."""

    def __init__(
        self,
        coll_files,
        coll_events,
        coll_sync,
        feature_defaults: Dict[str, Any],
        replica_endpoints: Optional[Iterable[str]] = None,
    ):
        self.coll_files = coll_files
        self.coll_events = coll_events
        self.coll_sync = coll_sync
        self.feature_defaults = feature_defaults
        self.replicas: List[str] = [
            endpoint.strip()
            for endpoint in (replica_endpoints or [])
            if endpoint and endpoint.strip()
        ]

    # ------------------------------------------------------------------
    # bootstrap helpers
    # ------------------------------------------------------------------
    def ensure_indexes(self) -> None:
        if self.coll_files is None:
            return
        try:
            self.coll_files.create_index("id", unique=True)
        except Exception:
            pass
        if self.coll_sync is not None:
            try:
                self.coll_sync.create_index("file_id", unique=True)
            except Exception:
                pass

    def ensure_document(self, file_id: str) -> Dict[str, Any]:
        doc = self.coll_files.find_one({"id": file_id})
        if doc:
            if "version" not in doc:
                self.coll_files.update_one(
                    {"id": file_id},
                    {
                        "$set": {
                            "version": doc.get("version", 0) or 1,
                            "sync_state": doc.get(
                                "sync_state",
                                {
                                    "status": "in_sync",
                                    "last_synced": None,
                                    "replicas": [],
                                    "last_error": None,
                                },
                            ),
                        }
                    },
                )
                doc = self.coll_files.find_one({"id": file_id})
            return doc

        base_doc = {
            "id": file_id,
            "version": 1,
            "sync_state": {
                "status": "pending",
                "last_synced": None,
                "replicas": [],
                "last_error": None,
            },
        }
        for key, value in self.feature_defaults.items():
            base_doc.setdefault(key, value)
        base_doc.setdefault("current_location", "s3")
        base_doc.setdefault("last_access_ts", datetime.now(timezone.utc).isoformat())
        self.coll_files.insert_one(base_doc)
        return base_doc

    def mark_seed_synced(self, file_id: str) -> None:
        doc = self.ensure_document(file_id)
        state = {
            "status": "seeded" if self.replicas else "local_only",
            "replicas": [],
            "last_attempt": time.time(),
            "last_error": None,
        }
        self._set_sync_state(file_id, state)
        if self.coll_sync is not None:
            self.coll_sync.update_one(
                {"file_id": file_id},
                {
                    "$set": {
                        "file_id": file_id,
                        "status": state["status"],
                        "replicas": [],
                        "version": doc.get("version", 1),
                        "reason": "seed",
                        "last_attempt": state.get("last_attempt"),
                        "last_error": None,
                    }
                },
                upsert=True,
            )

    def reconcile_pending(self) -> Dict[str, Any]:
        if self.coll_sync is None:
            return {"synced": 0, "note": "no sync collection"}
        synced = 0
        attempted = 0
        for entry in self.coll_sync.find({"status": {"$ne": "in_sync"}}):
            attempted += 1
            doc = self.coll_files.find_one({"id": entry["file_id"]})
            if not doc:
                continue
            status = self._propagate(doc, reason="reconcile")
            if status["status"] == "in_sync":
                synced += 1
        return {"attempted": attempted, "synced": synced}

    def status(self) -> Dict[str, Any]:
        if self.coll_sync is None:
            return {"status": "disabled", "replicas": self.replicas}
        pending = list(self.coll_sync.find({}, {"_id": 0}))
        aggregate = {
            "replicas": self.replicas,
            "items": pending,
            "total": len(pending),
            "pending": sum(1 for p in pending if p.get("status") != "in_sync"),
        }
        return aggregate

    # ------------------------------------------------------------------
    # optimistic concurrency
    # ------------------------------------------------------------------
    def safe_update(
        self,
        file_id: str,
        mutate: Callable[[Dict[str, Any]], Dict[str, Dict[str, Any]]],
        reason: str = "update",
        retries: int = 5,
    ) -> Dict[str, Any]:
        last_error: Optional[str] = None
        for attempt in range(retries):
            doc = self.ensure_document(file_id)
            base_version = doc.get("version", 0)
            update_spec = mutate(dict(doc)) or {}
            set_fields: Dict[str, Any] = dict(update_spec.get("set", {}))
            inc_fields: Dict[str, Any] = dict(update_spec.get("inc", {}))

            # ensure sync metadata is tracked for every mutation
            sync_state = set_fields.get("sync_state", doc.get("sync_state", {}))
            if not sync_state:
                sync_state = {
                    "status": "pending",
                    "last_synced": doc.get("sync_state", {}).get("last_synced"),
                    "replicas": doc.get("sync_state", {}).get("replicas", []),
                    "last_error": doc.get("sync_state", {}).get("last_error"),
                }
            else:
                sync_state = {
                    **doc.get("sync_state", {}),
                    **sync_state,
                    "status": "pending",
                }
            set_fields["sync_state"] = sync_state

            next_version = base_version + 1
            set_fields["version"] = next_version

            result = self.coll_files.update_one(
                {"id": file_id, "version": base_version},
                {"$set": set_fields, "$inc": inc_fields},
            )
            if result.modified_count == 1:
                merged = dict(doc)
                merged.update(set_fields)
                for key, value in inc_fields.items():
                    merged[key] = merged.get(key, 0) + value
                merged["version"] = next_version
                self._record_sync(merged, reason=reason)
                return merged

            # conflict detected, log and retry
            last_error = f"conflict on attempt {attempt + 1}"
            self._log_conflict(file_id, reason)
            time.sleep(0.1 * (attempt + 1))

        if last_error:
            raise RuntimeError(last_error)
        raise RuntimeError("conflict without details")

    # ------------------------------------------------------------------
    # network + replica propagation
    # ------------------------------------------------------------------
    def _record_sync(self, doc: Dict[str, Any], reason: str) -> None:
        status = self._propagate(doc, reason=reason)
        if self.coll_sync is None:
            return
        payload = {
            "file_id": doc["id"],
            "status": status["status"],
            "reason": reason,
            "replicas": status.get("replicas", []),
            "last_attempt": status.get("last_attempt"),
            "last_error": status.get("last_error"),
            "version": doc.get("version"),
        }
        self.coll_sync.update_one(
            {"file_id": doc["id"]},
            {"$set": payload},
            upsert=True,
        )

    def _propagate(self, doc: Dict[str, Any], reason: str) -> Dict[str, Any]:
        if not self.replicas:
            state = {
                "status": "local_only",
                "replicas": [],
                "last_attempt": time.time(),
                "last_error": None,
            }
            self._set_sync_state(doc["id"], state)
            return state

        statuses = []
        had_error = False
        last_error: Optional[str] = None
        payload = {"file": doc, "reason": reason, "version": doc.get("version")}
        for endpoint in self.replicas:
            try:
                resp = requests.post(endpoint, json=payload, timeout=2)
                resp.raise_for_status()
                statuses.append(
                    {
                        "endpoint": endpoint,
                        "status": "ok",
                        "ts": time.time(),
                    }
                )
            except Exception as exc:
                had_error = True
                last_error = str(exc)
                statuses.append(
                    {
                        "endpoint": endpoint,
                        "status": "error",
                        "error": str(exc),
                        "ts": time.time(),
                    }
                )
        state = {
            "status": "in_sync" if not had_error else "degraded",
            "replicas": statuses,
            "last_attempt": time.time(),
            "last_error": last_error,
        }
        if had_error:
            self._record_network_issue(doc["id"], last_error)
        self._set_sync_state(doc["id"], state)
        return state

    def _set_sync_state(self, file_id: str, state: Dict[str, Any]) -> None:
        self.coll_files.update_one(
            {"id": file_id},
            {
                "$set": {
                    "sync_state": {
                        "status": state.get("status"),
                        "replicas": state.get("replicas", []),
                        "last_synced": datetime.now(timezone.utc).isoformat(),
                        "last_error": state.get("last_error"),
                    }
                }
            },
        )

    def _record_network_issue(self, file_id: str, error: Optional[str]) -> None:
        if self.coll_events is not None:
            self.coll_events.insert_one(
                {
                    "type": "sync_error",
                    "file_id": file_id,
                    "error": error,
                    "ts": time.time(),
                }
            )
        self.coll_files.update_one(
            {"id": file_id},
            {
                "$inc": {"network_failures_last_hour": 1},
            },
        )

    def record_failure(self, file_id: str, reason: str, error: Optional[str]) -> None:
        if self.coll_events is not None:
            self.coll_events.insert_one(
                {
                    "type": "availability_error",
                    "file_id": file_id,
                    "reason": reason,
                    "error": error,
                    "ts": time.time(),
                }
            )
        self.coll_files.update_one(
            {"id": file_id},
            {
                "$inc": {"network_failures_last_hour": 1},
            },
        )

    def _log_conflict(self, file_id: str, reason: str) -> None:
        if self.coll_events is not None:
            self.coll_events.insert_one(
                {
                    "type": "sync_conflict",
                    "file_id": file_id,
                    "reason": reason,
                    "ts": time.time(),
                }
            )
        self.coll_files.update_one(
            {"id": file_id},
            {
                "$inc": {"sync_conflicts_last_1hr": 1},
            },
        )


def parse_replica_env(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    try:
        if raw.strip().startswith("["):
            return [str(item) for item in json.loads(raw)]
        return [piece.strip() for piece in raw.split(",") if piece.strip()]
    except Exception:
        return []
