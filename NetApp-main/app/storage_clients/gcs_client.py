import os
from pathlib import Path
from typing import Optional

import requests

GCS_ENDPOINT = os.getenv("GCS_ENDPOINT", "http://fakegcs:4443")
GCS_FALLBACK_DIR = Path(os.getenv("GCS_FALLBACK_DIR", "/data/gcs_fallback"))
GCS_FALLBACK_DIR.mkdir(parents=True, exist_ok=True)


class GCSClient:
    """Hybrid client that prefers the fake-gcs HTTP API but falls back to local disk.

    The hackathon environment does not always run the fake-gcs container, which caused
    migrations to fail with ``FileNotFoundError`` when the control plane attempted to read
    objects from GCS. To keep demos reliable we mirror every write to a local directory and
    transparently serve reads from that cache whenever the HTTP endpoint is unavailable.
    """

    def __init__(self):
        self.base = GCS_ENDPOINT.rstrip("/")
        self.session = requests.Session()

    # ------------------------------------------------------------------
    # helpers
    def _request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        timeout = kwargs.pop("timeout", 5)
        try:
            resp = self.session.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code >= 400:
                return None
            return resp
        except Exception:
            return None

    def _bucket_dir(self, bucket: str) -> Path:
        path = GCS_FALLBACK_DIR / bucket
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _fallback_path(self, bucket: str, key: str) -> Path:
        return self._bucket_dir(bucket) / key

    # ------------------------------------------------------------------
    # public API used by the mover
    def ensure_bucket(self, bucket: str):
        url = f"{self.base}/storage/v1/b/{bucket}"
        payload = {"name": bucket}
        headers = {"Content-Type": "application/json"}
        if not self._request("PUT", url, json=payload, headers=headers):
            # Always provision the local mirror so migrations stay consistent.
            self._bucket_dir(bucket)

    def put_object(self, bucket: str, key: str, data: bytes):
        url = f"{self.base}/upload/storage/v1/b/{bucket}/o"
        params = {"uploadType": "media", "name": key}
        headers = {"Content-Type": "application/octet-stream"}
        self._request("POST", url, params=params, headers=headers, data=data)
        # Mirror to local storage to guarantee reads even when fake-gcs is offline.
        path = self._fallback_path(bucket, key)
        path.write_bytes(data)

    def get_object(self, bucket: str, key: str) -> Optional[bytes]:
        url = f"{self.base}/storage/v1/b/{bucket}/o/{key}"
        params = {"alt": "media"}
        resp = self._request("GET", url, params=params)
        if resp is not None:
            return resp.content
        path = self._fallback_path(bucket, key)
        if path.exists():
            return path.read_bytes()
        return None

    def delete_object(self, bucket: str, key: str):
        url = f"{self.base}/storage/v1/b/{bucket}/o/{key}"
        self._request("DELETE", url)
        path = self._fallback_path(bucket, key)
        if path.exists():
            try:
                path.unlink()
            except FileNotFoundError:
                pass
