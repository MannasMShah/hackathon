import os, requests

GCS_ENDPOINT = os.getenv("GCS_ENDPOINT","http://fakegcs:4443")

class GCSClient:
    def __init__(self):
        self.base = GCS_ENDPOINT

    def ensure_bucket(self, bucket: str):
    # fake-gcs-server creates bucket on PUT
        try:
            url = f"{self.base}/storage/v1/b/{bucket}"
            headers = {"Content-Type": "application/json"}
            data = {"name": bucket}
            requests.put(url, json=data, headers=headers, timeout=5)
        except Exception:
            pass

    def put_object(self, bucket: str, key: str, data: bytes):
        url = f"{self.base}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={key}"
        try:
            requests.post(
                url,
                headers={"Content-Type": "application/octet-stream"},
                data=data,
                timeout=5,
            )
        except Exception:
            pass

    def get_object(self, bucket: str, key: str) -> bytes | None:
        url = f"{self.base}/storage/v1/b/{bucket}/o/{key}?alt=media"
        r = requests.get(url)
        return r.content if r.status_code == 200 else None

    def delete_object(self, bucket: str, key: str):
        url = f"{self.base}/storage/v1/b/{bucket}/o/{key}"
        requests.delete(url)
