import os
from pathlib import Path
from storage_clients.s3_client import S3Client
from storage_clients.azure_client import AzureClient
from storage_clients.gcs_client import GCSClient

S3_BUCKET = os.getenv("S3_BUCKET","netapp-bucket")
GCS_BUCKET = os.getenv("GCS_BUCKET","netapp-gcs")

s3 = S3Client()
az = AzureClient()
gcs = GCSClient()

def ensure_buckets():
    # S3 is required for seeds
    try:
        s3.ensure_bucket(S3_BUCKET)
    except Exception:
        pass

    # Best-effort for others; don't crash on failures
    try:
        az.ensure_container("netapp-blob")
    except Exception:
        pass
    try:
        gcs.ensure_bucket(GCS_BUCKET)
    except Exception:
        pass


def put_seed_objects(seed_dir: str):
    p = Path(seed_dir)
    for fp in p.glob("*.txt"):
        key = fp.name
        body = fp.read_bytes()
        s3.put_object(S3_BUCKET, key, body)

def move_object(obj_key: str, src: str, dst: str):
    """
    Copy between emulated clouds; delete source after copy (simulate move).
    Locations: "s3", "azure", "gcs"
    """
    data = None
    if src == "s3":
        data = s3.get_object(S3_BUCKET, obj_key)
    elif src == "azure":
        data = az.get_blob("netapp-blob", obj_key)
    elif src == "gcs":
        data = gcs.get_object(GCS_BUCKET, obj_key)
    else:
        raise ValueError("unknown src")

    if data is None:
        raise FileNotFoundError(f"{obj_key} not found in {src}")

    if dst == "s3":
        s3.put_object(S3_BUCKET, obj_key, data)
    elif dst == "azure":
        az.put_blob("netapp-blob", obj_key, data)
    elif dst == "gcs":
        gcs.put_object(GCS_BUCKET, obj_key, data)
    else:
        raise ValueError("unknown dst")

    # delete source
    if src == "s3":
        s3.delete_object(S3_BUCKET, obj_key)
    elif src == "azure":
        az.delete_blob("netapp-blob", obj_key)
    elif src == "gcs":
        gcs.delete_object(GCS_BUCKET, obj_key)
