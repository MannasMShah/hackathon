import os, io
import boto3
from botocore.client import Config

S3_ENDPOINT = os.getenv("S3_ENDPOINT","http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY","minio")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY","minio12345")

class S3Client:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )

    def ensure_bucket(self, bucket: str):
        existing = [b["Name"] for b in self.s3.list_buckets().get("Buckets",[])]
        if bucket not in existing:
            try:
                self.s3.create_bucket(Bucket=bucket)
            except Exception:
                pass

    def put_object(self, bucket: str, key: str, data: bytes):
        self.s3.put_object(Bucket=bucket, Key=key, Body=data)

    def get_object(self, bucket: str, key: str) -> bytes | None:
        try:
            resp = self.s3.get_object(Bucket=bucket, Key=key)
            return resp["Body"].read()
        except Exception:
            return None

    def delete_object(self, bucket: str, key: str):
        self.s3.delete_object(Bucket=bucket, Key=key)
