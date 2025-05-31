import boto3
import os
from dotenv import load_dotenv
load_dotenv("config/.env")

s3 = boto3.client("s3")
S3_BUCKET = os.getenv("S3_BUCKET")

def upload_file(local_path, s3_key):
    s3.upload_file(local_path, S3_BUCKET, s3_key)

def list_files(prefix):
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    return [item["Key"] for item in resp.get("Contents", [])]
