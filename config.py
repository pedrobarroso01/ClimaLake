import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent

load_dotenv(PROJECT_ROOT / ".env")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_PROFILE = os.getenv("AWS_PROFILE", "climalake")
S3_BUCKET = os.getenv("S3_BUCKET")

# Pasta local onde os objetos baixados do S3 são guardados (ignorada pelo Git).
DATA_DIR = Path(os.getenv("CLIMALAKE_DATA_DIR", PROJECT_ROOT / "data"))


def s3_client():
    return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION).client("s3")
