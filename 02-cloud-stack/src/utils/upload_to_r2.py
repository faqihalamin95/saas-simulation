"""
upload_to_r2.py
───────────────
Upload Y2 parquet files to Cloudflare R2.

Usage
-----
1. Copy .env.example → .env and fill in credentials
2. pip install boto3 python-dotenv
3. python upload_to_r2.py

Structure in R2
───────────────
y2/subscription_events/event_date=2025-01-01/filename.parquet
y2/payments/event_date=2025-01-01/filename.parquet
y2/product_events/event_date=2025-01-01/filename.parquet
y2/users/event_date=.../filename.parquet
"""

import os
import glob
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

# ── Credentials from .env ────────────────────────────────
ACCOUNT_ID       = os.environ["R2_ACCOUNT_ID"]
ACCESS_KEY_ID    = os.environ["R2_ACCESS_KEY_ID"]
SECRET_ACCESS_KEY= os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET_NAME      = os.environ["R2_BUCKET_NAME"]

# ── Local source ─────────────────────────────────────────
LOCAL_SOURCE = Path("data/raw_y2")   # folder hasil generator Y2
R2_PREFIX    = "y2"                  # prefix di dalam bucket

# ─────────────────────────────────────────────────────────

def get_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def upload_directory(client, local_dir: Path, prefix: str):
    """
    Walk local_dir and upload every .parquet file,
    preserving the folder structure under the given prefix.
    """
    files = list(local_dir.rglob("*.parquet"))

    if not files:
        print(f"No parquet files found in {local_dir}")
        return

    print(f"Found {len(files)} files to upload.\n")
    ok, failed = 0, []

    for local_path in files:
        # e.g. subscription_events/event_date=2025-01-01/file.parquet
        relative  = local_path.relative_to(local_dir)
        r2_key    = f"{prefix}/{relative}"

        try:
            client.upload_file(
                Filename=str(local_path),
                Bucket=BUCKET_NAME,
                Key=r2_key,
            )
            print(f"  ✓  {r2_key}")
            ok += 1
        except Exception as e:
            print(f"  ✗  {r2_key}  →  {e}")
            failed.append(r2_key)

    print(f"\nDone. Uploaded: {ok} | Failed: {len(failed)}")
    if failed:
        print("Failed files:")
        for f in failed:
            print(f"  - {f}")


if __name__ == "__main__":
    if not LOCAL_SOURCE.exists():
        raise FileNotFoundError(
            f"Local source not found: {LOCAL_SOURCE}\n"
            "Make sure you've run runner_y2.py first."
        )

    client = get_client()
    upload_directory(client, LOCAL_SOURCE, R2_PREFIX)