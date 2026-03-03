"""
Integration test for the marine-engine-processor Lambda function.

Instead of hardcoding bucket names and file paths in the AWS console,
this script builds the S3 trigger event dynamically from environment
variables and invokes the Lambda function programmatically via the CLI.

What this script does:
    1. Reads bucket name and S3 prefix from .env — no hardcoded values.
    2. Finds the most recently uploaded CSV under the engine-data/ prefix in S3.
    3. Constructs a realistic S3 event payload that mimics what AWS would send
       when a new file arrives in the bucket.
    4. Invokes the Lambda function synchronously and prints the response.
    5. Reports whether each file was processed successfully or failed.

How to run (from the project root):
    python notebooks/test_lambda.py

Prerequisites:
    - The Lambda function must already be deployed:
          aws lambda create-function ... (see AWS_SERVICES.md)
    - At least one CSV must exist in S3 under the engine-data/ prefix.
      Upload one by running: python notebooks/test_s3.py
    - .env must be configured with S3_BUCKET_NAME, AWS_PROFILE, and AWS_REGION.
"""
import sys
import json
import boto3
from pathlib import Path
from dotenv import load_dotenv
import os

sys.path.insert(0, str(Path(__file__).parent.parent))
load_dotenv(Path(__file__).parent.parent / ".env")

BUCKET          = os.environ["S3_BUCKET_NAME"]
PREFIX          = os.environ.get("S3_PREFIX", "engine-data")
REGION          = os.environ.get("AWS_REGION", "us-east-1")
PROFILE         = os.environ.get("AWS_PROFILE")
FUNCTION_NAME   = "marine-engine-processor"

session = boto3.Session(profile_name=PROFILE, region_name=REGION)
s3      = session.client("s3")
lam     = session.client("lambda")

# ── Find the most recent CSV in S3 ─────────────────────────────────────────
print(f"Looking for CSVs in s3://{BUCKET}/{PREFIX}/...")
response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

if "Contents" not in response:
    print("No files found. Run notebooks/test_s3.py first to upload a CSV.")
    sys.exit(1)

csvs = [o for o in response["Contents"] if o["Key"].endswith(".csv")]
if not csvs:
    print("No CSV files found under the engine-data/ prefix.")
    sys.exit(1)

latest_key = max(csvs, key=lambda o: o["LastModified"])["Key"]
print(f"Using: {latest_key}\n")

# ── Build S3 trigger event ──────────────────────────────────────────────────
event = {
    "Records": [{
        "s3": {
            "bucket": {"name": BUCKET},
            "object": {"key": latest_key}
        }
    }]
}

print("Invoking Lambda...")
print(f"  Function : {FUNCTION_NAME}")
print(f"  Bucket   : {BUCKET}")
print(f"  Key      : {latest_key}\n")

# ── Invoke Lambda synchronously ─────────────────────────────────────────────
result = lam.invoke(
    FunctionName=FUNCTION_NAME,
    InvocationType="RequestResponse",
    Payload=json.dumps(event).encode()
)

status  = result["StatusCode"]
payload = json.loads(result["Payload"].read())
body    = json.loads(payload.get("body", "[]"))

print(f"HTTP status : {status}")
print(f"Results     : {len(body)} file(s) processed\n")

for item in body:
    if item["status"] == "success":
        metrics = item["metrics"]
        print(f"  SUCCESS")
        print(f"    Input  : {item['input']}")
        print(f"    Output : {item['output']}")
        print(f"    Rows   : {metrics['total_rows']}")
        print(f"    Avg health score : {metrics['avg_health_score']}")
        print(f"    Min health score : {metrics['min_health_score']}")
        if metrics["anomalies"]:
            print(f"    Anomalies detected:")
            for a in metrics["anomalies"]:
                print(f"      - {a['parameter']}: {a['count']} readings, max z={a['max_zscore']}")
        else:
            print(f"    No anomalies detected")
    else:
        print(f"  FAILED")
        print(f"    Input : {item['input']}")
        print(f"    Error : {item['error']}")
