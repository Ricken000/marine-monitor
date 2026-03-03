"""
End-to-end S3 upload test for the Marine Engine Predictive Monitoring System.

This script verifies that the AWS S3 integration works correctly by performing
a real upload to the configured bucket. It is intended to be run manually after
the main pipeline has produced at least one processed CSV file.

What this script does:
    1. Connects to S3 using the credentials and bucket defined in .env.
    2. Looks for the most recently modified CSV file in data/processed/.
    3. Loads that file into a DataFrame and uploads it to S3 with metadata
       (average health score and data source label).
    4. Lists all objects currently stored in the bucket and prints their
       key, size in KB, and upload timestamp.

Expected output:
    - A confirmation line with the S3 URI of the uploaded file.
    - A table of all files currently in the bucket.

How to run (from the project root):
    python notebooks/test_s3.py

Prerequisites:
    - Run the main pipeline first to generate a processed CSV:
          python -m src.pipeline.run_pipeline
    - .env must be configured with S3_BUCKET_NAME, AWS_PROFILE, and AWS_REGION.
    - The IAM user referenced by AWS_PROFILE must have s3:PutObject and
      s3:ListBucket permissions on the target bucket.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aws.s3_uploader import S3DataUploader
from src.analysis.data_loader import load_engine_data

# Verificar conexión
print("Conectando a S3...")
uploader = S3DataUploader()
print(f"Bucket: {uploader.bucket}")

# Subir el CSV procesado más reciente
processed_files = list((Path(__file__).parent.parent / "data" / "processed").glob("*.csv"))
if not processed_files:
    print("No hay archivos procesados. Ejecuta el pipeline primero.")
else:
    latest = max(processed_files, key=lambda f: f.stat().st_mtime)
    print(f"Subiendo: {latest.name}")

    df = load_engine_data(str(latest))
    s3_uri = uploader.upload_dataframe(
        df,
        latest.name,
        metadata={
            "avg_health_score": str(round(df.describe()["health_score"]["mean"], 2))
            if "health_score" in df.columns else "N/A",
            "source": "marine-monitor-pipeline"
        }
    )
    print(f"Uploaded successfully: {s3_uri}")

    # Listar uploads recientes
    print("\nArchivos en S3:")
    for obj in uploader.list_recent_uploads():
        print(f"  {obj['key']}")
        print(f"    Tamaño: {obj['size_kb']} KB")
        print(f"    Fecha:  {obj['last_modified']}")