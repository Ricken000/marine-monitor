"""
AWS S3 upload module for the Marine Engine Predictive Monitoring System.

This module handles all communication with Amazon S3. It reads connection
settings (bucket name, region, AWS profile) from the .env file so that no
credentials are ever written directly in the code.

Uploaded files are organized in S3 using date-based partitioning:

    s3://bucket/engine-data/year=YYYY/month=MM/day=DD/filename.csv

This layout is compatible with AWS Athena and Glue, which means the data
can be queried with standard SQL directly on S3 without loading it into
a database first.

Main class:
    S3DataUploader -- uploads DataFrames as CSV files and HTML reports to S3,
                      and lists recently uploaded objects.

Prerequisites:
    - A .env file at the project root with S3_BUCKET_NAME, AWS_PROFILE, and
      AWS_REGION set (see .env.example for the full template).
    - The AWS CLI profile referenced in .env must be configured with valid
      credentials (run: aws configure --profile <name>).
"""
import boto3
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde .env
load_dotenv()

logger = logging.getLogger(__name__)


def _get_s3_client():
    """
    Create and return a boto3 S3 client.

    Reads AWS_PROFILE and AWS_REGION from environment variables (loaded from
    .env). If AWS_PROFILE is set, a named session is created so that the
    correct IAM credentials are used. If the variable is absent, boto3 falls
    back to the default AWS CLI credentials on the machine.
    """
    profile = os.getenv("AWS_PROFILE")
    region  = os.getenv("AWS_REGION", "us-east-1")

    if profile:
        session = boto3.Session(
            profile_name=profile,
            region_name=region
        )
        return session.client("s3")

    return boto3.client("s3", region_name=region)


class S3DataUploader:
    """
    Uploads engine monitoring data and reports to an AWS S3 bucket.

    Files are stored using a date-partitioned folder structure:

        s3://bucket/engine-data/year=YYYY/month=MM/day=DD/filename.csv

    This layout lets AWS Athena and Glue discover and query the files
    automatically without any additional configuration.

    Configuration is read from environment variables (via .env):
        S3_BUCKET_NAME -- name of the target S3 bucket (required)
        S3_PREFIX      -- top-level folder inside the bucket (default: "engine-data")
        AWS_PROFILE    -- named AWS CLI profile to use for authentication
        AWS_REGION     -- AWS region where the bucket lives (default: "us-east-1")
    """

    def __init__(self):
        self.s3         = _get_s3_client()
        self.bucket     = os.getenv("S3_BUCKET_NAME")
        self.prefix     = os.getenv("S3_PREFIX", "engine-data")

        if not self.bucket:
            raise ValueError(
                "S3_BUCKET_NAME no está configurado. "
                "Verifica tu archivo .env"
            )

    def _build_s3_key(self, filename: str) -> str:
        """Build the full S3 object key with today's date partition."""
        now = datetime.utcnow()
        return (
            f"{self.prefix}/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"{filename}"
        )

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        filename: str,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Upload a pandas DataFrame to S3 as a CSV file.

        The DataFrame is serialized to CSV in memory (no temporary file is
        written to disk) and stored under the date-partitioned prefix.

        Args:
            df:       DataFrame containing the engine readings to upload.
            filename: Name to use for the object in S3 (e.g. "readings.csv").
            metadata: Optional dictionary of string key-value pairs attached
                      to the S3 object as custom metadata (e.g. average health
                      score, data source identifier).

        Returns:
            The full S3 URI of the uploaded object, e.g.
            "s3://<bucket>/engine-data/year=2026/month=03/day=02/readings.csv".
        """
        csv_buffer = df.to_csv(index=True).encode("utf-8")
        key        = self._build_s3_key(filename)

        extra_args: dict[str, Any] = {"ContentType": "text/csv"}
        if metadata:
            extra_args["Metadata"] = {
                k: str(v) for k, v in metadata.items()
            }

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=csv_buffer,
            **extra_args
        )

        s3_uri = f"s3://{self.bucket}/{key}"
        logger.info(f"Subido: {s3_uri}")
        return s3_uri

    def upload_report(self, filepath: str) -> str:
        """
        Upload an HTML report file to S3.

        The report is stored under a "reports/" sub-folder inside the
        date-partitioned prefix, keeping it separate from raw data files.

        Args:
            filepath: Absolute or relative path to the local HTML file.

        Returns:
            The full S3 URI of the uploaded report.
        """
        filename = Path(filepath).name
        key      = self._build_s3_key(f"reports/{filename}")

        with open(filepath, "rb") as f:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=f,
                ContentType="text/html"
            )

        s3_uri = f"s3://{self.bucket}/{key}"
        logger.info(f"Reporte subido: {s3_uri}")
        return s3_uri

    def list_recent_uploads(self, days: int = 7) -> list:
        """
        List objects currently stored in the S3 bucket under the configured prefix.

        Args:
            days: Unused parameter kept for API compatibility. The listing
                  currently returns all objects under the prefix regardless
                  of upload date.

        Returns:
            A list of dictionaries, one per object, each containing:
                - "key"           -- full S3 object key (path inside the bucket)
                - "size_kb"       -- file size in kilobytes, rounded to one decimal
                - "last_modified" -- upload timestamp in "YYYY-MM-DD HH:MM:SS" format
            Returns an empty list if no objects are found.
        """
        response = self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix=self.prefix
        )

        if "Contents" not in response:
            return []

        return [
            {
                "key":           obj["Key"],
                "size_kb":       round(obj["Size"] / 1024, 1),
                "last_modified": str(obj["LastModified"])[:19]
            }
            for obj in response["Contents"]
        ]