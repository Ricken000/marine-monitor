"""
AWS CloudWatch metrics publisher for the Marine Engine Predictive Monitoring System.

Publishes engine sensor readings and health scores as CloudWatch Custom Metrics,
enabling native AWS alarms, dashboards, and anomaly detection directly on
operational engine data.

Namespace : MarineEngine/Monitoring
Dimension : EngineId — supports monitoring multiple independent engines

Metrics published per reading:
    HealthScore         — weighted engine health index (0-100, dimensionless)
    ExhaustTemperature  — exhaust gas temperature (°C, dimensionless)
    LubeOilPressure     — lubricating oil pressure (bar, dimensionless)
    VibrationRMS        — RMS vibration level (mm/s, dimensionless)
    EngineRPM           — shaft speed (RPM, dimensionless)

Summary metrics (published per pipeline session):
    HealthScoreMin      — minimum health score in the batch
    AnomalyCount        — number of fault-injected readings (if column present)
    <Metric>Last        — last observed value for each sensor

Environment variables (loaded from .env, never hardcoded):
    AWS_PROFILE  — named AWS CLI profile used to create the boto3 session
    AWS_REGION   — target AWS region (default: us-east-1)
"""
import boto3
import logging
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _get_cloudwatch_client():
    """
    Create a CloudWatch client using the configured AWS profile.

    Reads AWS_PROFILE and AWS_REGION from the environment. If AWS_PROFILE is
    set, a named boto3 Session is created so the correct credentials are used
    without touching the default profile. Falls back to boto3's standard
    credential chain (environment variables, instance role, etc.) when no
    profile is configured.

    Returns:
        A boto3 CloudWatch client bound to the configured region.
    """
    profile = os.getenv("AWS_PROFILE")
    region  = os.getenv("AWS_REGION", "us-east-1")

    if profile:
        session = boto3.Session(
            profile_name=profile,
            region_name=region
        )
        return session.client("cloudwatch")

    return boto3.client("cloudwatch", region_name=region)


class CloudWatchPublisher:
    """
    Publishes marine engine sensor readings as CloudWatch Custom Metrics.

    Each metric is tagged with an EngineId dimension, which allows a single
    CloudWatch namespace to monitor multiple independent engines without
    metric collisions.

    Usage:
        publisher = CloudWatchPublisher(engine_id="ENGINE-01")
        publisher.publish_reading(row_dict)
        publisher.publish_dataframe_summary(df)
    """

    NAMESPACE = "MarineEngine/Monitoring"

    # Maps DataFrame column names to (CloudWatch metric name, unit).
    # CloudWatch does not have native RPM or pressure units, so "None"
    # (dimensionless) is the correct choice for all engine-specific metrics.
    METRIC_MAP = {
        "health_score":        ("HealthScore",        "None"),
        "temperature_exhaust": ("ExhaustTemperature", "None"),
        "pressure_lube":       ("LubeOilPressure",    "None"),
        "vibration_rms":       ("VibrationRMS",       "None"),
        "rpm":                 ("EngineRPM",           "None"),
    }

    def __init__(self, engine_id: str = "ENGINE-01"):
        self.cw        = _get_cloudwatch_client()
        self.engine_id = engine_id

    def _build_metric(
        self,
        metric_name: str,
        value: float,
        unit: str,
        timestamp: Optional[datetime] = None
    ) -> dict:
        """
        Build a single CloudWatch MetricDatum dictionary.

        Args:
            metric_name: The CloudWatch metric name (e.g. "HealthScore").
            value:       The numeric value to publish.
            unit:        A valid CloudWatch unit string (e.g. "None", "Count").
            timestamp:   UTC datetime for the data point. Defaults to now.

        Returns:
            A dict formatted as a CloudWatch MetricDatum entry.
        """
        return {
            "MetricName": metric_name,
            "Dimensions": [
                {
                    "Name":  "EngineId",
                    "Value": self.engine_id
                }
            ],
            "Value":     float(value),
            "Unit":      unit,
            "Timestamp": timestamp or datetime.now(timezone.utc)
        }

    def publish_single(
        self,
        metric_name: str,
        value: float,
        unit: str = "None"
    ) -> None:
        """
        Publish a single metric data point to CloudWatch.

        Useful for one-off alerts or metrics that do not map directly to a
        DataFrame column (e.g. a custom event counter).

        Args:
            metric_name: The CloudWatch metric name.
            value:       The numeric value to publish.
            unit:        A valid CloudWatch unit string (default: "None").
        """
        self.cw.put_metric_data(
            Namespace=self.NAMESPACE,
            MetricData=[
                self._build_metric(metric_name, value, unit)
            ]
        )
        logger.info(f"Published {metric_name}={value}")

    def publish_reading(
        self,
        row: dict,
        timestamp: Optional[datetime] = None
    ) -> None:
        """
        Publish all sensor metrics from a single engine reading.

        Iterates over METRIC_MAP and publishes every column that is present
        and non-null in the row. Batches the API calls in groups of 20 to
        respect the CloudWatch PutMetricData limit.

        Args:
            row:       A dict (or dict-like object) containing sensor values
                       keyed by DataFrame column name.
            timestamp: UTC datetime for all data points. Defaults to now.
        """
        metrics = []

        for col, (metric_name, unit) in self.METRIC_MAP.items():
            if col in row and row[col] is not None:
                metrics.append(
                    self._build_metric(
                        metric_name,
                        row[col],
                        unit,
                        timestamp
                    )
                )

        if not metrics:
            logger.warning("No metrics to publish")
            return

        # CloudWatch PutMetricData accepts at most 20 MetricDatum entries
        # per call — batch to stay within this hard limit.
        for i in range(0, len(metrics), 20):
            self.cw.put_metric_data(
                Namespace=self.NAMESPACE,
                MetricData=metrics[i:i + 20]
            )

        logger.info(
            f"Published {len(metrics)} metrics for {self.engine_id}"
        )

    def publish_dataframe_summary(
        self,
        df: pd.DataFrame
    ) -> None:
        """
        Publish a statistical summary of a full engine-reading DataFrame.

        Intended to be called once at the end of each pipeline session to
        report aggregate health metrics. Publishes the average and minimum
        health scores, the total anomaly count, and the last observed value
        for every sensor in METRIC_MAP.

        The method batches API calls in groups of 20 (CloudWatch limit) to
        handle any future growth in METRIC_MAP safely.

        Args:
            df: Processed DataFrame produced by the pipeline. Must contain
                a ``health_score`` column; all other columns are optional.
        """
        if "health_score" not in df.columns:
            logger.warning("DataFrame has no health_score column — skipping summary")
            return

        summary_metrics = [
            self._build_metric(
                "HealthScore",
                df["health_score"].mean(),
                "None"
            ),
            self._build_metric(
                "HealthScoreMin",
                df["health_score"].min(),
                "None"
            ),
            self._build_metric(
                "AnomalyCount",
                int(df["fault_injected"].sum())
                if "fault_injected" in df.columns else 0,
                "Count"
            ),
        ]

        # Append last observed value for each sensor (excluding health_score,
        # which is already captured above as the mean and min).
        last_row = df.iloc[-1]
        for col, (metric_name, unit) in self.METRIC_MAP.items():
            if col in last_row and col != "health_score":
                summary_metrics.append(
                    self._build_metric(
                        f"{metric_name}Last",
                        last_row[col],
                        unit
                    )
                )

        # Batch in groups of 20 to respect the CloudWatch PutMetricData limit.
        for i in range(0, len(summary_metrics), 20):
            self.cw.put_metric_data(
                Namespace=self.NAMESPACE,
                MetricData=summary_metrics[i:i + 20]
            )

        logger.info(
            f"Published summary — avg health: "
            f"{df['health_score'].mean():.1f}"
        )