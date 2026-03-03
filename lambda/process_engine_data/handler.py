"""
AWS Lambda handler for automatic marine engine data processing.

This function is triggered automatically whenever a new CSV file is uploaded
to the S3 bucket under the engine-data/ prefix. It reads the raw sensor
readings, enriches each row with a health score and z-score anomaly flags,
and saves the processed result back to S3 under the processed/ prefix.

Trigger : S3 PutObject event on the engine-data/ prefix
Input   : CSV file with raw engine sensor readings
Output  : Enriched CSV saved to S3 with health scores, status labels,
          and per-sensor z-scores. Detailed logs written to CloudWatch.

Environment variables (configured in the Lambda console, never hardcoded):
    OUTPUT_PREFIX -- S3 folder where processed files are saved
                     (default: "processed")

Health score interpretation:
    90 - 100  OPTIMAL   Normal operation
    75 -  90  GOOD      Minor deviations, monitor closely
    60 -  75  CAUTION   Noticeable deviation, maintenance recommended
    40 -  60  ALERT     Significant fault, immediate inspection required
     0 -  40  CRITICAL  Severe fault, stop engine

Anomaly detection uses z-scores against known operational baselines.
A z-score above 3.0 (3 standard deviations from the mean) is flagged
as a critical anomaly and logged as a WARNING in CloudWatch.
"""
import json
import boto3
import pandas as pd
import logging
import os
from io import StringIO
from datetime import datetime, timezone
from urllib.parse import unquote_plus

# ── Logging ────────────────────────────────────────────────────────────────
# In Lambda, all log output goes automatically to CloudWatch Logs.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── AWS clients ─────────────────────────────────────────────────────────────
# No credentials are passed here. Lambda uses the execution role assigned
# to the function (marine-monitor-lambda-role) for all AWS API calls.
s3 = boto3.client("s3")

# ── Environment variables ───────────────────────────────────────────────────
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "processed")

# ── Engine sensor configuration ─────────────────────────────────────────────
# Optimal operating ranges and scoring weights for each sensor.
# Weights must sum to 1.0. Higher weight = greater impact on health score.
PARAMETERS = {
    "temperature_exhaust": {"optimal": (330, 370), "weight": 0.25},
    "pressure_lube":       {"optimal": (3.8, 4.8), "weight": 0.25},
    "vibration_rms":       {"optimal": (1.5, 3.0), "weight": 0.20},
    "temperature_cooling": {"optimal": (72, 82),   "weight": 0.15},
    "rpm":                 {"optimal": (90, 100),  "weight": 0.10},
    "pressure_fuel":       {"optimal": (8.5, 9.5), "weight": 0.05},
}

# Statistical baselines derived from normal engine operation data.
# Used to compute z-scores for anomaly detection.
BASELINES = {
    "temperature_exhaust": {"mean": 350.0, "std": 17.0},
    "pressure_lube":       {"mean": 4.22,  "std": 0.43},
    "vibration_rms":       {"mean": 2.39,  "std": 0.55},
    "temperature_cooling": {"mean": 77.58, "std": 4.35},
    "rpm":                 {"mean": 95.06, "std": 5.72},
    "pressure_fuel":       {"mean": 8.99,  "std": 0.58},
}


def compute_parameter_score(
    value: float,
    optimal_low: float,
    optimal_high: float
) -> float:
    """
    Compute a 0-100 health score for a single sensor reading.

    Returns 100 if the value is within the optimal range. Outside that range,
    the score decreases linearly based on how far the value has deviated.
    A deviation equal to 50% of the optimal range width reduces the score
    by 50 points. The minimum possible score is 0.

    Args:
        value:        The sensor reading to evaluate.
        optimal_low:  Lower bound of the optimal operating range.
        optimal_high: Upper bound of the optimal operating range.

    Returns:
        A float between 0.0 and 100.0 representing sensor health.
    """
    if optimal_low <= value <= optimal_high:
        return 100.0

    margin = (optimal_high - optimal_low) * 0.5

    if value < optimal_low:
        deviation = (optimal_low - value) / margin
    else:
        deviation = (value - optimal_high) / margin

    return max(0.0, round(100.0 - (deviation * 50), 2))


def compute_health_score(row: pd.Series) -> float:
    """
    Compute the overall weighted health score for a single engine reading.

    Each sensor contributes to the final score according to its configured
    weight. If any sensor scores below 20 (critical failure), an additional
    penalty proportional to that sensor's weight is subtracted from the total.

    Args:
        row: A pandas Series representing one row of engine sensor readings.
             Must contain the column names defined in PARAMETERS.

    Returns:
        A float between 0.0 and 100.0. Higher is healthier.
    """
    weighted_score   = 0.0
    total_weight     = 0.0
    critical_penalty = 0.0

    for param, config in PARAMETERS.items():
        if param not in row.index:
            continue

        score = compute_parameter_score(
            row[param], *config["optimal"]
        )
        weighted_score += score * config["weight"]
        total_weight   += config["weight"]

        if score < 20:
            critical_penalty += config["weight"] * 40

    if total_weight == 0:
        return 0.0

    base_score = weighted_score / total_weight
    return round(max(0.0, base_score - critical_penalty), 2)


def compute_zscore(value: float, param: str) -> float:
    """
    Compute the z-score of a sensor reading against its known baseline.

    A z-score measures how many standard deviations a value is from the
    expected mean during normal engine operation. Values above 3.0 or
    below -3.0 are considered statistical anomalies.

    Args:
        value: The sensor reading to evaluate.
        param: The sensor name (must be a key in BASELINES).

    Returns:
        The z-score rounded to 3 decimal places, or 0.0 if the sensor
        has no baseline or its standard deviation is zero.
    """
    if param not in BASELINES:
        return 0.0
    baseline = BASELINES[param]
    if baseline["std"] == 0:
        return 0.0
    return round((value - baseline["mean"]) / baseline["std"], 3)


def process_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Enrich a raw engine DataFrame with health scores, status labels, and
    z-score anomaly flags.

    Processing steps:
        1. Compute a health score (0-100) for every row using weighted
           sensor scores and critical penalties.
        2. Assign a status label (OPTIMAL / GOOD / CAUTION / ALERT / CRITICAL)
           based on the health score range.
        3. Compute a z-score for each sensor column and flag readings where
           the absolute z-score exceeds 3.0 as anomalies.
        4. Record the UTC processing timestamp on every row.

    Args:
        df: Raw DataFrame loaded from S3, containing one row per engine
            reading and one column per sensor.

    Returns:
        A tuple of:
            - df_processed: The enriched DataFrame with added columns
              (health_score, health_status, <param>_zscore, processed_at).
            - metrics: A summary dictionary with total_rows, avg_health_score,
              min_health_score, a list of detected anomalies, and a count
              of readings per status label.
    """
    df = df.copy()

    # Health score
    df["health_score"] = df.apply(compute_health_score, axis=1)

    # Status label
    df["health_status"] = pd.cut(
        df["health_score"],
        bins=[0, 40, 60, 75, 90, 100],
        labels=["CRITICAL", "ALERT", "CAUTION", "GOOD", "OPTIMAL"],
        right=True,
        include_lowest=True
    )

    # Z-scores and anomaly detection
    anomalies_found = []
    for param in BASELINES.keys():
        if param not in df.columns:
            continue
        df[f"{param}_zscore"] = df[param].apply(
            lambda v: compute_zscore(v, param)
        )
        critical = df[df[f"{param}_zscore"].abs() >= 3.0]
        if len(critical) > 0:
            anomalies_found.append({
                "parameter":  param,
                "count":      len(critical),
                "max_zscore": float(critical[f"{param}_zscore"].abs().max())
            })

    df["processed_at"] = datetime.now(timezone.utc).isoformat()

    metrics = {
        "total_rows":       len(df),
        "avg_health_score": round(float(df["health_score"].mean()), 2),
        "min_health_score": round(float(df["health_score"].min()), 2),
        "anomalies":        anomalies_found,
        "status_counts":    df["health_status"].value_counts().to_dict(),
    }

    return df, metrics


def handler(event, context):
    """
    Lambda entry point — invoked by AWS when an S3 PutObject event fires.

    The event payload contains one or more S3 records, each identifying the
    bucket and object key of the newly uploaded file. For each record the
    function reads the CSV, processes it, and writes the enriched result
    back to S3 under the OUTPUT_PREFIX folder.

    Args:
        event:   AWS event dict containing a "Records" list. Each record
                 includes s3.bucket.name and s3.object.key.
        context: AWS Lambda context object (execution metadata, not used).

    Returns:
        A dict with statusCode 200 and a JSON body listing each file's
        processing outcome (input path, output path, metrics, status).
        Failed files are included with an error message instead of metrics.
    """
    logger.info("=== Marine Engine Lambda Processor ===")
    logger.info(f"Event: {json.dumps(event)}")

    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key    = unquote_plus(record["s3"]["object"]["key"])

        logger.info(f"Processing: s3://{bucket}/{key}")

        try:
            # Read CSV from S3
            response    = s3.get_object(Bucket=bucket, Key=key)
            csv_content = response["Body"].read().decode("utf-8")
            df          = pd.read_csv(StringIO(csv_content))

            logger.info(f"Loaded {len(df)} rows from S3")

            # Process
            df_processed, metrics = process_dataframe(df)

            logger.info(f"Avg health score: {metrics['avg_health_score']}")
            logger.info(f"Anomalies found: {len(metrics['anomalies'])}")

            if metrics["anomalies"]:
                for anomaly in metrics["anomalies"]:
                    logger.warning(
                        f"ANOMALY — {anomaly['parameter']}: "
                        f"{anomaly['count']} readings, "
                        f"max z={anomaly['max_zscore']:.2f}"
                    )

            # Save enriched CSV back to S3
            output_key = key.replace("engine-data", OUTPUT_PREFIX)
            output_csv = df_processed.to_csv(index=False)

            s3.put_object(
                Bucket=bucket,
                Key=output_key,
                Body=output_csv.encode("utf-8"),
                ContentType="text/csv",
                Metadata={
                    "avg_health_score": str(metrics["avg_health_score"]),
                    "processed_at":     datetime.now(timezone.utc).isoformat(),
                    "source":           "marine-monitor-lambda"
                }
            )

            logger.info(f"Saved: s3://{bucket}/{output_key}")

            results.append({
                "input":   key,
                "output":  output_key,
                "metrics": metrics,
                "status":  "success"
            })

        except Exception as e:
            logger.error(f"Error processing {key}: {str(e)}")
            results.append({
                "input":  key,
                "error":  str(e),
                "status": "failed"
            })

    return {
        "statusCode": 200,
        "body": json.dumps(results, default=str)
    }
