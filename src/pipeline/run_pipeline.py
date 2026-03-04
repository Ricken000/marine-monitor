"""
run_pipeline.py — End-to-end pipeline for the Marine Engine Monitoring System.

Executes the full processing chain from raw simulation to scored output:

    [1] SIMULATE  → Generates synthetic engine sensor readings that mimic
                    a real engine watch period (one reading per minute).
    [2] LOAD      → Reads the generated CSV and prepares it for analysis.
    [3] DETECT    → Compares each reading to the normal baseline and flags
                    statistically anomalous values (z-score threshold).
    [4] SCORE     → Computes a health score (0–100) for every reading and
                    classifies engine state into five severity levels.

Output files written to disk:
    data/raw/engine_data_<timestamp>.csv       — raw sensor readings
    data/processed/processed_engine_data_*.csv — enriched readings with scores

Usage (from the marine-monitor/ directory):
    python -m src.pipeline.run_pipeline                          # 24 h, 3% faults
    python -m src.pipeline.run_pipeline --hours 48               # 48 h of data
    python -m src.pipeline.run_pipeline --hours 24 --fault-prob 0.05  # 5% faults

Optional CloudWatch integration (set in .env):
    USE_CLOUDWATCH=true  — publishes a summary of metrics after each run

Internal dependencies:
    src.simulator.engine_simulator  — data generation
    src.analysis.data_loader        — data loading and preparation
    src.analysis.anomaly_detector   — statistical anomaly detection
    src.analysis.health_score       — engine health index
"""
import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from src.simulator.engine_simulator import MarineEngineSimulator
from src.analysis.data_loader import load_engine_data, get_summary
from src.analysis.anomaly_detector import StatisticalAnomalyDetector
from src.analysis.health_score import EngineHealthScorer
from src.aws.cloudwatch_publisher import CloudWatchPublisher

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


def run(
    hours: int        = 24,
    fault_prob: float = 0.03,
    output_dir: str   = "data/raw/"
) -> dict:
    """
    Execute all four pipeline steps and return a summary of key metrics.

    CloudWatch publishing is controlled by the USE_CLOUDWATCH environment
    variable (set to "true" in .env to enable). It is evaluated at call time
    so that changes to the environment after import are respected.

    Args:
        hours:      Hours of engine operation to simulate. At one reading per
                    minute this produces hours × 60 rows (default: 24).
        fault_prob: Fraction of readings that will have an injected fault
                    (default: 0.03 = 3%). Higher values stress-test the
                    anomaly detector and produce more alert-level events.
        output_dir: Directory where the raw CSV is saved. Created automatically
                    if it does not exist (default: "data/raw/").

    Returns:
        A dict with the following keys:
            filename            — name of the raw CSV file generated
            total_readings      — total number of rows processed
            faults_injected     — number of faults injected by the simulator
            critical_anomalies  — critical anomalies detected (z-score ≥ 3)
            avg_health_score    — average health score for the period (0–100)
            min_health_score    — worst health score observed (0–100)
            status_distribution — full breakdown of readings by status level
            processed_path      — path to the enriched CSV with scores
    """
    use_cloudwatch = os.getenv("USE_CLOUDWATCH", "false").lower() == "true"

    logger.info("=" * 50)
    logger.info("MARINE ENGINE MONITORING PIPELINE")
    logger.info("=" * 50)

    # ── STEP 1: Simulate data ──────────────────────────────
    logger.info(f"[1/4] Simulating {hours}h of engine data...")

    sim = MarineEngineSimulator(seed=42)
    df_raw = sim.generate_dataset(
        hours=hours,
        interval_seconds=60,
        fault_probability=fault_prob
    )

    timestamp   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename    = f"engine_data_{timestamp}.csv"
    output_path = Path(output_dir) / filename
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    df_raw.to_csv(output_path, index=False)

    logger.info(f"    Readings generated : {len(df_raw)}")
    logger.info(f"    Faults injected    : {df_raw['fault_injected'].sum()}")
    logger.info(f"    File saved         : {output_path}")

    # ── STEP 2: Load and prepare ───────────────────────────
    logger.info("[2/4] Loading and preparing data...")

    df = load_engine_data(str(output_path))
    summary = get_summary(df)

    logger.info(f"    Period   : {summary['start_time'][:19]} "
                f"→ {summary['end_time'][:19]}")
    logger.info(f"    Duration : {summary['duration_hours']}h")

    # ── STEP 3: Detect anomalies ───────────────────────────
    logger.info("[3/4] Detecting anomalies...")

    # Use the first 6 h (up to 360 rows) as the normal baseline.
    # Falls back to the first 25% of the dataset for shorter simulations.
    baseline_size = min(360, len(df) // 4)
    baseline_df   = df.iloc[:baseline_size]

    detector = StatisticalAnomalyDetector(
        warning_threshold=2.0,
        critical_threshold=3.0
    )
    detector.fit(baseline_df)

    df_analyzed, anomalies = detector.detect(df)

    critical_anomalies = [a for a in anomalies if a.severity == "critical"]
    warning_anomalies  = [a for a in anomalies if a.severity == "warning"]

    logger.info(f"    Critical anomalies : {len(critical_anomalies)}")
    logger.info(f"    Warnings           : {len(warning_anomalies)}")

    # ── STEP 4: Health score ───────────────────────────────
    logger.info("[4/4] Computing health scores...")

    scorer    = EngineHealthScorer()
    df_final  = scorer.add_health_score(df_analyzed)
    hs_summary = scorer.get_status_summary(df_final)

    # ── CLOUDWATCH (optional) ─────────────────────────────
    if use_cloudwatch:
        logger.info("Publishing metrics to CloudWatch...")
        publisher = CloudWatchPublisher(engine_id="ENGINE-01")
        publisher.publish_dataframe_summary(df_final)
        logger.info("Metrics published")

    logger.info(f"    Average score : {hs_summary['avg_score']}")
    logger.info(f"    Minimum score : {hs_summary['min_score']}")

    # ── FINAL REPORT ───────────────────────────────────────
    logger.info("=" * 50)
    logger.info("FINAL RESULTS")
    logger.info("=" * 50)

    for status in ["OPTIMAL", "GOOD", "CAUTION", "ALERT", "CRITICAL"]:
        data = hs_summary[status]
        if data["count"] > 0:
            logger.info(
                f"    {status:<10} {data['count']:>5} readings "
                f"({data['percent']:>5.1f}%)"
            )

    # Save processed CSV
    processed_dir  = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    processed_path = processed_dir / f"processed_{filename}"
    df_final.to_csv(processed_path, index=False)

    logger.info(f"\n    Processed CSV: {processed_path}")
    logger.info("=" * 50)

    # ── RETURN METRICS ─────────────────────────────────────
    return {
        "filename":         filename,
        "total_readings":   len(df_final),
        "faults_injected":  int(df_raw["fault_injected"].sum()),
        "critical_anomalies": len(critical_anomalies),
        "avg_health_score": hs_summary["avg_score"],
        "min_health_score": hs_summary["min_score"],
        "status_distribution": hs_summary,
        "processed_path":   str(processed_path),
    }


# ── ENTRY POINT ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Marine Engine Monitoring Pipeline"
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Hours of data to simulate (default: 24)"
    )
    parser.add_argument(
        "--fault-prob",
        type=float,
        default=0.03,
        help="Fault injection probability per reading (default: 0.03)"
    )
    args = parser.parse_args()

    results = run(
        hours=args.hours,
        fault_prob=args.fault_prob
    )

    print("\nPipeline metrics:")
    for key, value in results.items():
        if key != "status_distribution":
            print(f"  {key}: {value}")