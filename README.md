# Marine Engine Predictive Monitoring System `v1.0`

A production-grade, end-to-end pipeline that ingests marine engine telemetry,
detects anomalies through statistical analysis, computes a weighted engine health
index, and delivers real-time alerts through AWS CloudWatch and SNS — with a
Streamlit dashboard for interactive exploration.

Built to demonstrate the integration of data engineering, cloud infrastructure,
and domain knowledge from commercial maritime operations.

---

## Overview

Diesel engines on commercial vessels generate continuous sensor data across
dozens of parameters. Interpreting that data manually — reading individual gauges
during a watch — is slow and error-prone. This system automates the detection of
abnormal operating conditions and reduces the information overload to a single
health index (0–100) with an actionable severity label.

The project covers the full data lifecycle:

1. **Simulation** — Synthetic telemetry calibrated to MAN B&W engine specifications
2. **Detection** — Statistical anomaly detection using z-score analysis
3. **Scoring** — Weighted composite health index per engine reading
4. **Storage** — Raw and processed data persisted in AWS S3 (date-partitioned)
5. **Alerting** — CloudWatch alarms + SNS email notifications
6. **Visualization** — Interactive Streamlit dashboard

---

## Architecture

```
┌──────────────────────────── Local Pipeline ─────────────────────────────────┐
│                                                                               │
│   MarineEngineSimulator                                                       │
│         │  synthetic telemetry, 1 reading/min                                │
│         ▼                                                                     │
│     Raw CSV ──────────────────────────────────► S3  engine-data/             │
│         │                                              │                     │
│         ▼                                              │  S3 PutObject       │
│   StatisticalAnomalyDetector                           ▼                     │
│         │  z-score vs baseline                Lambda  handler.py             │
│         ▼                                       │  health score + z-scores   │
│   EngineHealthScorer                            ▼                            │
│         │  weighted 0-100 index           S3  processed/                     │
│         ▼                                                                     │
│   CloudWatchPublisher ──────────────────► CloudWatch  MarineEngine/Monitoring│
│                                                  │                           │
│                                          Alarm   HealthScore < 60            │
│                                                  │                           │
│                                           SNS    marine-engine-alerts        │
│                                                  │                           │
│                                            Email notification                │
└───────────────────────────────────────────────────────────────────────────────┘

┌──────────────────── Streamlit Dashboard ───────────────────┐
│  Health Score Timeline  │  Engine Parameter Charts (6)     │
│  Status Distribution    │  Recent Alerts Table             │
│  Simulation Controls    │  CSV Export                      │
└────────────────────────────────────────────────────────────┘
```

---

## Features

- **Synthetic telemetry** calibrated to MAN B&W series L/K engine specs — RPM,
  exhaust temperature, cooling temperature, lube pressure, fuel pressure, vibration RMS
- **Statistical anomaly detection** with configurable warning (z ≥ 2.0) and
  critical (z ≥ 3.0) thresholds against a static normal baseline
- **Weighted health score** (0–100) with five severity levels and a critical
  penalty mechanism for extreme sensor deviations
- **AWS S3 data lake** with Hive-style date partitioning, Athena-compatible
- **AWS Lambda** serverless processor triggered automatically on each S3 upload
- **CloudWatch Custom Metrics** in namespace `MarineEngine/Monitoring` with
  a `HealthScore < 60` alarm
- **SNS email alerts** delivered within seconds of an alarm state change
- **Streamlit dashboard** with synchronized slider + numeric input controls,
  calibrated y-axis ranges per sensor, and a formatted alert table
- **23 unit tests** covering all core modules

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Data processing | pandas, NumPy |
| Visualization | Plotly, Streamlit |
| Cloud | AWS S3, Lambda, CloudWatch, SNS, IAM |
| AWS SDK | boto3, python-dotenv |
| Testing | pytest |
| Lambda runtime | Python 3.11 |

---

## Project Structure

```
marine-monitor/
├── src/
│   ├── simulator/
│   │   └── engine_simulator.py      # Synthetic telemetry generator
│   ├── analysis/
│   │   ├── data_loader.py           # CSV ingestion and DatetimeIndex setup
│   │   ├── anomaly_detector.py      # Z-score anomaly detection
│   │   └── health_score.py          # Weighted health index
│   ├── aws/
│   │   ├── s3_uploader.py           # S3 upload with date partitioning
│   │   └── cloudwatch_publisher.py  # CloudWatch Custom Metrics publisher
│   └── pipeline/
│       └── run_pipeline.py          # End-to-end pipeline entry point
├── lambda/
│   └── build/
│       └── handler.py               # Lambda function (serverless processor)
├── dashboard/
│   └── app.py                       # Streamlit interactive dashboard
├── tests/
│   ├── test_health_score.py         # Unit tests — EngineHealthScorer
│   └── test_simulator.py            # Unit tests — MarineEngineSimulator
├── data/
│   ├── raw/                         # Raw CSV files (git-ignored)
│   └── processed/                   # Enriched CSV files (git-ignored)
├── AWS_SERVICES.md                  # Infrastructure registry and teardown guide
├── .env.example                     # Environment variable template
└── requirements.txt
```

---

## Quick Start

### Prerequisites

- Python 3.11+
- AWS CLI configured with a named profile (see AWS Setup below)

### Installation

```bash
git clone https://github.com/Ricken000/marine-monitor.git
cd marine-monitor
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Environment configuration

```bash
cp .env.example .env
```

Edit `.env` with your values:

```env
AWS_PROFILE=your-profile-name
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
S3_PREFIX=engine-data
USE_CLOUDWATCH=false             # set to true to publish metrics to CloudWatch
DEFAULT_HOURS=24
DEFAULT_FAULT_PROBABILITY=0.03
```

### Run the pipeline

```bash
# 24 hours of data, 3% fault rate (defaults)
python -m src.pipeline.run_pipeline

# Custom duration and fault rate
python -m src.pipeline.run_pipeline --hours 48 --fault-prob 0.05
```

### Launch the dashboard

```bash
streamlit run dashboard/app.py
```

### Run the tests

```bash
pytest tests/ -v
```

---

## AWS Infrastructure

All AWS resources are documented in [AWS_SERVICES.md](AWS_SERVICES.md), including
ARNs (anonymised), a teardown checklist, and a setup guide for deploying to a new
account.

### Services used

| Service | Resource | Purpose |
|---|---|---|
| S3 | `<bucket>` | Raw and processed data lake |
| Lambda | `marine-engine-processor` | Serverless CSV enrichment on upload |
| CloudWatch | `MarineEngine/Monitoring` | Custom metrics namespace and alarms |
| SNS | `marine-engine-alerts` | Email notification fanout |
| IAM | `marine-monitor-lambda-role` | Lambda execution role |

### Lambda deployment

The Lambda function is packaged as a zip with its dependencies pre-built in
`lambda/build/`. Runtime: Python 3.11, timeout: 60 s, memory: 256 MB. It is
triggered automatically by S3 PutObject events under the `engine-data/` prefix.

---

## Technical Decisions

### Z-score anomaly detection

Z-score was chosen over more complex approaches (Isolation Forest, LSTM autoencoders)
for two reasons:

1. **Interpretability** — A z-score of 3.2 on `pressure_lube` has a direct physical
   meaning: lube pressure is 3.2 standard deviations below its normal mean. A marine
   engineer can immediately relate to that number without statistical training.

2. **No labeled training data required** — The baseline is derived from the first
   6 hours of each run (assumed normal operation). This mirrors how a watchkeeper
   establishes a reference: observe normal behavior first, then flag deviations.

**Known limitation:** The static baseline does not adapt to gradual engine aging.
A production system would use an adaptive rolling baseline with a sliding window.

### Weighted health score

A simple average of the six sensor scores would give equal importance to every
parameter. In a marine diesel, that is physically incorrect:

| Sensor | Weight | Rationale |
|---|---|---|
| `temperature_exhaust` | 0.25 | Primary indicator of combustion quality |
| `pressure_lube` | 0.25 | Loss of lube pressure destroys the engine within minutes |
| `vibration_rms` | 0.20 | Early indicator of bearing failure or propeller imbalance |
| `temperature_cooling` | 0.15 | Prevents thermal deformation |
| `rpm` | 0.10 | Shaft speed indicator |
| `pressure_fuel` | 0.05 | Fuel injection feed pressure |

A **critical penalty** is applied when any sensor scores below 20 (extreme
deviation). This ensures the composite score drops sharply rather than being
averaged out by healthy sensors — reflecting that a single severe fault is
sufficient to require immediate action regardless of the overall average.

### Date-partitioned S3 layout

Files are stored as:

```
s3://bucket/engine-data/year=YYYY/month=MM/day=DD/filename.csv
```

This Hive-style layout is natively understood by AWS Athena and Glue, enabling
SQL queries directly on S3 with no ETL pipeline:

```sql
SELECT AVG(health_score)
FROM engine_readings
WHERE year = '2026' AND month = '03';
```

### Lambda over a persistent service

The Lambda function processes each file independently and only runs when triggered.
For a system receiving one batch per watch period (every 4–8 hours), an always-on
service would be idle the majority of the time. Lambda eliminates that idle cost
with no trade-off in latency for this use case.

### Sensor reference values

All normal operating ranges and baseline statistics are calibrated from MAN B&W
series L/K engine maintenance documentation. The sensor weights and score
thresholds reflect the priority hierarchy used in professional engine room practice.

---

## Testing

```
tests/test_health_score.py   14 tests
tests/test_simulator.py       9 tests
─────────────────────────────────────
Total                        23 tests — all passing
```

Coverage includes parameter score bounds, boundary conditions, DataFrame
immutability guarantees, fault injection correctness, seed-based reproducibility,
and dataset length and fault rate accuracy.

---

## Author

Ricardo Caviedes — Merchant Marine Engineer / AWS Solutions Architect Associate / Data Analyst

> This project was developed with [Claude Code](https://claude.com/claude-code)
> (Anthropic) as an AI pair-programming assistant, used throughout for code review,
> architectural decisions, and iterative refinement.
