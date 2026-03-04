# AWS Services — Marine Engine Predictive Monitoring System

This file tracks every AWS resource created for this project.
Use it to **tear down** services when no longer needed, and as a
**setup guide** when deploying the project in a new account.

---

## Active Services

### IAM — Identity & Access Management

| Resource | Name | Purpose |
|---|---|---|
| User | `marine-monitor-dev` | Programmatic access for local development and CLI deployments |
| Inline policy | `marine-monitor-lambda-deploy` | Grants the dev user permission to create and deploy Lambda functions and IAM roles scoped to this project |
| Role | `marine-monitor-lambda-role` | Execution role assumed by the Lambda function at runtime |
| Attached policy | `AWSLambdaBasicExecutionRole` | Allows Lambda to write logs to CloudWatch |
| Attached policy | `AmazonS3FullAccess` | Allows Lambda to read raw data and write processed results to S3 |

### S3 — Simple Storage Service

| Resource | Name | Purpose |
|---|---|---|
| Bucket | `marine-monitor-rc-2026` | Stores raw engine CSV uploads, processed results, and HTML reports |

Configuration: versioning enabled, SSE-S3 encryption, all public access blocked.

Folder structure inside the bucket:
```
marine-monitor-rc-2026/
├── engine-data/year=YYYY/month=MM/day=DD/   ← raw readings uploaded by the pipeline
├── processed/year=YYYY/month=MM/day=DD/     ← enriched CSVs produced by Lambda
└── reports/year=YYYY/month=MM/day=DD/       ← HTML monitoring reports
```

### Lambda

| Resource | Name | Purpose |
|---|---|---|
| Function | `marine-engine-processor` | Triggered automatically when a new CSV lands in S3. Computes health scores, z-score anomaly flags, and saves the enriched file back to S3 |

Runtime: Python 3.11 — Timeout: 60 s — Memory: 256 MB

### SNS — Simple Notification Service

| Resource | Name | Purpose |
|---|---|---|
| Topic | `marine-engine-alerts` | Receives CloudWatch alarm notifications and fans them out to all subscribers |
| Subscription | `<notification-email>` | Email endpoint that receives an alert whenever a CloudWatch alarm fires |

ARN: `arn:aws:sns:<region>:<account-id>:marine-engine-alerts`

### CloudWatch — Alarms

| Resource | Name | Condition | Action |
|---|---|---|---|
| Alarm | `MarineEngine-LowHealthScore` | `HealthScore` (avg, 5 min) < 60 | Publishes to `marine-engine-alerts` SNS topic |

Namespace: `MarineEngine/Monitoring` — Dimension: `EngineId=ENGINE-01`

---

## Teardown Checklist

Run these steps in order to delete all project resources:

- [ ] Delete CloudWatch alarm `MarineEngine-LowHealthScore`
- [ ] Delete SNS subscription (email) and topic `marine-engine-alerts`
- [ ] Delete Lambda function `marine-engine-processor`
- [ ] Detach policies from role `marine-monitor-lambda-role`, then delete the role
- [ ] Delete all objects inside `marine-monitor-rc-2026`, then delete the bucket
- [ ] Delete inline policy `marine-monitor-lambda-deploy` from user `marine-monitor-dev`
- [ ] Delete IAM user `marine-monitor-dev`

---

## Setup Checklist (new account)

- [ ] Create IAM user with programmatic access and configure AWS CLI profile
- [ ] Create S3 bucket with versioning, SSE-S3 encryption, and public access blocked
- [ ] Create IAM role `marine-monitor-lambda-role` with trust policy for `lambda.amazonaws.com`
- [ ] Attach `AWSLambdaBasicExecutionRole` and `AmazonS3FullAccess` to the role
- [ ] Package and deploy the Lambda function from `lambda/process_engine_data/`
- [ ] Configure the S3 trigger to invoke the Lambda on `s3:ObjectCreated:*` events under the `engine-data/` prefix
- [ ] Create SNS topic `marine-engine-alerts` and subscribe the notification email
- [ ] Create CloudWatch alarm `MarineEngine-LowHealthScore` pointing to the SNS topic ARN
- [ ] Update `.env` with the new bucket name and AWS profile
