# PySpark Data Engineering Project

## Components
- Mechanism X: Pulls 10,000 records every second from a CSV and uploads to S3.
- Mechanism Y: Reads chunks from S3 and detects 3 business patterns.
- Outputs are batched and saved back to S3.

## Tech Stack
- PySpark + Databricks
- PostgreSQL (for offsets)
- AWS S3 (for chunk + result storage)

## Setup
1. Fill `.env` with AWS and DB credentials.
2. Run `init.sql` in PostgreSQL to set offset state.
3. Schedule both mechanisms in Databricks or a Python scheduler.

