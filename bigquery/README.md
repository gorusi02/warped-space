# BigQuery Operational SQL

This directory contains SQL for layered BigQuery operations.

## Files

- `01_create_datasets.sql`: creates `jra_raw`, `jra_core`, `jra_serving`, `jra_ml`
- `02_build_serving_tables.sql`: builds canonical tables and serving tables
- `03_quality_checks.sql`: duplicate/null/freshness monitoring queries

## Run

Use the bootstrap script:

```bash
cd warped-space/jra_van_loader
python bootstrap_bigquery.py --project horse-racing-m1 --location asia-northeast1
```

The script replaces `${PROJECT_ID}` placeholders and executes SQL files in filename order.

To run only serving refresh SQL:

```bash
cd warped-space/jra_van_loader
python bootstrap_bigquery.py --project horse-racing-m1 --location asia-northeast1 --only "02_build_serving_tables.sql,03_quality_checks.sql"
```
