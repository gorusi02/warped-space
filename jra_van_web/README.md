# JRA VAN Web

Next.js app that reads race data from BigQuery and renders the race list.

## Prerequisites

- Node.js 18+
- npm (or pnpm/yarn)
- Access to a Google Cloud project with BigQuery tables

## Environment Variables

Create a local `.env.local` from `.env.example` and set:

- `GOOGLE_CLOUD_PROJECT`: target GCP project ID
- `JRA_SERVING_DATASET`: serving layer dataset (default: `jra_serving`)
- `BIGQUERY_LOCATION`: BigQuery job location (default: `asia-northeast1`)
- `GOOGLE_APPLICATION_CREDENTIALS`: absolute path to a service-account key file stored **outside** this repository

If your environment already has Application Default Credentials (ADC), `GOOGLE_APPLICATION_CREDENTIALS` can be omitted.

Required tables in the serving dataset:
- `serving_races`
- `serving_entries`

Required view/table in the analysis dataset:
- `analysis_view`

Optional (recommended) in the analysis dataset:
- `speed_index_master` (for original-style speed index display)

## Security Notes

- Do not store service-account JSON files in this repository.
- If a key was ever committed, revoke it and issue a new key immediately.
- Keep key files in a secure external location and reference them via environment variables only.

## Getting Started

```bash
npm install
npm run dev
```

Open `http://localhost:3000`.

Main routes:
- `/`: レース一覧
- `/racecard/[raceId]`: 出馬表
- `/analysis/[raceId]`: 指数分析

API route:
- `/api/races/[raceId]/analysis`

## Quality Checks

```bash
npm run lint
npm run build
```

## Build Speed Index Table (Recommended)

Run from `jra_van_loader` to refresh the index master used by `/analysis/[raceId]`:

```bash
cd ../jra_van_loader
python build_speed_index.py --project horse-racing-m1 --dataset jra_common --location asia-northeast1
```

Note:
- `jra-web-viewer` should stay read-only for web serving.
- Run `build_speed_index.py` with a separate writer account (for example `python-upload`) that has write permission on `jra_common`.
