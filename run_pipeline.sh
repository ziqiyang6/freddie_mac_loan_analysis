#!/usr/bin/env bash
# Run the Spark pipeline end to end: raw ZIPs -> Parquet -> risk marts.
# Expects the Freddie Mac yearly ZIPs in data/raw_zip/ (see README).
# Every step skips vintages that already have a _SUCCESS marker, so reruns are cheap.
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

echo "==> [00] Unzip yearly/quarterly ZIPs -> data/raw_txt"
bash scripts/00_unzip_raw_data.sh

echo "==> [01] ETL origination TXT -> data/processed/orig"
python scripts/01_etl_orig.py

echo "==> [02] ETL performance TXT -> data/processed/perf"
python scripts/02_etl_perf.py

echo "==> [03] Build 12M/24M delinquency targets"
python scripts/03_build_targets.py

echo "==> [04] Join origination + targets -> risk_analysis"
python scripts/04_build_risk_dataset.py

echo "==> [05] Build portfolio_monthly / delinquency_transition / loan_lifecycle"
python scripts/05_build_perf_marts.py

echo "==> Done. Marts are in data/processed/. To load them into PostgreSQL, see README."
