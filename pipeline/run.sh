#!/usr/bin/env bash
# Daily pipeline: pull new postings, then rebuild and test the dbt models.
# Run from anywhere:  ./pipeline/run.sh
set -euo pipefail
cd "$(dirname "$0")/.."
python pipeline/ingest.py
cd job_market && dbt build
