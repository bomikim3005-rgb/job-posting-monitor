"""Extract + load: pull Adzuna postings for every search in searches.yml and save
them as one Parquet file in data/raw/.

Run from the repo root:
    export ADZUNA_APP_ID=... ADZUNA_APP_KEY=...
    python pipeline/ingest.py               # local: data/raw/*.parquet (DuckDB)
    python pipeline/ingest.py --bigquery    # BigQuery: needs BQ_PROJECT and
                                            # GOOGLE_APPLICATION_CREDENTIALS

Each run appends a new snapshot (a new Parquet file, or new rows in the
BigQuery raw table) and never overwrites old ones, so the raw layer keeps
every snapshot. Cleaning and deduplication happen later, in dbt (job_market/).
"""
import argparse
import io
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "pipeline" / "searches.yml"
RAW_DIR = ROOT / "data" / "raw"
API_URL = "https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
PER_PAGE = 50

# BigQuery destination for --bigquery runs. dbt reads this table as its source.
BQ_RAW_DATASET = "job_market_raw"
BQ_RAW_TABLE = "adzuna_postings"
BQ_LOCATION = "US"

# Fixed schema so every file is identical and dbt can read them all with one glob,
# even on a day when e.g. no posting has a salary. Timestamps are UTC.
SCHEMA = pa.schema([
    ("job_id", pa.string()),
    ("title", pa.string()),
    ("company", pa.string()),
    ("location", pa.string()),
    ("area", pa.string()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("category", pa.string()),
    ("contract_type", pa.string()),
    ("contract_time", pa.string()),
    ("salary_min", pa.float64()),
    ("salary_max", pa.float64()),
    ("salary_is_predicted", pa.bool_()),
    ("created_at", pa.timestamp("us")),
    ("redirect_url", pa.string()),
    ("description", pa.string()),
    ("search_name", pa.string()),
    ("search_keywords", pa.string()),
    ("search_location", pa.string()),
    ("ingested_at", pa.timestamp("us")),
])


def fetch(search: dict, country: str, max_pages: int, app_id: str, app_key: str):
    """Yield raw job dicts for one search, newest first."""
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": PER_PAGE,
        "what_phrase": search["keywords"],
        "where": search["location"],
        "sort_by": "date",
        "content-type": "application/json",
    }
    if search.get("distance_km"):
        params["distance"] = search["distance_km"]
    for page in range(1, max_pages + 1):
        resp = requests.get(API_URL.format(country=country, page=page), params=params, timeout=30)
        if resp.status_code != 200:
            # Don't print the URL or exception text: they contain app_key.
            raise RuntimeError(f"page {page}: HTTP {resp.status_code}")
        batch = resp.json().get("results", [])
        yield from batch
        if len(batch) < PER_PAGE:
            return
        time.sleep(1)  # be polite to the free tier


def parse_ts(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def flatten(job: dict, search: dict, ingested_at: datetime) -> dict:
    location = job.get("location") or {}
    predicted = job.get("salary_is_predicted")
    return {
        "job_id": str(job["id"]),
        "title": job.get("title"),
        "company": (job.get("company") or {}).get("display_name"),
        "location": location.get("display_name"),
        "area": " > ".join(location.get("area") or []) or None,
        "latitude": job.get("latitude"),
        "longitude": job.get("longitude"),
        "category": (job.get("category") or {}).get("label"),
        "contract_type": job.get("contract_type"),
        "contract_time": job.get("contract_time"),
        "salary_min": job.get("salary_min"),
        "salary_max": job.get("salary_max"),
        "salary_is_predicted": None if predicted is None else str(predicted) == "1",
        "created_at": parse_ts(job.get("created")),
        "redirect_url": job.get("redirect_url"),
        "description": job.get("description"),
        "search_name": search["name"],
        "search_keywords": search["keywords"],
        "search_location": search["location"],
        "ingested_at": ingested_at,
    }


def main(bigquery_project: str | None = None) -> int:
    app_id, app_key = os.getenv("ADZUNA_APP_ID"), os.getenv("ADZUNA_APP_KEY")
    if not app_id or not app_key:
        print("Set ADZUNA_APP_ID and ADZUNA_APP_KEY first.", file=sys.stderr)
        return 1

    config = yaml.safe_load(CONFIG.read_text())
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    rows, failed = [], []

    for search in config["searches"]:
        # Top-level location / distance_km apply to every search unless it sets its own.
        search = {"location": config.get("location"), "distance_km": config.get("distance_km"), **search}
        try:
            jobs = list(fetch(search, config.get("country", "us"), config.get("max_pages", 3), app_id, app_key))
        except (RuntimeError, requests.RequestException) as e:
            msg = str(e) if isinstance(e, RuntimeError) else type(e).__name__
            print(f"  {search['name']:<20} FAILED ({msg})", file=sys.stderr)
            failed.append(search["name"])
            continue
        rows.extend(flatten(j, search, ingested_at) for j in jobs)
        print(f"  {search['name']:<20} {len(jobs):>4} postings")

    if not rows:
        # Fail so the caller stops here instead of running dbt on no data.
        print("No rows fetched; nothing written. Check location / keywords in "
              "pipeline/searches.yml.", file=sys.stderr)
        return 2

    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    if bigquery_project:
        dest = load_to_bigquery(table, bigquery_project)
    else:
        dest = write_parquet(table, ingested_at)
    print(f"Wrote {len(rows)} rows to {dest}")

    return 1 if failed else 0


def write_parquet(table: pa.Table, ingested_at: datetime) -> str:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    dest = RAW_DIR / f"adzuna_postings_{ingested_at:%Y-%m-%dT%H%M%S}.parquet"
    tmp = dest.with_suffix(".tmp")  # write then rename, so dbt never sees a half-written file
    pq.write_table(table, tmp)
    tmp.rename(dest)
    return str(dest.relative_to(ROOT))


def load_to_bigquery(table: pa.Table, project: str) -> str:
    """Append this run's rows to the raw table with a load job.

    Load jobs are free and work in the BigQuery sandbox (streaming inserts don't).
    Credentials come from GOOGLE_APPLICATION_CREDENTIALS (service account key file).
    """
    from google.cloud import bigquery  # only needed for BigQuery runs

    client = bigquery.Client(project=project)
    dataset = bigquery.Dataset(f"{project}.{BQ_RAW_DATASET}")
    dataset.location = BQ_LOCATION
    client.create_dataset(dataset, exists_ok=True)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    dest = f"{project}.{BQ_RAW_DATASET}.{BQ_RAW_TABLE}"
    job = client.load_table_from_file(
        buf,
        dest,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()  # wait; raises if the load failed
    return dest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--bigquery", action="store_true",
        help="Load into BigQuery (project from $BQ_PROJECT) instead of data/raw/*.parquet.",
    )
    args = parser.parse_args()
    project = None
    if args.bigquery:
        project = os.getenv("BQ_PROJECT")
        if not project:
            sys.exit("--bigquery needs BQ_PROJECT set, e.g. export BQ_PROJECT=my-project-id")
    sys.exit(main(project))
