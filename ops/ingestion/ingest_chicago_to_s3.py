"""
Ingest Chicago crime data into S3 as raw CSV.

This script:
- Downloads ALL historical crime data for Chicago from the open data JSON endpoint.
- Paginates over the API using $limit and $offset.
- Converts the JSON objects to a flat CSV (one row per record).
- Writes the CSV to S3 under a partitioned key:
  chicago/year=YYYY/month=MM/chicago_crime_YYYYMMDD.csv

We do NOT rename or drop columns here. This is our "raw" layer, only changing
format (JSON -> CSV) to make downstream loading easier.
"""

import argparse
import csv
import datetime as dt
from io import StringIO, BytesIO
from typing import List, Dict
import datetime as dt

import boto3
import requests
import os
from dotenv import load_dotenv

load_dotenv()
# Chicago crime JSON endpoint (full history)
CHICAGO_CRIME_URL = os.getenv("CHICAGO_API")

# S3 config (ajusta el bucket si es diferente)
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

# pagination
PAGE_SIZE = 50000  # max por página (Socrata suele permitir 50k)

def get_month_window(run_date: dt.date) -> tuple[str, str]:
    """
    Given a run_date, return ISO strings for:
      - start of previous month
      - start of current month

    Example:
      run_date = 2025-12-18
      -> ('2025-11-01T00:00:00.000', '2025-12-01T00:00:00.000')
    """
    # first day of current month
    current_month_start = run_date.replace(day=1)

    # first day of previous month
    if current_month_start.month == 1:
        prev_month_start = current_month_start.replace(year=current_month_start.year - 1, month=12)
    else:
        prev_month_start = current_month_start.replace(month=current_month_start.month - 1)

    # Format as SoQL-friendly ISO timestamps
    prev_iso = prev_month_start.strftime("%Y-%m-%dT%H:%M:%S.000")
    current_iso = current_month_start.strftime("%Y-%m-%dT%H:%M:%S.000")

    return prev_iso, current_iso

def fetch_chicago_page(offset: int, start_iso: str, end_iso: str) -> list[dict]:
    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$order": "date",
        "$where": f"date >= '{start_iso}' AND date < '{end_iso}'",
    }

    resp = requests.get(str(CHICAGO_CRIME_URL), params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("Unexpected JSON structure, expected a list of records.")
    return data


def download_chicago_month_window(start_iso: str, end_iso: str) -> list[dict]:
    all_records: list[dict] = []
    offset = 0

    while True:
        print(f"Fetching page with offset={offset}, window {start_iso} → {end_iso}")
        page = fetch_chicago_page(offset, start_iso, end_iso)
        if not page:
            break

        all_records.extend(page)
        print(f"Fetched {len(page)} records, total so far: {len(all_records)}")

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    print(f"Total records downloaded for window: {len(all_records):,}")
    return all_records


def build_s3_key(run_date: dt.date) -> str:
    """
    S3 key:
      chicago/chicago_crime_YYYYMMDD.csv
    """
    # first day of current month
    current_month_start = run_date.replace(day=1)

    # first day of previous month
    if current_month_start.month == 1:
        prev_month_start = current_month_start.replace(year=current_month_start.year - 1, month=12)
    else:
        prev_month_start = current_month_start.replace(month=current_month_start.month - 1)

    year = prev_month_start.year
    month = f"{prev_month_start.month:02d}"
    date_str = prev_month_start.strftime("%Y%m%d")

    key = f"chicago/chicago_crime_{date_str}.csv"
    return key


def records_to_csv_bytes(records: List[Dict]) -> bytes:
    """
    Convert a list of dict records to CSV bytes.

    - Column names are inferred from the union of keys across all records.
    - Missing fields in a given row are left empty.
    """
    if not records:
        raise ValueError("No records to write to CSV.")

    # Infer columnas dinámicamente usando el primer registro,
    # y opcionalmente unimos con más claves si queremos ser extra cuidadosos.
    fieldnames = set()
    for rec in records:
        fieldnames.update(rec.keys())
    fieldnames = sorted(fieldnames)  # orden estable

    print(f"Detected {len(fieldnames)} columns in JSON data.")

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()

    for rec in records:
        writer.writerow(rec)

    csv_text = output.getvalue()
    output.close()
    return csv_text.encode("utf-8")


def upload_to_s3(data: bytes, key: str) -> None:
    """
    Upload the CSV bytes to S3 using the given key.
    """
    s3 = boto3.client("s3")
    file_obj = BytesIO(data)

    s3.upload_fileobj(file_obj, S3_BUCKET, key)
    print(f"Uploaded {len(data):,} bytes to s3://{S3_BUCKET}/{key}")


def parse_args() -> argparse.Namespace:
    """
    Allow overriding the run date for partitioning:
      --run-date 2024-01-01
    If not provided, defaults to today.
    """
    parser = argparse.ArgumentParser(description="Ingest Chicago crime data into S3 as raw CSV.")
    parser.add_argument(
        "--run-date",
        type=str,
        help="Run date in YYYY-MM-DD format (defaults to today).",
    )
    return parser.parse_args()


def get_run_date(arg_date: str | None) -> dt.date:
    """
    Parse the run date argument or use today's date.
    """
    if arg_date:
        return dt.datetime.strptime(arg_date, "%Y-%m-%d").date()
    return dt.date.today()


def main() -> None:
    args = parse_args()
    run_date = get_run_date(args.run_date)

    # Window = previous month
    start_iso, end_iso = get_month_window(run_date)
    print(f"Downloading Chicago crime data for window: {start_iso} → {end_iso}")

    records = download_chicago_month_window(start_iso, end_iso)

    if not records:
        print("No records for this month window; exiting.")
        return

    csv_bytes = records_to_csv_bytes(records)

    # usamos run_date para particionar por "mes actual - 1"
    key = build_s3_key(run_date)
    upload_to_s3(csv_bytes, key)