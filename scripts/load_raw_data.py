"""
Extract/load step for the C-MAPSS pipeline.

Parses a raw C-MAPSS text file (space-delimited, no header) and loads it
into raw.sensor_readings in the warehouse Postgres database.

Usage:
    python load_raw_data.py --file data/raw/train_FD001.txt --dataset-id FD001

This is deliberately written as a standalone, parameterized script (not
notebook code) so Airflow can call it as a task, and so it doubles as
something you could point at a new file drop in a real pipeline.
"""

import argparse
import os
import sys

import pandas as pd
from sqlalchemy import create_engine

COLUMN_NAMES = (
    ["unit_number", "time_in_cycles", "op_setting_1", "op_setting_2", "op_setting_3"]
    + [f"sensor_{i}" for i in range(1, 22)]
)


def get_engine():
    """Build a SQLAlchemy engine from environment variables.

    Keeping credentials out of code is a small thing, but it's the
    difference between a project that looks like a tutorial and one that
    looks like it was built the way a real pipeline would be.
    """
    user = os.environ.get("CMAPSS_DB_USER", "cmapss")
    password = os.environ.get("CMAPSS_DB_PASSWORD", "cmapss_pw")
    host = os.environ.get("CMAPSS_DB_HOST", "localhost")
    port = os.environ.get("CMAPSS_DB_PORT", "5432")
    dbname = os.environ.get("CMAPSS_DB_NAME", "cmapss")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def load_file(filepath: str, dataset_id: str) -> int:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"No such file: {filepath}")

    # C-MAPSS files are whitespace-delimited with a trailing separator,
    # which produces two extra all-NaN columns if you don't handle it.
    df = pd.read_csv(filepath, sep=r"\s+", header=None)
    df = df.iloc[:, : len(COLUMN_NAMES)]  # drop any trailing empty columns
    df.columns = COLUMN_NAMES
    df["dataset_id"] = dataset_id

    engine = get_engine()
    df.to_sql(
        "sensor_readings",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Load a raw C-MAPSS file into the warehouse.")
    parser.add_argument("--file", required=True, help="Path to the raw C-MAPSS text file")
    parser.add_argument(
        "--dataset-id",
        required=True,
        help="Sub-dataset identifier, e.g. FD001, FD002, FD003, FD004",
    )
    args = parser.parse_args()

    n_rows = load_file(args.file, args.dataset_id)
    print(f"Loaded {n_rows} rows from {args.file} into raw.sensor_readings (dataset_id={args.dataset_id})")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # surfaces cleanly as a failed Airflow task
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
