"""Integration test: download real Parquet from data.gouv.fr and validate.

Usage:
    python -m tests.test_parquet_integration [--file stock_ul|hist_ul|hist_etab] [--limit N]

Downloads the actual INSEE Parquet file to /tmp, reads it with read_parquet_batches,
and validates schema + data integrity. No Airflow required.
"""

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

logging.basicConfig(level=logging.INFO, format="%(message)s")


def read_parquet_batches(filepath, batch_size=100000, columns=None):
    """Standalone copy of helpers.utils.read_parquet_batches (no Airflow deps)."""
    pf = pq.ParquetFile(filepath)
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        string_arrays = [col.cast(pa.string()) for col in batch.columns]
        string_batch = pa.RecordBatch.from_arrays(
            string_arrays, names=batch.schema.names
        )
        yield string_batch.to_pandas()


PARQUET_SOURCES = {
    "stock_ul": {
        "url": "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockUniteLegale_utf8.parquet",
        "expected_columns": [
            "siren",
            "dateCreationUniteLegale",
            "denominationUniteLegale",
            "categorieJuridiqueUniteLegale",
            "etatAdministratifUniteLegale",
            "statutDiffusionUniteLegale",
        ],
        "min_rows": 25_000_000,
    },
    "hist_ul": {
        "url": "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockUniteLegaleHistorique_utf8.parquet",
        "expected_columns": [
            "siren",
            "dateFin",
            "dateDebut",
            "etatAdministratifUniteLegale",
            "changementEtatAdministratifUniteLegale",
            "nicSiegeUniteLegale",
        ],
        "min_rows": 30_000_000,
    },
    "hist_etab": {
        "url": "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissementHistorique_utf8.parquet",
        "expected_columns": [
            "siren",
            "siret",
            "dateFin",
            "dateDebut",
            "etatAdministratifEtablissement",
            "changementEtatAdministratifEtablissement",
        ],
        "min_rows": 40_000_000,
    },
}


def download_if_missing(url: str, dest: Path) -> Path:
    if dest.exists():
        logging.info(f"Using cached file: {dest} ({dest.stat().st_size / 1e6:.0f} MB)")
        return dest

    logging.info(f"Downloading {url}...")
    t0 = time.time()
    r = requests.get(url, stream=True)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    elapsed = time.time() - t0
    size_mb = dest.stat().st_size / 1e6
    logging.info(f"Downloaded {size_mb:.0f} MB in {elapsed:.0f}s")
    return dest


def validate(file_key: str, limit: int | None):
    source = PARQUET_SOURCES[file_key]
    dest = Path(f"/tmp/test_{file_key}.parquet")

    # 1. Download
    download_if_missing(source["url"], dest)

    # 2. Check schema with PyArrow metadata (no data loaded)
    pf = pq.ParquetFile(dest)
    schema = pf.schema_arrow
    col_names = schema.names
    logging.info(f"\nSchema: {len(col_names)} columns, {pf.metadata.num_rows} rows")
    logging.info(f"Columns: {col_names[:10]}{'...' if len(col_names) > 10 else ''}")

    for expected_col in source["expected_columns"]:
        assert expected_col in col_names, f"Missing expected column: {expected_col}"
    logging.info("All expected columns present.")

    assert pf.metadata.num_rows >= source["min_rows"], (
        f"Expected >= {source['min_rows']} rows, got {pf.metadata.num_rows}"
    )
    logging.info(f"Row count OK: {pf.metadata.num_rows:,}")

    # 3. Read with read_parquet_batches and validate
    total_rows = 0
    batch_count = 0
    t0 = time.time()

    for df in read_parquet_batches(str(dest), batch_size=100_000):
        batch_count += 1
        total_rows += len(df)

        # Validate all values are strings or null
        for col in df.columns:
            assert pd.api.types.is_string_dtype(df[col]), (
                f"Batch {batch_count}, col {col}: dtype={df[col].dtype}, expected string"
            )

        if batch_count == 1:
            logging.info(f"\nFirst batch sample (5 rows):")
            logging.info(df.head().to_string())

        if limit and total_rows >= limit:
            logging.info(f"\nStopped after {limit} rows (--limit)")
            break

    elapsed = time.time() - t0
    logging.info(
        f"\nRead {total_rows:,} rows in {batch_count} batches ({elapsed:.1f}s)"
    )
    logging.info(f"Speed: {total_rows / elapsed:,.0f} rows/s")
    logging.info("PASS")


def main():
    parser = argparse.ArgumentParser(description="Test Parquet stock SIRENE")
    parser.add_argument(
        "--file",
        choices=list(PARQUET_SOURCES.keys()),
        default="hist_ul",
        help="Which file to test (default: hist_ul, smallest)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500_000,
        help="Max rows to read (default: 500000, 0=all)",
    )
    args = parser.parse_args()

    validate(args.file, args.limit or None)


if __name__ == "__main__":
    main()
