"""Tests for read_parquet_batches — validates Parquet reading matches CSV dtype=str behavior."""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_pipelines_annuaire.helpers.utils import read_parquet_batches

# Columns matching StockUniteLegale schema (subset)
SAMPLE_COLUMNS = [
    "siren",
    "dateCreationUniteLegale",
    "denominationUniteLegale",
    "categorieJuridiqueUniteLegale",
    "trancheEffectifsUniteLegale",
    "etatAdministratifUniteLegale",
]


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample Parquet file with mixed types and nulls."""
    table = pa.table(
        {
            "siren": pa.array(["123456789", "987654321", "111222333", None]),
            "dateCreationUniteLegale": pa.array(
                ["2020-01-15", "2019-06-30", None, "2021-12-01"]
            ),
            "denominationUniteLegale": pa.array(
                ["ACME SAS", None, "FOO SARL", "BAR & CIE"]
            ),
            "categorieJuridiqueUniteLegale": pa.array([5710, 5499, 1000, None]),
            "trancheEffectifsUniteLegale": pa.array(
                ["11", "00", None, "22"], type=pa.string()
            ),
            "etatAdministratifUniteLegale": pa.array(["A", "A", "C", "A"]),
        }
    )
    path = tmp_path / "test_stock.parquet"
    pq.write_table(table, path)
    return path


@pytest.fixture
def equivalent_csv(tmp_path):
    """Create the same data as CSV for comparison."""
    df = pd.DataFrame(
        {
            "siren": ["123456789", "987654321", "111222333", None],
            "dateCreationUniteLegale": ["2020-01-15", "2019-06-30", None, "2021-12-01"],
            "denominationUniteLegale": ["ACME SAS", None, "FOO SARL", "BAR & CIE"],
            "categorieJuridiqueUniteLegale": [5710, 5499, 1000, None],
            "trancheEffectifsUniteLegale": ["11", "00", None, "22"],
            "etatAdministratifUniteLegale": ["A", "A", "C", "A"],
        }
    )
    path = tmp_path / "test_stock.csv"
    df.to_csv(path, index=False)
    return path


def test_all_values_are_strings(sample_parquet):
    """All non-null values must be strings (matches dtype=str behavior)."""
    for df in read_parquet_batches(str(sample_parquet)):
        for col in df.columns:
            non_null = df[col].dropna()
            assert all(isinstance(v, str) for v in non_null), (
                f"Column {col} has non-string values"
            )


def test_nulls_are_preserved(sample_parquet):
    """Null values must remain NaN/None, not become 'None' or 'nan' strings."""
    for df in read_parquet_batches(str(sample_parquet)):
        # siren row 4 is null
        assert pd.isna(df["siren"].iloc[3])
        # dateCreation row 3 is null
        assert pd.isna(df["dateCreationUniteLegale"].iloc[2])
        # denomination row 2 is null
        assert pd.isna(df["denominationUniteLegale"].iloc[1])
        # categorieJuridique row 4 is null
        assert pd.isna(df["categorieJuridiqueUniteLegale"].iloc[3])


def test_integers_become_strings(sample_parquet):
    """Integer columns in Parquet must be cast to strings."""
    for df in read_parquet_batches(str(sample_parquet)):
        assert df["categorieJuridiqueUniteLegale"].iloc[0] == "5710"
        assert df["categorieJuridiqueUniteLegale"].iloc[1] == "5499"


def test_column_selection(sample_parquet):
    """columns parameter filters at read time."""
    cols = ["siren", "etatAdministratifUniteLegale"]
    for df in read_parquet_batches(str(sample_parquet), columns=cols):
        assert list(df.columns) == cols


def test_batch_size_produces_multiple_batches(tmp_path):
    """With small batch_size, large files produce multiple batches."""
    # 100 rows, batch_size=30 -> 4 batches (30+30+30+10)
    table = pa.table(
        {
            "siren": pa.array([f"{i:09d}" for i in range(100)]),
            "nom": pa.array([f"entreprise_{i}" for i in range(100)]),
        }
    )
    path = tmp_path / "big.parquet"
    pq.write_table(table, path)

    batches = list(read_parquet_batches(str(path), batch_size=30))
    assert len(batches) >= 3
    total_rows = sum(len(b) for b in batches)
    assert total_rows == 100


def _normalize_numeric_string(val):
    """Strip trailing '.0' from pandas float-string artifacts (e.g. '5710.0' -> '5710')."""
    if val.endswith(".0"):
        try:
            return str(int(float(val)))
        except ValueError:
            return val
    return val


def test_matches_csv_dtype_str(sample_parquet, equivalent_csv):
    """Parquet output matches pd.read_csv(dtype=str) for non-null values (modulo float artifacts)."""
    parquet_dfs = list(read_parquet_batches(str(sample_parquet)))
    csv_df = pd.read_csv(str(equivalent_csv), dtype=str)

    parquet_df = pd.concat(parquet_dfs, ignore_index=True)

    for col in parquet_df.columns:
        for i in range(len(parquet_df)):
            pval = parquet_df[col].iloc[i]
            cval = csv_df[col].iloc[i]
            if pd.isna(pval) and pd.isna(cval):
                continue
            assert pval == _normalize_numeric_string(cval), (
                f"Mismatch at row {i}, col {col}: parquet={pval!r} vs csv={cval!r}"
            )
