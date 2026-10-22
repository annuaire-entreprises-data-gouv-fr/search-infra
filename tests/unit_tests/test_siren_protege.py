from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.processor import (
    SirenProtegeProcessor,
)


def test_clean_siren_list():
    df = pd.DataFrame(
        {
            "siren": ["356000000", " 552 100 554 ", "", "356000000", "12345"],
            "other": ["a", "b", "c", "d", "e"],
        }
    )

    result = SirenProtegeProcessor.clean_siren_list(df)

    assert list(result.columns) == ["siren"]
    assert list(result["siren"]) == ["12345", "356000000", "552100554"]


def test_clean_siren_list_empty_table():
    result = SirenProtegeProcessor.clean_siren_list(
        pd.DataFrame({"siren": []}, dtype=str)
    )

    assert result.empty


def test_clean_siren_list_missing_column():
    with pytest.raises(ValueError, match="'siren' column"):
        SirenProtegeProcessor.clean_siren_list(pd.DataFrame({"sirens": ["1"]}))


def test_check_removed_siren_within_threshold():
    previous = {str(i).zfill(9) for i in range(200)}
    current = (previous - {"000000000", "000000001"}) | {"999999999"}

    SirenProtegeProcessor.check_removed_siren(previous, current)


def test_check_removed_siren_above_threshold():
    previous = {str(i).zfill(9) for i in range(200)}
    current = previous - {"000000000", "000000001", "000000002"}

    with pytest.raises(ValueError, match="3 SIREN removed"):
        SirenProtegeProcessor.check_removed_siren(previous, current)


def test_check_removed_siren_replaced_list():
    # Same size but different content: an overwritten list must not go through.
    with pytest.raises(ValueError):
        SirenProtegeProcessor.check_removed_siren(
            {"111111111", "222222222"}, {"333333333", "444444444"}
        )


NOW = datetime(2026, 10, 21, 12, tzinfo=UTC)
PREVIOUS_DATE = "2026-10-15T08:00:00+00:00"


@pytest.mark.parametrize(
    "previous, current, previous_date, expected",
    [
        (None, {"111111111"}, None, NOW.isoformat()),
        ({"111111111"}, {"111111111"}, None, NOW.isoformat()),
        ({"111111111"}, {"111111111", "222222222"}, PREVIOUS_DATE, NOW.isoformat()),
        ({"111111111"}, {"111111111"}, PREVIOUS_DATE, PREVIOUS_DATE),
        ({"111111111", "222222222"}, {"111111111"}, PREVIOUS_DATE, PREVIOUS_DATE),
    ],
)
def test_compute_date_last_addition(previous, current, previous_date, expected):
    assert (
        SirenProtegeProcessor.compute_date_last_addition(
            previous, current, previous_date, NOW
        )
        == expected
    )


def test_check_date_last_addition():
    SirenProtegeProcessor.check_date_last_addition(
        (NOW - timedelta(days=9)).isoformat(), NOW
    )

    with pytest.raises(ValueError, match="more than 10 days"):
        SirenProtegeProcessor.check_date_last_addition(
            (NOW - timedelta(days=11)).isoformat(), NOW
        )
