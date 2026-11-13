import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.config import (
    GRIST_COLUMN_NAMES,
    MOTIF_MAX_LENGTH,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.object_storage.config import (
    SIREN_PROTEGE_CONFIG,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.object_storage.processor import (
    SirenProtegeProcessor,
)


def row(
    siren,
    prenoms="JEAN",
    nom="DUPONT",
    date="1970-01",
    legacy="false",
    motif="",
    numero_dossier="",
):
    return {
        "siren": siren,
        "prenoms": prenoms,
        "nom": nom,
        "date_de_naissance": date,
        "legacy": legacy,
        "date_ajout": "2026-11-02",
        "motif": motif,
        "date_validation_instructeur": "2026-11-01",
        "date_depot_demande": "2026-10-20",
        "numero_dossier": numero_dossier,
    }


def test_clean_list():
    df = pd.DataFrame(
        [
            row(
                "356000000",
                prenoms=" jean  pierre ",
                legacy="TRUE",
                motif='=HYPERLINK("x")\n',
            ),
            row("552100554", nom="Martin", motif="a" * (MOTIF_MAX_LENGTH + 1)),
            row(" 552 100 554 ", nom="Martin"),
            row(""),
            row("356000000", prenoms="JEAN PIERRE", legacy="true"),
            row("12345"),
        ]
    ).assign(other="x")

    result = SirenProtegeProcessor.clean_list(df)

    assert list(result.columns) == GRIST_COLUMN_NAMES
    assert result.to_dict("records") == [
        row("12345"),
        row(
            "356000000",
            prenoms="JEAN PIERRE",
            legacy="true",
            motif="'=HYPERLINK(x)\n",
        ),
        row("552100554", nom="MARTIN", motif="a" * MOTIF_MAX_LENGTH),
    ]


def test_csv_quotes_multiline_motif(tmp_path):
    df = SirenProtegeProcessor.clean_list(
        pd.DataFrame([row("356000000", motif='Ligne 1, "a"\r\nLigne 2')])
    )
    path = tmp_path / "siren_protege.csv"
    df.to_csv(path, index=False)

    assert b'"Ligne 1, a\r\nLigne 2"' in path.read_bytes()
    assert pd.read_csv(path, dtype=str, keep_default_na=False).equals(df)


def test_clean_list_keeps_oldest_request_of_a_dirigeant():
    df = pd.DataFrame(
        [
            row("356000000", motif="première", numero_dossier=" 12 "),
            row("356000000", motif="seconde", numero_dossier="34"),
        ]
    )

    result = SirenProtegeProcessor.clean_list(df)

    assert result.to_dict("records") == [
        row("356000000", motif="première", numero_dossier="12")
    ]


def test_clean_list_keeps_homonyms_of_other_companies():
    df = pd.DataFrame([row("356000000"), row("552100554")])

    assert len(SirenProtegeProcessor.clean_list(df)) == 2


def test_clean_list_empty_table():
    result = SirenProtegeProcessor.clean_list(
        pd.DataFrame(columns=list(row("")), dtype=str)
    )

    assert result.empty


def test_clean_list_missing_column():
    with pytest.raises(ValueError, match="missing the columns"):
        SirenProtegeProcessor.clean_list(pd.DataFrame({"siren": ["1"]}))


def keys(n):
    return {(str(i).zfill(9), "JEAN", "DUPONT", "1970-01") for i in range(n)}


def test_check_removed_rows_within_threshold():
    previous = keys(200)
    current = set(sorted(previous)[2:]) | {("999999999", "A", "B", "1970-01")}

    SirenProtegeProcessor.check_removed_rows(previous, current)


def test_check_removed_rows_above_threshold():
    previous = keys(200)

    with pytest.raises(ValueError, match="3 rows removed"):
        SirenProtegeProcessor.check_removed_rows(previous, set(sorted(previous)[3:]))


def test_check_removed_rows_replaced_list():
    # Same size but different content: an overwritten list must not go through.
    with pytest.raises(ValueError):
        SirenProtegeProcessor.check_removed_rows(
            {("111111111", "A", "B", "")}, {("222222222", "A", "B", "")}
        )


NOW = datetime(2026, 10, 21, 12, tzinfo=UTC)
PREVIOUS_DATE = "2026-10-15T08:00:00+00:00"
A = ("111111111", "A", "B", "1970-01")
B = ("222222222", "A", "B", "1970-01")


@pytest.mark.parametrize(
    "previous, current, previous_date, expected",
    [
        (None, {A}, None, NOW.isoformat()),
        ({A}, {A}, None, NOW.isoformat()),
        ({A}, {A, B}, PREVIOUS_DATE, NOW.isoformat()),
        ({A}, {A}, PREVIOUS_DATE, PREVIOUS_DATE),
        ({A, B}, {A}, PREVIOUS_DATE, PREVIOUS_DATE),
    ],
)
def test_compute_date_last_addition(previous, current, previous_date, expected):
    assert (
        SirenProtegeProcessor.compute_date_last_addition(
            previous, current, previous_date, NOW
        )
        == expected
    )


@pytest.mark.parametrize("days, fails", [(13, False), (15, True)])
def test_check_new_row_added(tmp_path, monkeypatch, days, fails):
    context = {"ti": MagicMock()}
    monkeypatch.setattr(
        "data_pipelines_annuaire.workflows.data_pipelines.siren_protege."
        "object_storage.processor.get_current_context",
        lambda: context,
    )
    processor = SirenProtegeProcessor.__new__(SirenProtegeProcessor)
    processor.metadata_path = str(tmp_path / "metadata.json")
    last_addition = datetime.now(tz=UTC) - timedelta(days=days)
    (tmp_path / "metadata.json").write_text(
        json.dumps({processor.METADATA_KEY_LAST_INSERT: last_addition.isoformat()})
    )

    if fails:
        with pytest.raises(ValueError, match="more than 14 days"):
            processor.check_last_dossier_insertion_date()
        context["ti"].xcom_push.assert_called_once()
    else:
        processor.check_last_dossier_insertion_date()
        context["ti"].xcom_push.assert_not_called()


def test_compare_files_object_storage_publishes_privately():
    processor = SirenProtegeProcessor.__new__(SirenProtegeProcessor)
    processor.config = SIREN_PROTEGE_CONFIG
    processor.object_storage_client = MagicMock()
    processor.object_storage_client.compare_files.return_value = False

    assert processor.compare_files_object_storage() is True
    call = processor.object_storage_client.send_files.call_args
    assert [f["dest_path"] + f["dest_name"] for f in call.kwargs["list_files"]] == [
        "siren_protege/latest/siren_protege.csv",
        "siren_protege/latest/metadata.json",
    ]
    assert call.kwargs["is_public"] is False
