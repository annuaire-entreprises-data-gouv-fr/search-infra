import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.config import (
    GRIST_COLUMN_NAMES,
    SIREN_PROTEGE_CONFIG,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.processor import (
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
        "date_validation_demande": "2026-11-01",
        "instructeur": "agent@example.gouv.fr",
        "date_depot_demande": "2026-10-20",
        "numero_dossier": numero_dossier,
    }


def preprocess(tmp_path, monkeypatch, rows, columns=None):
    """Run preprocess_data on a fake Grist table, return the CSV path and the
    saved last insertion date."""
    module = "data_pipelines_annuaire.workflows.data_pipelines.siren_protege.processor"
    grist_df = pd.DataFrame(rows, columns=columns, dtype=str)
    monkeypatch.setattr(f"{module}.Variable.get", lambda key: key)
    monkeypatch.setattr(
        f"{module}.GristTable", lambda *_: MagicMock(read=lambda: grist_df)
    )
    processor = SirenProtegeProcessor.__new__(SirenProtegeProcessor)
    processor.config = replace(SIREN_PROTEGE_CONFIG, tmp_folder=str(tmp_path))
    processor.metadata_path = str(tmp_path / "metadata.json")

    processor.preprocess_data()

    metadata = json.loads((tmp_path / "metadata.json").read_text())
    return (
        tmp_path / "siren_protege.csv",
        metadata[SirenProtegeProcessor.METADATA_KEY_LAST_INSERT],
    )


def read(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False).to_dict("records")


def test_preprocess_data(tmp_path, monkeypatch):
    path, _ = preprocess(
        tmp_path,
        monkeypatch,
        [
            {**row("356000000", legacy="TRUE", motif='=HYPERLINK("x")\n'), "x": "x"},
            {**row("552100554", nom="MARTIN", motif="premier"), "x": "x"},
            {**row("552100554", nom="MARTIN"), "x": "x"},
            {**row("552100554", nom=" martin "), "x": "x"},
            {**row(""), "x": "x"},
            {**row("356000000", date="1970-01-15", legacy="true"), "x": "x"},
            {**row("12345", date=" 1970-01-31 "), "x": "x"},
        ],
    )

    assert list(pd.read_csv(path, dtype=str).columns) == GRIST_COLUMN_NAMES
    assert read(path) == [
        row("12345"),
        row("356000000", legacy="TRUE", motif='=HYPERLINK("x")\n'),
        row("552100554", nom=" martin "),
        row("552100554", nom="MARTIN", motif="premier"),
    ]


def test_preprocess_data_csv_round_trip(tmp_path, monkeypatch):
    tricky = 'a, "b"\r\nc;d\te'
    record = {
        **{column: tricky for column in GRIST_COLUMN_NAMES},
        "siren": "356000000",
        "date_de_naissance": "1970-01",
        "date_ajout": "2026-11-02",
    }

    path, _ = preprocess(tmp_path, monkeypatch, [record])

    assert b'"a, ""b""\r\nc;d\te"' in path.read_bytes()
    assert read(path) == [record]


def test_preprocess_data_keeps_oldest_request_of_a_dirigeant(tmp_path, monkeypatch):
    newer = {**row("356000000", motif="seconde"), "date_ajout": "2026-11-10"}

    path, last_insertion_date = preprocess(
        tmp_path, monkeypatch, [row("356000000", motif="première"), newer]
    )

    assert read(path) == [row("356000000", motif="première")]
    # The newer request is dropped from the CSV but still counts as an insertion
    assert last_insertion_date == "2026-11-10"


def test_preprocess_data_keeps_homonyms_of_other_companies(tmp_path, monkeypatch):
    path, _ = preprocess(tmp_path, monkeypatch, [row("356000000"), row("552100554")])

    assert len(read(path)) == 2


def test_preprocess_data_empty_table(tmp_path, monkeypatch):
    path, last_insertion_date = preprocess(
        tmp_path, monkeypatch, [], columns=GRIST_COLUMN_NAMES
    )

    assert read(path) == []
    assert last_insertion_date == ""


def test_preprocess_data_missing_column(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="missing the columns {'motif'}"):
        preprocess(
            tmp_path,
            monkeypatch,
            [],
            columns=[c for c in GRIST_COLUMN_NAMES if c != "motif"],
        )


@pytest.mark.parametrize("days, fails", [(14, False), (15, True), (None, True)])
def test_check_last_dossier_insertion_date(tmp_path, monkeypatch, days, fails):
    context = {"ti": MagicMock()}
    monkeypatch.setattr(
        "data_pipelines_annuaire.workflows.data_pipelines.siren_protege."
        "processor.get_current_context",
        lambda: context,
    )
    processor = SirenProtegeProcessor.__new__(SirenProtegeProcessor)
    processor.metadata_path = str(tmp_path / "metadata.json")
    last_addition = (
        ""
        if days is None
        else (datetime.now(tz=UTC).date() - timedelta(days=days)).isoformat()
    )
    (tmp_path / "metadata.json").write_text(
        json.dumps({processor.METADATA_KEY_LAST_INSERT: last_addition})
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
