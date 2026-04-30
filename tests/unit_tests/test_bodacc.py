import json

import pandas as pd
import pytest

from data_pipelines_annuaire.workflows.data_pipelines.bodacc.utils import (
    get_previous_ids_to_discard,
    get_processed_ids_to_discard,
    parse_date_bodacc,
    parse_jugement_json,
    parse_radiation_json,
    process_discarded_announcements,
)

# parse_date_bodacc()


@pytest.mark.parametrize(
    "input, expected",
    [
        ("2022-01-27", "2022-01-27"),
        ("27/01/2022", "2022-01-27"),
        ("27 novembre 2008", "2008-11-27"),
        ("1er janvier 2020", "2020-01-01"),
        ("27\xa0novembre\xa02008", "2008-11-27"),  # non-breaking spaces
        ("", ""),
        ("not a date", ""),
    ],
)
def test_parse_date_bodacc(input, expected):
    assert parse_date_bodacc(input) == expected


# parse_radiation_json()


def test_parse_radiation_json_pp():
    assert (
        parse_radiation_json('{"dateCessationActivitePP": "2022-01-27"}')
        == "2022-01-27"
    )


def test_parse_radiation_json_pp_legacy():
    data = '{"radiationPP": {"dateCessationActivitePP": "2019-06-15"}}'
    assert parse_radiation_json(data) == "2019-06-15"


def test_parse_radiation_json_pm_no_date():
    assert parse_radiation_json("{}") == ""


def test_parse_radiation_json_empty():
    assert parse_radiation_json("") == ""


# parse_jugement_json()


def test_parse_jugement_json_full():
    data = '{"famille": "Ouverture", "nature": "Redressement judiciaire", "date": "2024-07-24"}'
    assert parse_jugement_json(data) == {
        "famille": "Ouverture",
        "nature": "Redressement judiciaire",
        "date": "2024-07-24",
    }


def test_parse_jugement_json_date_converted():
    data = '{"famille": "Ouverture", "nature": "Redressement judiciaire", "date": "27/11/2008"}'
    result = parse_jugement_json(data)
    assert result["date"] == "2008-11-27"


def test_parse_jugement_json_missing_keys():
    assert parse_jugement_json("{}") == {"famille": "", "nature": "", "date": ""}


def test_parse_jugement_json_invalid_json():
    assert parse_jugement_json("not json") == {"famille": "", "nature": "", "date": ""}


def test_parse_jugement_json_empty():
    assert parse_jugement_json("") == {"famille": "", "nature": "", "date": ""}


# Helper to build parutionavisprecedent JSON


def _avis_precedent(num_parution: str, num_annonce: str) -> str:
    return json.dumps(
        {
            "nomPublication": "BODACC A",
            "numeroParution": num_parution,
            "numeroAnnonce": num_annonce,
        }
    )


# get_discarded_ids()


def test_get_discarded_ids_annulation():
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20242000"],
            "typeavis": ["annonce", "annulation"],
            "parutionavisprecedent": ["", _avis_precedent("2024", "1000")],
        }
    )
    assert get_previous_ids_to_discard(df) == {"A20241000"}


def test_get_discarded_ids_rectificatif():
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20243000"],
            "typeavis": ["annonce", "rectificatif"],
            "parutionavisprecedent": ["", _avis_precedent("2024", "1000")],
        }
    )
    assert get_previous_ids_to_discard(df) == {"A20241000"}


def test_get_discarded_ids_both():
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20242000", "A20243000"],
            "typeavis": ["annonce", "annulation", "rectificatif"],
            "parutionavisprecedent": [
                "",
                _avis_precedent("2024", "1000"),
                _avis_precedent("2024", "2000"),
            ],
        }
    )
    discarded = get_previous_ids_to_discard(df)
    assert discarded == {"A20241000", "A20242000"}


# get_ids_to_discard()


def test_get_ids_to_discard_annulation():
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20242000"],
            "typeavis": ["annonce", "annulation"],
            "parutionavisprecedent": ["", _avis_precedent("2024", "1000")],
        }
    )
    assert get_processed_ids_to_discard(df) == {"A20242000"}


def test_get_ids_to_discard_radiation_doffice():
    radiation_doffice = json.dumps({"commentaire": "Rapport de radiation d'office"})
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20243000"],
            "typeavis": ["annonce", "rectificatif"],
            "parutionavisprecedent": ["", _avis_precedent("2024", "1000")],
            "radiationaurcs": ["", radiation_doffice],
        }
    )
    assert get_processed_ids_to_discard(df) == {"A20243000"}


def test_get_ids_to_discard_keeps_normal_rectificatif():
    normal_radiation = json.dumps({"dateCessationActivitePP": "2024-01-15"})
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20243000"],
            "typeavis": ["annonce", "rectificatif"],
            "parutionavisprecedent": ["", _avis_precedent("2024", "1000")],
            "radiationaurcs": ["", normal_radiation],
        }
    )
    assert get_processed_ids_to_discard(df) == set()


# process_discarded_announcements()


def test_filter_discarded_removes_annulation_and_its_target():
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20242000"],
            "typeavis": ["annonce", "annulation"],
            "parutionavisprecedent": ["", _avis_precedent("2024", "1000")],
        }
    )
    result = process_discarded_announcements(df)
    assert result["id"].tolist() == []


def test_filter_discarded_removes_rectified_but_keeps_rectificatif():
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20243000"],
            "typeavis": ["annonce", "rectificatif"],
            "parutionavisprecedent": ["", _avis_precedent("2024", "1000")],
        }
    )
    result = process_discarded_announcements(df)
    assert result["id"].tolist() == ["A20243000"]
    assert result["typeavis"].tolist() == ["rectificatif"]


def test_filter_discarded_keeps_unrelated_annonce():
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20245000", "A20243000"],
            "typeavis": ["annonce", "annonce", "rectificatif"],
            "parutionavisprecedent": ["", "", _avis_precedent("2024", "1000")],
        }
    )
    result = process_discarded_announcements(df)
    assert set(result["id"].tolist()) == {"A20245000", "A20243000"}


def test_filter_discarded_removes_rectificatif_radiation_doffice():
    radiation_doffice = json.dumps({"commentaire": "Rapport de radiation d'office"})
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20243000"],
            "typeavis": ["annonce", "rectificatif"],
            "parutionavisprecedent": ["", _avis_precedent("2024", "1000")],
            "radiationaurcs": ["", radiation_doffice],
        }
    )
    result = process_discarded_announcements(df)
    assert result["id"].tolist() == []


def test_filter_discarded_keeps_normal_rectificatif_with_radiationaurcs():
    normal_radiation = json.dumps({"dateCessationActivitePP": "2024-01-15"})
    df = pd.DataFrame(
        {
            "id": ["A20241000", "A20243000"],
            "typeavis": ["annonce", "rectificatif"],
            "parutionavisprecedent": ["", _avis_precedent("2024", "1000")],
            "radiationaurcs": ["", normal_radiation],
        }
    )
    result = process_discarded_announcements(df)
    assert result["id"].tolist() == ["A20243000"]
