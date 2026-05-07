import pytest

from data_pipelines_annuaire.workflows.data_pipelines.bodacc.utils import (
    parse_date_bodacc,
    parse_jugement_json,
    parse_radiation_json,
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
