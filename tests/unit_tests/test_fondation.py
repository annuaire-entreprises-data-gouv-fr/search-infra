import pytest

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.indexing_fondation import (
    format_fondation_adresse,
)


@pytest.mark.parametrize(
    "fondation, expected",
    [
        (
            {"adresse": "12 rue de la Paix", "code_postal": "75002", "ville": "Paris"},
            "12 rue de la Paix 75002 Paris",
        ),
        (
            {"adresse": "12 rue de la Paix", "code_postal": None, "ville": "Paris"},
            "12 rue de la Paix Paris",
        ),
        (
            {"adresse": None, "code_postal": "75002", "ville": "Paris"},
            "75002 Paris",
        ),
        (
            {"adresse": "  ", "code_postal": "75002", "ville": " Paris "},
            "75002 Paris",
        ),
        (
            {"adresse": None, "code_postal": None, "ville": None},
            "",
        ),
        (
            {},
            "",
        ),
    ],
)
def test_format_fondation_adresse(fondation, expected):
    assert format_fondation_adresse(fondation) == expected
