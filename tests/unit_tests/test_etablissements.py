import pytest

from data_pipelines_annuaire.workflows.data_pipelines.etl.data_fetch_clean.etablissements import (
    combine_numero_voie,
)

NaN = float("nan")


@pytest.mark.parametrize(
    "numero_voie, dernier_numero_voie, expected",
    [
        ("1", "3", "1-3"),
        ("10", "12", "10-12"),
        ("1", NaN, "1"),
        (NaN, "3", "3"),
        (NaN, NaN, None),
        ("5", NaN, "5"),
    ],
)
def test_combine_numero_voie(numero_voie, dernier_numero_voie, expected):
    assert combine_numero_voie(numero_voie, dernier_numero_voie) == expected
