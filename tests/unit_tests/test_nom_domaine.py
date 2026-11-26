import pytest

from data_pipelines_annuaire.helpers.utils import normalize_nom_domaine
from data_pipelines_annuaire.workflows.data_pipelines.etl.data_fetch_clean.nom_domaine import (
    MAX_SIREN_PER_NOM_DOMAINE,
    clean_noms_de_domaine,
)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("belami.fr", "belami.fr"),
        (" BelAmi.FR ", "belami.fr"),
        ("www.belami.fr", "belami.fr"),
        ("https://www.belami.fr/", "belami.fr"),
        ("http:/belami.fr", "belami.fr"),
        ("https://belami.wixsite.com/maupassant", "belami.wixsite.com"),
        ("belami.fr:8080?ref=1", "belami.fr"),
        ("belami.fr.", "belami.fr"),
        ("néanails.fr", "néanails.fr"),
        ("bel-ami.co.uk", "bel-ami.co.uk"),
        ("guy@belami.fr", None),
        ("aucun", None),
        ("néant", None),
        ("n/a", None),
        (".fr", None),
        ("bel ami.fr", None),
        ("bel_ami.fr", None),
        ("-belami.fr", None),
        ("belami;fr", None),
        ("éclatd'azur.fr", None),
        ("belami.f", None),
        ("belami.123", None),
        ("", None),
        (None, None),
    ],
)
def test_normalize_nom_domaine(value, expected):
    assert normalize_nom_domaine(value) == expected


def test_clean_noms_de_domaine_keeps_every_rne_row():
    rows = [
        ("123456789", "https://www.belami.fr"),
        ("123456789", "belami.fr"),
        ("123456789", "aucun"),
        ("987654321", "BELAMI.FR"),
    ]
    assert clean_noms_de_domaine(rows) == [
        ("123456789", "https://www.belami.fr", "belami.fr"),
        ("123456789", "belami.fr", "belami.fr"),
        ("123456789", "aucun", None),
        ("987654321", "BELAMI.FR", "belami.fr"),
    ]


def test_clean_noms_de_domaine_drops_shared_domains():
    sirens = [f"{i:09d}" for i in range(MAX_SIREN_PER_NOM_DOMAINE)]
    rows = [(siren, "inpi.fr") for siren in sirens]
    rows += [(siren, "www.inpi.fr") for siren in sirens]
    rows += [(siren, "groupe.fr") for siren in sirens[1:]]
    result = clean_noms_de_domaine(rows)
    assert len(result) == len(rows)
    assert {nom for *_, nom in result} == {"groupe.fr", None}
