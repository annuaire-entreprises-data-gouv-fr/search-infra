import pytest

from helpers.clean_dirigeants import (
    drop_duplicates_dirigeants_pm,
    drop_duplicates_dirigeants_pp,
)


@pytest.mark.parametrize(
    """
        list_dict_dirigeants,
        expected,
    """,
    [
        (
            [
                {
                    "nom": "vatin (jordens)",
                    "prenoms": "regina",
                    "date_naissance": None,
                    "ville_naissance": "",
                    "pays_naissance": "",
                    "qualite": "Directeur général",
                },
                {
                    "nom": "vatin (jordens)",
                    "prenoms": "regina",
                    "date_naissance": "1948-08-07",
                    "ville_naissance": " Allemagne ",
                    "pays_naissance": "ALLEMAGNE",
                    "qualite": "Directeur général",
                },
                {
                    "nom": "vatin",
                    "prenoms": "gerard",
                    "date_naissance": None,
                    "ville_naissance": "",
                    "pays_naissance": "",
                    "qualite": "Président",
                },
                {
                    "nom": "vatin",
                    "prenoms": "gerard",
                    "date_naissance": "1947-10-07",
                    "ville_naissance": "79270 Saint-Maixent-L'École 79 ",
                    "pays_naissance": "FRANCE",
                    "qualite": "Président",
                },
                {
                    "nom": "vatin",
                    "prenoms": "gérard",
                    "date_naissance": None,
                    "ville_naissance": "",
                    "pays_naissance": "",
                    "qualite": "Président",
                },
                {
                    "nom": "vatin",
                    "prenoms": "jacques",
                    "date_naissance": None,
                    "ville_naissance": "",
                    "pays_naissance": "",
                    "qualite": "Directeur général",
                },
                {
                    "nom": "vatin",
                    "prenoms": "jacques",
                    "date_naissance": "1930-04-03",
                    "ville_naissance": "02691 Saint-Quentin 02 ",
                    "pays_naissance": "FRANCE",
                    "qualite": "Directeur général",
                },
                {
                    "nom": "vatin-chaix (vatin)",
                    "prenoms": "bertrand heïnrich Rene",
                    "date_naissance": None,
                    "ville_naissance": "",
                    "pays_naissance": "",
                    "qualite": "directeur general, Administrateur",
                },
                {
                    "nom": "vatin-chaix (vatin)",
                    "prenoms": "bertrand heinrich rené",
                    "date_naissance": "1900-01-08",
                    "ville_naissance": " Chêne bougeries ",
                    "pays_naissance": "SUISSE",
                    "qualite": "Directeur général",
                },
            ],
            [
                {
                    "nom": "vatin (jordens)",
                    "prenoms": "regina",
                    "date_naissance": "1948-08-07",
                    "qualite": "Directeur général",
                },
                {
                    "nom": "vatin",
                    "prenoms": "gérard",
                    "date_naissance": "1947-10-07",
                    "qualite": "Président",
                },
                {
                    "nom": "vatin",
                    "prenoms": "jacques",
                    "date_naissance": "1930-04-03",
                    "qualite": "Directeur général",
                },
                {
                    "nom": "vatin-chaix (vatin)",
                    "prenoms": "bertrand heinrich rené",
                    "date_naissance": "1900-01-08",
                    "qualite": "Directeur général, Administrateur",
                },
            ],
        ),
    ],
)
def test_drop_dirigeant_pp_duplicates(
    list_dict_dirigeants,
    expected,
):
    print(drop_duplicates_dirigeants_pp(list_dict_dirigeants))
    assert (
        drop_duplicates_dirigeants_pp(
            list_dict_dirigeants,
        )
        == expected
    )


@pytest.mark.parametrize(
    """
        list_dict_dirigeants,
        expected,
    """,
    [
        (
            [
                {
                    "siren": "046350088",
                    "denomination": "AEQUITAS AUDIT - SOCIETE DE COMMISSARIAT AUX COMPTES",
                    "sigle": None,
                    "qualite": "Commissaire aux comptes titulaire",
                },
                {
                    "siren": "302715966",
                    "denomination": "VIVESCIA",
                    "sigle": None,
                    "qualite": "Administrateur, Président",
                },
                {
                    "siren": "302715966",
                    "denomination": "VIVE",
                    "sigle": None,
                    "qualite": "Administrateur",
                },
                {
                    "siren": "330189028",
                    "denomination": "NORIAP",
                    "sigle": None,
                    "qualite": "Administrateur",
                },
                {
                    "siren": "349954636",
                    "denomination": "SCP MONTIEL ET ASSOCIES",
                    "sigle": "DTD",
                    "qualite": "Commissaire aux comptes titulaire",
                },
                {
                    "siren": "349954636",
                    "denomination": "SCP MONTIEL ET ASSOCIES",
                    "sigle": None,
                    "qualite": "administrateur",
                },
            ],
            [
                {
                    "siren": "046350088",
                    "denomination": "AEQUITAS AUDIT - SOCIETE DE COMMISSARIAT AUX COMPTES",
                    "sigle": None,
                    "qualite": "Commissaire aux comptes titulaire",
                },
                {
                    "siren": "302715966",
                    "denomination": "VIVE",
                    "sigle": None,
                    "qualite": "Administrateur, Président",
                },
                {
                    "siren": "330189028",
                    "denomination": "NORIAP",
                    "sigle": None,
                    "qualite": "Administrateur",
                },
                {
                    "siren": "349954636",
                    "denomination": "SCP MONTIEL ET ASSOCIES",
                    "sigle": None,
                    "qualite": "Commissaire aux comptes titulaire, administrateur",
                },
            ],
        ),
    ],
)
def test_drop_dirigeant_pm_duplicates(
    list_dict_dirigeants,
    expected,
):
    print(drop_duplicates_dirigeants_pm(list_dict_dirigeants))
    assert (
        drop_duplicates_dirigeants_pm(
            list_dict_dirigeants,
        )
        == expected
    )
