import pandas as pd

from data_pipelines_annuaire.workflows.data_pipelines.etl.data_fetch_clean.dirigeants import (
    DIRIGEANT_PP_KEYS,
    group_roles,
)


def personne(prenoms, date_de_naissance, role, role_description):
    return {
        "siren": "123456789",
        "nom": "DE MAUPASSANT",
        "prenoms": prenoms,
        "nom_usage": None,
        "date_de_naissance": date_de_naissance,
        "nationalite": None,
        "date_mise_a_jour": "1893-07-06",
        "role": role,
        "role_description": role_description,
    }


def roles_by_personne(df):
    return sorted(zip(df["prenoms"], df["role_description"]))


def test_group_roles_aggregates_distinct_roles():
    df = pd.DataFrame(
        [
            personne("GUSTAVE", "1821-12", "75", "Associé"),
            personne("GUY", "1850-08", "75", "Associé indéfiniment responsable"),
            personne("GUY", "1850-08", "40", "Liquidateur"),
        ]
    )
    result = group_roles(df, DIRIGEANT_PP_KEYS)
    assert roles_by_personne(result) == [
        ("GUSTAVE", "Associé"),
        ("GUY", "Liquidateur, Associé indéfiniment responsable"),
    ]
    assert sorted(result["role"]) == ["40, 75", "75"]


def test_group_roles_splits_duplicated_role():
    df = pd.DataFrame(
        [
            personne("GUY", "1850-08", "40", "Liquidateur"),
            personne("GUY", "1850-08", "75", "Associé indéfiniment responsable"),
            personne("GUY", "1850-08", "40", "Liquidateur"),
            personne("GUY", "1850-08", "40", "Liquidateur"),
        ]
    )
    result = group_roles(df, DIRIGEANT_PP_KEYS)
    assert roles_by_personne(result) == [
        ("GUY", "Liquidateur"),
        ("GUY", "Liquidateur"),
        ("GUY", "Liquidateur, Associé indéfiniment responsable"),
    ]


def test_group_roles_keeps_missing_keys():
    df = pd.DataFrame(
        [
            personne("GUY", None, "40", "Liquidateur"),
            personne("GUY", None, "75", "Associé indéfiniment responsable"),
            personne("GUY", "1850-08", "40", "Liquidateur"),
        ]
    )
    result = group_roles(df, DIRIGEANT_PP_KEYS)
    assert len(result) == 2
    assert set(result["role"]) == {"40, 75", "40"}
