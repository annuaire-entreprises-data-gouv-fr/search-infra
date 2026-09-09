import json
import sqlite3

import pandas as pd
import pytest

from data_pipelines_annuaire.workflows.data_pipelines.bodacc.annonces_couples import (
    process_annonces_couples,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.config import (
    ANNONCES_COUPLES_CONFIG,
    BODACC_CONFIG,
    CREATIONS_CONFIG,
    RADIATIONS_CONFIG,
    build_annonces_couples_url,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.creations import (
    _parse_creation_date,
    process_creations,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.procedures_collectives import (
    _apply_procedure_collective_rules,
    _is_cloture,
    _load_procedure_collective_rules,
    _parse_jugement_json,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.radiations import (
    _is_transfert_siege_hors_ressort,
    _parse_radiation_json,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.utils import (
    _extract_sirens_from_personne,
    extract_sirens_from_listepersonnes,
    extract_sirens_greffes_from_listepersonnes,
    fix_mojibake,
    get_previous_ids_to_discard,
    get_processed_ids_to_discard,
    normalize_greffe,
    parse_date_bodacc,
    process_discarded_announcements,
)

# fix_mojibake()


@pytest.mark.parametrize(
    "input, expected",
    [
        ("clÃ´ture", "clôture"),
        ("procÃ©dure", "procédure"),
        ("DÃ©pÃ´t de l'Ã©tat des crÃ©ances", "Dépôt de l'état des créances"),
        ("ArrÃªt de la cour d'appel", "Arrêt de la cour d'appel"),
        ("Jugement prononÃ§ant", "Jugement prononçant"),
        ("Jugement de clôture", "Jugement de clôture"),
        ("", ""),
    ],
)
def test_fix_mojibake(input, expected):
    assert fix_mojibake(input) == expected


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


# _parse_radiation_json()


def test_parse_radiation_json_pp():
    assert (
        _parse_radiation_json('{"dateCessationActivitePP": "2022-01-27"}')
        == "2022-01-27"
    )


def test_parse_radiation_json_pp_legacy():
    data = '{"radiationPP": {"dateCessationActivitePP": "2019-06-15"}}'
    assert _parse_radiation_json(data) == "2019-06-15"


def test_parse_radiation_json_pm_no_date():
    assert _parse_radiation_json("{}") == ""


def test_parse_radiation_json_empty():
    assert _parse_radiation_json("") == ""


# _is_transfert_siege_hors_ressort()


def test_is_transfert_siege_hors_ressort():
    data = json.dumps(
        {"radiationPM": "O", "commentaire": "Transfert du siège hors ressort"}
    )
    assert _is_transfert_siege_hors_ressort(data) is True


def test_is_transfert_siege_hors_ressort_other_comment():
    data = json.dumps({"commentaire": "Rapport de radiation d'office"})
    assert _is_transfert_siege_hors_ressort(data) is False


def test_is_transfert_siege_hors_ressort_no_comment():
    data = json.dumps({"dateCessationActivitePP": "2024-01-15"})
    assert _is_transfert_siege_hors_ressort(data) is False


def test_is_transfert_siege_hors_ressort_empty():
    assert _is_transfert_siege_hors_ressort("") is False


# _parse_jugement_json()


def test_parse_jugement_json_full():
    data = '{"famille": "Ouverture", "nature": "Redressement judiciaire", "date": "2024-07-24"}'
    assert _parse_jugement_json(data) == {
        "famille": "Ouverture",
        "nature": "Redressement judiciaire",
        "complementJugement": "",
        "date": "2024-07-24",
    }


def test_parse_jugement_json_date_converted():
    data = '{"famille": "Ouverture", "nature": "Redressement judiciaire", "date": "27/11/2008"}'
    result = _parse_jugement_json(data)
    assert result["date"] == "2008-11-27"


def test_parse_jugement_json_missing_keys():
    assert _parse_jugement_json("{}") == {
        "famille": "",
        "nature": "",
        "complementJugement": "",
        "date": "",
    }


def test_parse_jugement_json_invalid_json():
    assert _parse_jugement_json("not json") == {
        "famille": "",
        "nature": "",
        "complementJugement": "",
        "date": "",
    }


def test_parse_jugement_json_empty():
    assert _parse_jugement_json("") == {
        "famille": "",
        "nature": "",
        "complementJugement": "",
        "date": "",
    }


def test_parse_jugement_json_fixes_mojibake():
    data = '{"famille": "Jugement de clÃ´ture", "nature": "Jugement de clÃ´ture pour insuffisance d\'actif", "date": "2024-01-15"}'
    result = _parse_jugement_json(data)
    assert result["famille"] == "Jugement de clôture"
    assert result["nature"] == "Jugement de clôture pour insuffisance d'actif"


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


# Expiration des procédures > 10 ans


def _apply_expiration_logic(df: pd.DataFrame) -> pd.DataFrame:
    """Reproduit la logique d'expiration de process_procedures_collectives."""
    df["date"] = pd.to_datetime(df["date"], errors="coerce", format="%Y-%m-%d")
    df["is_cloture"] = df["procedure_collective_famille"].apply(_is_cloture)
    ten_years_ago = pd.Timestamp.now() - pd.DateOffset(years=10)
    df["is_expired"] = df["date"] < ten_years_ago
    df["nature"] = df.apply(
        lambda row: "" if row["is_cloture"] or row["is_expired"] else row["nature"],
        axis=1,
    )
    return df


def test_procedure_older_than_10_years_has_no_nature():
    df = pd.DataFrame(
        {
            "procedure_collective_famille": ["Ouverture"],
            "nature": ["Redressement judiciaire"],
            "date": ["2010-01-01"],
        }
    )
    result = _apply_expiration_logic(df)
    assert result["nature"].iloc[0] == ""


def test_recent_procedure_keeps_nature():
    df = pd.DataFrame(
        {
            "procedure_collective_famille": ["Ouverture"],
            "nature": ["Redressement judiciaire"],
            "date": ["2024-01-01"],
        }
    )
    result = _apply_expiration_logic(df)
    assert result["nature"].iloc[0] == "Redressement judiciaire"


def test_cloture_procedure_has_no_nature_regardless_of_age():
    df = pd.DataFrame(
        {
            "procedure_collective_famille": ["Jugement de clôture"],
            "nature": ["Liquidation judiciaire"],
            "date": ["2024-01-01"],
        }
    )
    result = _apply_expiration_logic(df)
    assert result["nature"].iloc[0] == ""


# _apply_procedure_collective_rules()


@pytest.fixture
def rules():
    return _load_procedure_collective_rules()


def test_rules_liquidation_judiciaire(rules):
    statut = _apply_procedure_collective_rules(
        "Jugement d'ouverture de liquidation judiciaire", "", rules
    )
    assert statut == "liquidation_judiciaire"


def test_rules_redressement_judiciaire(rules):
    statut = _apply_procedure_collective_rules(
        "Jugement d'ouverture d'une procédure de redressement judiciaire", "", rules
    )
    assert statut == "redressement_judiciaire"


def test_rules_sauvegarde(rules):
    statut = _apply_procedure_collective_rules(
        "Jugement d'ouverture d'une procédure de sauvegarde", "", rules
    )
    assert statut == "sauvegarde"


def test_rules_cloture_returns_none(rules):
    statut = _apply_procedure_collective_rules(
        "Jugement de clôture pour insuffisance d'actif", "", rules
    )
    assert statut is None


def test_rules_autre_jugement_with_complement(rules):
    statut = _apply_procedure_collective_rules(
        "Autre jugement prononçant",
        "la clôture de la procédure pour insuffisance d'actif",
        rules,
    )
    assert statut is None


def test_rules_autre_jugement_with_complement_liquidation(rules):
    statut = _apply_procedure_collective_rules(
        "Autre jugement prononçant",
        "l'ouverture d'une procédure de liquidation judiciaire",
        rules,
    )
    assert statut == "liquidation_judiciaire"


def test_rules_unknown_nature_returns_none(rules):
    statut = _apply_procedure_collective_rules("Nature inconnue", "", rules)
    assert statut is None


def test_rules_empty_nature():
    rules = _load_procedure_collective_rules()
    assert _apply_procedure_collective_rules("", "", rules) is None


# extract_sirens_from_personne()


def test_extract_sirens_rcs_and_rm():
    data = json.dumps(
        {
            "personne": [
                {
                    "typePersonne": "pm",
                    "numeroImmatriculation": {
                        "numeroIdentification": "481 738 821",
                        "codeRCS": "RCS",
                    },
                },
                {
                    "typePersonne": "pm",
                    "numeroImmatriculation": {
                        "numeroIdentification": "510 784 887",
                        "codeRCS": "RCS",
                    },
                },
                {
                    "typePersonne": "pp",
                    "inscriptionRM": {
                        "numeroIdentificationRM": "424 557 189",
                        "codeRM": "RM",
                    },
                },
            ]
        }
    )
    assert _extract_sirens_from_personne(data) == [
        "481738821",
        "510784887",
        "424557189",
    ]


def test_extract_sirens_deduplicates():
    data = json.dumps(
        {
            "personne": [
                {
                    "typePersonne": "pm",
                    "numeroImmatriculation": {
                        "numeroIdentification": "123 456 789",
                    },
                },
                {
                    "typePersonne": "pm",
                    "numeroImmatriculation": {
                        "numeroIdentification": "123 456 789",
                    },
                },
            ]
        }
    )
    assert _extract_sirens_from_personne(data) == ["123456789"]


def test_extract_sirens_empty_json():
    assert _extract_sirens_from_personne("{}") == []


def test_extract_sirens_invalid_json():
    assert _extract_sirens_from_personne("not json") == []


def test_extract_sirens_missing_identification():
    data = json.dumps(
        {
            "personne": [
                {"typePersonne": "pm", "denomination": "SOME COMPANY"},
            ]
        }
    )
    assert _extract_sirens_from_personne(data) == []


def test_extract_sirens_single_personne_not_list():
    data = json.dumps(
        {
            "personne": {
                "typePersonne": "pm",
                "numeroImmatriculation": {
                    "numeroIdentification": "111 222 333",
                },
            }
        }
    )
    assert _extract_sirens_from_personne(data) == ["111222333"]


# extract_sirens_from_listepersonnes()


def test_unnest_listepersonnes_creates_multiple_rows():
    lp = json.dumps(
        {
            "personne": [
                {
                    "typePersonne": "pm",
                    "numeroImmatriculation": {
                        "numeroIdentification": "111 111 111",
                    },
                },
                {
                    "typePersonne": "pp",
                    "inscriptionRM": {
                        "numeroIdentificationRM": "222 222 222",
                    },
                },
            ]
        }
    )
    df = pd.DataFrame(
        {
            "id": ["A001"],
            "listepersonnes": [lp],
            "dateparution": ["2024-01-01"],
        }
    )
    result = extract_sirens_from_listepersonnes(df)
    assert len(result) == 2
    assert result["siren"].tolist() == ["111111111", "222222222"]
    assert result["id"].tolist() == ["A001", "A001"]
    assert result["dateparution"].tolist() == ["2024-01-01", "2024-01-01"]


def test_unnest_listepersonnes_drops_rows_without_siren():
    df = pd.DataFrame(
        {
            "id": ["A001", "A002"],
            "listepersonnes": [
                "{}",
                json.dumps(
                    {
                        "personne": [
                            {
                                "typePersonne": "pm",
                                "numeroImmatriculation": {
                                    "numeroIdentification": "999 999 999"
                                },
                            }
                        ]
                    }
                ),
            ],
            "dateparution": ["2024-01-01", "2024-01-02"],
        }
    )
    result = extract_sirens_from_listepersonnes(df)
    assert len(result) == 1
    assert result["siren"].iloc[0] == "999999999"


# _parse_creation_date()


def test_parse_creation_date_immatriculation():
    acte = '{"dateImmatriculation": "2026-06-15", "dateCommencementActivite": "2026-06-16"}'
    assert _parse_creation_date(acte) == "2026-06-15"


def test_parse_creation_date_fallback_commencement():
    acte = '{"dateCommencementActivite": "2026-06-16"}'
    assert _parse_creation_date(acte) == "2026-06-16"


def test_parse_creation_date_no_date():
    assert _parse_creation_date('{"creation": {"categorieCreation": "x"}}') == ""


def test_parse_creation_date_empty():
    assert _parse_creation_date("") == ""


def test_parse_creation_date_invalid_json():
    assert _parse_creation_date("not json") == ""


# process_creations()


def _creation_listepersonnes(siren: str) -> str:
    return json.dumps(
        {"personne": {"numeroImmatriculation": {"numeroIdentification": siren}}}
    )


def test_process_creations_keeps_all_rows(tmp_path):
    raw = tmp_path / "creations-raw.csv"
    pd.DataFrame(
        {
            "id": ["A001", "A002", "A003"],
            "listepersonnes": [
                _creation_listepersonnes("111 111 111"),
                _creation_listepersonnes("222 222 222"),
                # Même SIREN ré-immatriculé : on garde les deux annonces.
                _creation_listepersonnes("111 111 111"),
            ],
            "dateparution": ["2024-01-10", "2024-02-10", "2025-03-10"],
            "typeavis": ["annonce", "annonce", "annonce"],
            "acte": [
                '{"dateImmatriculation": "2024-01-05"}',
                '{"dateCommencementActivite": "2024-02-05"}',
                '{"dateImmatriculation": "2025-03-05"}',
            ],
            "parutionavisprecedent": ["", "", ""],
        }
    ).to_csv(raw, sep=";", index=False)

    df = process_creations(str(raw), chunk_size=100)

    assert len(df) == 3
    assert sorted(df["siren"].tolist()) == ["111111111", "111111111", "222222222"]
    dates = dict(zip(df["id_annonce"], df["date"]))
    assert dates["A001"] == pd.Timestamp("2024-01-05")
    assert dates["A002"] == pd.Timestamp("2024-02-05")
    assert dates["A003"] == pd.Timestamp("2025-03-05")


def test_process_creations_filters_annulation(tmp_path):
    raw = tmp_path / "creations-raw.csv"
    # build_bodacc_id => f"{lettre}{numeroParution}{numeroAnnonce}" = "A11"
    avis_precedent = json.dumps(
        {"nomPublication": "BODACC A", "numeroParution": "1", "numeroAnnonce": "1"}
    )
    pd.DataFrame(
        {
            "id": ["A11", "A20", "A30"],
            "listepersonnes": [
                _creation_listepersonnes("111 111 111"),
                _creation_listepersonnes("222 222 222"),
                _creation_listepersonnes("333 333 333"),
            ],
            "dateparution": ["2024-01-10", "2024-02-10", "2024-03-10"],
            "typeavis": ["annonce", "annulation", "annonce"],
            "acte": [
                '{"dateImmatriculation": "2024-01-05"}',
                "{}",
                '{"dateImmatriculation": "2024-03-05"}',
            ],
            "parutionavisprecedent": ["", avis_precedent, ""],
        }
    ).to_csv(raw, sep=";", index=False)

    df = process_creations(str(raw), chunk_size=100)

    # A11 est annulée par A20, et l'avis d'annulation A20 est lui-même exclu.
    # Seule la création A30 subsiste.
    assert df["id_annonce"].tolist() == ["A30"]


# RADIATIONS_CONFIG.post_processing_queries


@pytest.fixture
def radiations_db():
    conn = sqlite3.connect(":memory:")
    # Les tables BODACC sont créées depuis leur DDL de production pour que le
    # test échoue si un post-traitement interroge une colonne qui n'existe pas.
    for config in (
        RADIATIONS_CONFIG,
        CREATIONS_CONFIG,
        ANNONCES_COUPLES_CONFIG,
    ):
        conn.executescript(config.table_ddl)
    conn.executescript(
        """
        CREATE TABLE unite_legale (
            siren TEXT,
            nature_juridique_unite_legale TEXT,
            etat_administratif_unite_legale TEXT
        );
        CREATE TABLE etablissement (
            siren TEXT,
            etat_administratif_etablissement TEXT,
            date_creation DATE,
            date_debut_activite DATE
        );
        CREATE TABLE immatriculation (siren TEXT, date_immatriculation DATE);
        """
    )
    yield conn
    conn.close()


def _run_post_processing(conn) -> dict[str, tuple[int, str]]:
    for query in RADIATIONS_CONFIG.post_processing_queries:
        conn.execute(query)
    return {
        siren: (visibility, reason)
        for siren, visibility, reason in conn.execute(
            "SELECT siren, visibility, visibility_reason FROM bodacc_radiations"
        )
    }


def test_post_processing_accumulates_visibility_reasons(radiations_db):
    radiations_db.executescript(
        """
        INSERT INTO bodacc_radiations (siren, id_annonce, est_radie, date)
            VALUES ('111111111', 'A1', 1, '2020-01-01');
        INSERT INTO unite_legale VALUES ('111111111', '1000', 'A');
        INSERT INTO etablissement VALUES ('111111111', 'A', '2021-05-01', '2021-05-01');
        INSERT INTO bodacc_creations (siren, date) VALUES ('111111111', '2021-06-01');
        """
    )

    visibility, reason = _run_post_processing(radiations_db)["111111111"]

    assert visibility == 0
    assert reason == (
        "ei_active_on_sirene"
        " | ei_with_new_etab_since_radiation"
        " | ei_with_bodacc_creation_since_radiation"
    )


def test_post_processing_single_reason_has_no_leading_separator(radiations_db):
    # L'unité légale est cessée dans SIRENE : seule la 2e requête doit matcher.
    radiations_db.executescript(
        """
        INSERT INTO bodacc_radiations (siren, id_annonce, est_radie, date)
            VALUES ('222222222', 'A2', 1, '2020-01-01');
        INSERT INTO unite_legale VALUES ('222222222', '1000', 'C');
        INSERT INTO etablissement VALUES ('222222222', 'C', '2021-05-01', '2021-05-01');
        """
    )

    visibility, reason = _run_post_processing(radiations_db)["222222222"]

    assert visibility == 0
    assert reason == "ei_with_new_etab_since_radiation"


def test_post_processing_keeps_unmatched_radiation_visible(radiations_db):
    radiations_db.executescript(
        """
        INSERT INTO bodacc_radiations (siren, id_annonce, est_radie, date)
            VALUES ('333333333', 'A3', 1, '2020-01-01');
        INSERT INTO unite_legale VALUES ('333333333', '1000', 'C');
        INSERT INTO etablissement VALUES ('333333333', 'C', '2019-01-01', '2019-01-01');
        """
    )

    visibility, reason = _run_post_processing(radiations_db)["333333333"]

    assert visibility == 1
    assert reason == "visible_by_default"


# normalize_greffe()


@pytest.mark.parametrize(
    "input, expected",
    [
        ("Aix-en-Provence", "aix en provence"),
        ("AIX EN PROVENCE", "aix en provence"),
        ("ANGOULEME", "angouleme"),
        ("Alençon", "alencon"),
        ("  Saverne  ", "saverne"),
        ("", None),
        (None, None),
        ("---", None),
    ],
)
def test_normalize_greffe(input, expected):
    assert normalize_greffe(input) == expected


def test_normalize_greffe_handles_nan():
    assert normalize_greffe(float("nan")) is None


# post-traitement : transfert de siège, comparaison des communes


# extract_sirens_greffes_from_listepersonnes()


def test_extract_sirens_greffes():
    df = pd.DataFrame(
        {
            "listepersonnes": [
                json.dumps(
                    {
                        "personne": {
                            "numeroImmatriculation": {
                                "numeroIdentification": "841 511 421",
                                "nomGreffeImmat": "Aix-en-Provence",
                            }
                        }
                    }
                )
            ]
        }
    )

    result = extract_sirens_greffes_from_listepersonnes(df)

    assert result["siren"].tolist() == ["841511421"]
    assert result["greffe"].tolist() == ["aix en provence"]


def test_extract_sirens_greffes_without_greffe():
    # Inscription au répertoire des métiers : pas de greffe d'immatriculation.
    df = pd.DataFrame(
        {
            "listepersonnes": [
                json.dumps(
                    {
                        "personne": {
                            "inscriptionRM": {"numeroIdentificationRM": "111 111 111"}
                        }
                    }
                )
            ]
        }
    )

    result = extract_sirens_greffes_from_listepersonnes(df)

    assert result["siren"].tolist() == ["111111111"]
    assert result["greffe"].isna().all()


# process_annonces()


def _personne(siren, greffe):
    return json.dumps(
        {
            "personne": {
                "numeroImmatriculation": {
                    "numeroIdentification": siren,
                    "nomGreffeImmat": greffe,
                }
            }
        }
    )


def test_process_annonces_keeps_latest_per_siren_and_greffe(tmp_path):
    raw = tmp_path / "annonces-raw.csv"
    pd.DataFrame(
        {
            "listepersonnes": [
                _personne("841 511 421", "Saverne"),
                _personne("841 511 421", "Metz"),
                _personne("841 511 421", "Metz"),
                _personne("222 222 222", "Nantes"),
            ],
            "dateparution": ["2024-10-10", "2024-11-22", "2025-03-01", "2020-01-01"],
        }
    ).to_csv(raw, sep=";", index=False)

    df = process_annonces_couples(str(raw), chunk_size=2)

    couples = {
        (r.siren, r.greffe): r.date_publication for r in df.itertuples(index=False)
    }
    assert couples[("841511421", "saverne")] == pd.Timestamp("2024-10-10")
    assert couples[("841511421", "metz")] == pd.Timestamp("2025-03-01")
    assert couples[("222222222", "nantes")] == pd.Timestamp("2020-01-01")
    assert len(df) == 3


def test_process_annonces_merges_previous_couples(tmp_path):
    raw = tmp_path / "annonces-raw.csv"
    pd.DataFrame(
        {
            "listepersonnes": [
                _personne("841 511 421", "Metz"),
                _personne("333 333 333", "Lyon"),
            ],
            "dateparution": ["2025-03-01", "2025-02-02"],
        }
    ).to_csv(raw, sep=";", index=False)

    previous = tmp_path / "annonces-couples.csv"
    pd.DataFrame(
        {
            "siren": ["841511421", "841511421", "222222222"],
            "greffe": ["saverne", "metz", "nantes"],
            "date_publication": ["2024-10-10", "2024-11-22", "2020-01-01"],
        }
    ).to_csv(previous, index=False)

    df = process_annonces_couples(
        str(raw), chunk_size=2, previous_couples_url=str(previous)
    )

    couples = {
        (r.siren, r.greffe): r.date_publication for r in df.itertuples(index=False)
    }
    # La fenêtre glissante rafraîchit la parution du couple déjà connu..
    assert couples[("841511421", "metz")] == pd.Timestamp("2025-03-01")
    # ..conserve ceux qu'elle ne couvre pas..
    assert couples[("841511421", "saverne")] == pd.Timestamp("2024-10-10")
    assert couples[("222222222", "nantes")] == pd.Timestamp("2020-01-01")
    # ..et ajoute les nouveaux.
    assert couples[("333333333", "lyon")] == pd.Timestamp("2025-02-02")
    assert len(df) == 4


def test_process_annonces_keeps_couples_with_unparsable_dates(tmp_path):
    raw = tmp_path / "annonces-raw.csv"
    pd.DataFrame(
        {
            "listepersonnes": [_personne("841 511 421", "Metz")],
            "dateparution": ["pas une date"],
        }
    ).to_csv(raw, sep=";", index=False)

    df = process_annonces_couples(str(raw), chunk_size=2)

    # La date illisible devient nulle : le couple est conservé mais reste inerte,
    # la comparaison avec la date de radiation ne peut jamais être vraie.
    assert len(df) == 1
    assert df["date_publication"].isna().all()


# construction des URLs de téléchargement


def test_build_annonces_couples_url_restricts_to_a_rolling_window():
    assert "dateparution+%3E%3D+now%28months%3D-3%29" in build_annonces_couples_url(3)
    assert "now" not in build_annonces_couples_url(None)


def test_download_urls_only_select_the_processed_columns():
    for name, params in BODACC_CONFIG.files_to_download.items():
        assert "select=" in params["url"], name


# post-traitement : transferts de sièges


def _insert_radiation(conn, siren, greffe, date_publication):
    conn.execute(
        "INSERT INTO bodacc_radiations "
        "(siren, id_annonce, est_radie, greffe, date_publication) VALUES (?,?,1,?,?)",
        (siren, f"A-{siren}", greffe, date_publication),
    )


def _insert_annonce(conn, siren, greffe, date_publication):
    conn.execute(
        "INSERT INTO bodacc_annonces_couples (siren, greffe, date_publication) VALUES (?,?,?)",
        (siren, greffe, date_publication),
    )


def test_transfert_de_siege_hidden_when_later_annonce_elsewhere(radiations_db):
    # AUTOSPACE : radiée par Saverne, réimmatriculée à Metz un mois plus tard.
    _insert_radiation(radiations_db, "841511421", "saverne", "2024-10-10")
    _insert_annonce(radiations_db, "841511421", "metz", "2024-11-22")

    visibility, reason = _run_post_processing(radiations_db)["841511421"]

    assert visibility == 0
    assert reason == "transfert_de_siege"


def test_radiation_reelle_when_nothing_follows(radiations_db):
    _insert_radiation(radiations_db, "222222222", "metz", "2020-06-01")

    visibility, reason = _run_post_processing(radiations_db)["222222222"]

    assert visibility == 1
    assert reason == "visible_by_default"


def test_radiation_reelle_when_later_annonces_same_greffe(radiations_db):
    # Clôture de liquidation ou dépôt de comptes tardif : même greffe, la
    # radiation reste réelle.
    _insert_radiation(radiations_db, "333333333", "metz", "2020-06-01")
    _insert_annonce(radiations_db, "333333333", "metz", "2021-02-01")
    _insert_annonce(radiations_db, "333333333", "metz", "2022-09-01")

    assert _run_post_processing(radiations_db)["333333333"][0] == 1


def test_radiation_reelle_when_other_greffe_is_anterior(radiations_db):
    # L'entreprise a transféré son siège puis est morte à destination : l'annonce
    # d'un autre greffe précède la radiation, elle ne la disqualifie pas.
    _insert_radiation(radiations_db, "444444444", "nantes", "2020-06-01")
    _insert_annonce(radiations_db, "444444444", "rennes", "2015-01-01")

    assert _run_post_processing(radiations_db)["444444444"][0] == 1


def test_transfert_de_siege_ignores_radiation_without_greffe(radiations_db):
    _insert_radiation(radiations_db, "555555555", None, "2020-06-01")
    _insert_annonce(radiations_db, "555555555", "bayonne", "2021-01-01")

    assert _run_post_processing(radiations_db)["555555555"][0] == 1
