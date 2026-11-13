from unittest.mock import MagicMock

import pandas as pd
import pytest

from data_pipelines_annuaire.helpers.demarche_numerique import (
    Champ,
    Demarche,
    Dossier,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.config import (
    MOTIF_MAX_LENGTH,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.demarche_numerique.processor import (
    DemarcheNumeriqueProcessor,
)


@pytest.fixture(autouse=True)
def airflow_variables(monkeypatch):
    # Demarche creates its API client, which reads the token, when built.
    monkeypatch.setattr(
        "data_pipelines_annuaire.helpers.demarche_numerique.Variable.get",
        lambda name: "test",
    )


SIRET = {"stringValue": "35600000000048", "etablissement": {"siret": "35600000000048"}}


TRAITEMENTS = [
    {
        "dateTraitement": "2026-11-01T10:15:00+01:00",
        "emailAgentTraitant": "instructeur@example.fr",
    },
    {
        "dateTraitement": "2026-10-20T09:30:00+02:00",
        "emailAgentTraitant": None,
    },
]


def dossier(
    champs=None,
    demandeur=None,
    label_case=str,
    traitements=TRAITEMENTS,
):
    # Labels and shape of the real form.
    overrides = champs or {}
    if demandeur is None:
        demandeur = {"nom": "Dupont", "prenom": " jean  pierre "}
    champs = {
        "Numéro SIRET": {"stringValue": "356 000 000"},
        "Justificatif d'identité": {"stringValue": ""},
        "Date de naissance": {"stringValue": "15 janvier 1970", "date": "1970-01-15"},
        "Motif de la demande": {"stringValue": "Menaces\u202e reçues\n\npar courrier"},
    }
    champs.update(overrides)
    return Dossier(
        Demarche(1, "instructeur"),
        {
            "id": "RG9zc2llci0x",
            "number": 1,
            "dateDepot": "2026-10-20T09:30:00+02:00",
            "dateTraitement": "2026-11-01T10:15:00+01:00",
            "traitements": traitements,
            "demandeur": demandeur,
            "champs": [
                {"label": label_case(label), **champ}
                for label, champ in champs.items()
                if champ is not None
            ],
        },
    )


def test_parse_dossier():
    assert DemarcheNumeriqueProcessor.parse_dossier(dossier()) == (
        {
            "siren": "356000000",
            "prenoms": "JEAN PIERRE",
            "nom": "DUPONT",
            "date_de_naissance": "1970-01-15",
            "motif": "Menaces reçues\n\npar courrier",
            "truncated": False,
            "date_validation_demande": "2026-11-01",
            "instructeur": "instructeur@example.fr",
            "date_depot_demande": "2026-10-20",
            "numero_dossier": "1",
            "legacy": False,
        },
        None,
    )


def test_parse_dossier_siret_champ():
    record, _ = DemarcheNumeriqueProcessor.parse_dossier(
        dossier({"Numéro SIRET": SIRET})
    )

    assert record["siren"] == "356000000"


def test_parse_dossier_long_motif():
    record, _ = DemarcheNumeriqueProcessor.parse_dossier(
        dossier({"Motif de la demande": {"stringValue": "a" * (MOTIF_MAX_LENGTH + 1)}})
    )

    assert record["motif"] == "a" * MOTIF_MAX_LENGTH
    assert record["truncated"] is True


def test_parse_dossier_label_case_insensitive():
    record, _ = DemarcheNumeriqueProcessor.parse_dossier(
        dossier(label_case=lambda label: label.upper() + " ")
    )

    assert record is not None


@pytest.mark.parametrize(
    "champs, demandeur, reason",
    [
        ({"Numéro SIRET": None}, None, "champ 'Numéro SIRET' absent ou vide"),
        (
            {"Numéro SIRET": {"stringValue": "1234"}},
            None,
            "champ 'Numéro SIRET' invalide",
        ),
        (
            {"Date de naissance": {"stringValue": "  "}},
            None,
            "champ 'Date de naissance' absent ou vide",
        ),
        (
            {"Date de naissance": {"stringValue": "janvier"}},
            None,
            "champ 'Date de naissance' invalide",
        ),
        (
            {"Motif de la demande": None},
            None,
            "champ 'Motif de la demande' absent ou vide",
        ),
        (
            {"Motif de la demande": {"stringValue": "\u200b\u202e"}},
            None,
            "champ 'Motif de la demande' invalide",
        ),
        (None, {"nom": "Dupont", "prenom": " "}, "nom ou prénom du demandeur absent"),
        # Démarche filed on behalf of a company: no PersonnePhysique fields.
        (None, {}, "nom ou prénom du demandeur absent"),
    ],
)
def test_parse_dossier_invalid(champs, demandeur, reason):
    assert DemarcheNumeriqueProcessor.parse_dossier(dossier(champs, demandeur)) == (
        None,
        reason,
    )


def processor_with_context(monkeypatch):
    context = {"ti": MagicMock()}
    monkeypatch.setattr(
        "data_pipelines_annuaire.workflows.data_pipelines.siren_protege."
        "demarche_numerique.processor.get_current_context",
        lambda: context,
    )
    processor = DemarcheNumeriqueProcessor.__new__(DemarcheNumeriqueProcessor)
    processor.demarche = Demarche(1, "instructeur")
    return processor, context["ti"]


def test_notify_dossiers_with_issues_nothing(monkeypatch):
    processor, ti = processor_with_context(monkeypatch)

    processor.notify_dossiers_with_issues(
        {"to_review": [], "failed": [], "truncated_texts": []}
    )

    ti.xcom_push.assert_not_called()


def test_notify_dossiers_with_issues_reports_both(monkeypatch):
    processor, ti = processor_with_context(monkeypatch)

    with pytest.raises(ValueError) as error:
        processor.notify_dossiers_with_issues(
            {
                "to_review": [{"number": 12, "reason": "SIREN introuvable"}],
                "failed": [{"number": 56, "reason": "archivage"}],
                "truncated_texts": [34],
            }
        )

    message = str(error.value)
    assert "dossier 12</a> : SIREN introuvable" in message
    assert f"texte tronqué à {MOTIF_MAX_LENGTH} caractères" in message
    assert "dossier 34</a>" in message
    assert "dossier 56</a> : archivage" in message
    ti.xcom_push.assert_called_once()


def test_dossier_from_api():
    result = dossier({"Numéro SIRET": SIRET})

    assert result.date_depot == "2026-10-20"
    assert result.date_traitement == "2026-11-01"
    assert (result.demandeur_nom, result.demandeur_prenom) == (
        "Dupont",
        " jean  pierre ",
    )
    assert result.url == "https://demarche.numerique.gouv.fr/procedures/1/dossiers/1"
    assert result.champ("numéro siret") == Champ(
        label="Numéro SIRET", value="35600000000048", siret="35600000000048"
    )
    assert result.champ("Date de naissance").date == "1970-01-15"
    assert result.champ("Justificatif d'identité").value == ""
    assert result.champ("Inconnu") is None


@pytest.mark.parametrize(
    "traitements, email",
    [
        (TRAITEMENTS, "instructeur@example.fr"),
        (TRAITEMENTS[::-1], "instructeur@example.fr"),
        ([TRAITEMENTS[1]], ""),
        ([], ""),
    ],
)
def test_dossier_email_instructeur(traitements, email):
    assert dossier(traitements=traitements).email_instructeur == email


def test_accepted_dossiers_paginates(monkeypatch):
    pages = iter(
        [
            {
                "nodes": [{"id": "a", "number": 1}],
                "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
            },
            {
                "nodes": [{"id": "b", "number": 2}],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            },
        ]
    )
    calls = []
    demarche = Demarche(1, "instructeur")

    def query(query, variables):
        calls.append(variables["after"])
        return {"demarche": {"dossiers": next(pages)}}

    monkeypatch.setattr(demarche._client, "query", query)

    assert [d.number for d in demarche.accepted_dossiers()] == [1, 2]
    assert calls == [None, "c1"]


def test_dossier_archive_errors(monkeypatch):
    result = dossier()
    monkeypatch.setattr(
        result.demarche._client,
        "query",
        lambda query, variables: {
            "dossierArchiver": {"errors": [{"message": "déjà archivé"}]}
        },
    )

    with pytest.raises(RuntimeError, match="déjà archivé"):
        result.archive()
    assert result.archived is False


def test_get_integrated_dossier_numbers():
    processor = DemarcheNumeriqueProcessor.__new__(DemarcheNumeriqueProcessor)
    processor.grist_table = MagicMock()
    processor.grist_table.read.return_value = pd.DataFrame(
        {"numero_dossier": ["35342341", " 7 ", ""]}
    )

    assert processor.get_integrated_dossier_numbers() == {"35342341", "7", ""}


def test_get_integrated_dossier_numbers_missing_column():
    processor = DemarcheNumeriqueProcessor.__new__(DemarcheNumeriqueProcessor)
    processor.grist_table = MagicMock()
    processor.grist_table.read.return_value = pd.DataFrame({"siren": ["1"]})

    with pytest.raises(ValueError, match="numero_dossier"):
        processor.get_integrated_dossier_numbers()


def numbered_dossier(number, champs=None):
    result = dossier(champs)
    result.number = number
    result.archive = MagicMock()
    return result


def processor_integrating(monkeypatch, dossiers, integrated=()):
    processor, ti = processor_with_context(monkeypatch)
    processor.demarche = MagicMock()
    processor.demarche.accepted_dossiers.return_value = dossiers
    processor.grist_table = MagicMock()
    processor.grist_table.read.return_value = pd.DataFrame(
        {"numero_dossier": list(integrated)}, dtype=str
    )
    monkeypatch.setattr(processor, "get_nature_juridique", lambda siren: "5710")
    return processor, ti


def appended_numbers(processor):
    return [
        call.args[0][0]["numero_dossier"]
        for call in processor.grist_table.append.call_args_list
    ]


def test_process_accepted_dossiers_one_at_a_time(monkeypatch):
    first, second = numbered_dossier(1), numbered_dossier(2)
    processor, ti = processor_integrating(monkeypatch, [first, second])

    result = processor.process_accepted_dossiers()

    assert appended_numbers(processor) == ["1", "2"]
    first.archive.assert_called_once()
    second.archive.assert_called_once()
    assert result == {"to_review": [], "failed": [], "truncated_texts": []}
    ti.xcom_push.assert_called_once()


def test_process_accepted_dossiers_failed_copy_not_archived(monkeypatch):
    first, second = numbered_dossier(1), numbered_dossier(2)
    processor, _ = processor_integrating(monkeypatch, [first, second])
    processor.grist_table.append.side_effect = [RuntimeError("Grist"), None]

    result = processor.process_accepted_dossiers()

    first.archive.assert_not_called()
    second.archive.assert_called_once()
    assert result["failed"] == [{"number": 1, "reason": "ajout dans Grist"}]


def test_process_accepted_dossiers_already_copied_only_archived(monkeypatch):
    already = numbered_dossier(1)
    processor, _ = processor_integrating(monkeypatch, [already], integrated=["1"])

    result = processor.process_accepted_dossiers()

    processor.grist_table.append.assert_not_called()
    already.archive.assert_called_once()
    assert result["failed"] == []


def test_process_accepted_dossiers_failed_archiving(monkeypatch):
    truncated = numbered_dossier(
        1, {"Motif de la demande": {"stringValue": "a" * (MOTIF_MAX_LENGTH + 1)}}
    )
    truncated.archive.side_effect = RuntimeError("API")
    processor, _ = processor_integrating(monkeypatch, [truncated])

    result = processor.process_accepted_dossiers()

    assert appended_numbers(processor) == ["1"]
    assert result["failed"] == [{"number": 1, "reason": "archivage"}]
    assert result["truncated_texts"] == [1]
