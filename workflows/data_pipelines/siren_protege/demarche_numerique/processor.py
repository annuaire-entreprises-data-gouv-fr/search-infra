import logging
import re
from datetime import UTC, datetime
from typing import Any

from airflow.sdk import Variable, get_current_context

from data_pipelines_annuaire.helpers import (
    ApiClient,
    DemarcheNumerique,
    GristTable,
    Notification,
    tchap,
)
from data_pipelines_annuaire.helpers.demarche_numerique import Champ, Dossier
from data_pipelines_annuaire.helpers.utils import sanitize_free_text_for_tabular_format
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.config import (
    GRIST_COLUMN_NAMES,
    GRIST_DOCUMENT_ID_VARIABLE,
    GRIST_TABLE_ID_VARIABLE,
    MOTIF_MAX_LENGTH,
)

logger = logging.getLogger(__name__)


class DemarcheNumeriqueProcessor:
    """
    Copy the accepted dossiers of the Démarche Numérique form to Grist.
    Once copied, change their status to archived.
    """

    MANDATORY_CHAMPS_FIELDS = {
        "siren": "Numéro SIRET",
        "date_de_naissance": "Date de naissance",
        "motif": "Motif de la demande",
    }

    def __init__(self) -> None:
        self.demarche = DemarcheNumerique(
            int(Variable.get("SIREN_PROTEGE_DEMARCHE_NUMBER")),
            Variable.get("DEMARCHE_NUMERIQUE_INSTRUCTEUR_ID"),
        )
        self.grist_table = GristTable(
            Variable.get(GRIST_DOCUMENT_ID_VARIABLE),
            Variable.get(GRIST_TABLE_ID_VARIABLE),
        )
        self.recherche_client = ApiClient(
            base_url="https://recherche-entreprises.api.gouv.fr"
        )

    @staticmethod
    def normalize_name(value: str) -> str:
        return " ".join(value.split()).upper()

    @staticmethod
    def parse_siren(champ: Champ) -> str | None:
        digits = re.sub(r"\s", "", champ.siret or champ.value)
        if re.fullmatch(r"\d{14}", digits):
            digits = digits[:9]
        return digits if re.fullmatch(r"\d{9}", digits) else None

    @classmethod
    def parse_dossier(cls, dossier: Dossier) -> tuple[dict | None, str | None]:
        """Return the Grist record of a dossier or its issue if there is any."""
        # "truncated" is not a Grist column: used to inform if a champ had to be truncated.
        record: dict[str, Any] = {"truncated": False}
        for column, label in cls.MANDATORY_CHAMPS_FIELDS.items():
            champ = dossier.champ(label)
            if champ is None:
                # Labels and types only: the values are personal data.
                available = ", ".join(f"'{c.label}' ({c.type})" for c in dossier.champs)
                logger.warning(
                    f"Dossier {dossier.number}: no champ '{label}', "
                    f"champs of the dossier: {available}"
                )
                return None, f"champ '{label}' absent ou vide"
            if not champ.value.strip():
                logger.warning(
                    f"Dossier {dossier.number}: champ '{label}' ({champ.type}) is empty"
                )
                return None, f"champ '{label}' absent ou vide"
            match column:
                case "siren":
                    value = cls.parse_siren(champ)
                case "date_de_naissance":
                    value = champ.date
                case "motif":
                    value, record["truncated"] = sanitize_free_text_for_tabular_format(
                        champ.value, MOTIF_MAX_LENGTH
                    )
                    value = value or None
            if value is None:
                logger.warning(
                    f"Dossier {dossier.number}: champ '{label}' ({champ.type}) "
                    f"could not be parsed, date set: {champ.date is not None}, "
                    f"SIRET set: {champ.siret is not None}"
                )
                return None, f"champ '{label}' invalide"
            record[column] = value
        if not dossier.demandeur_nom.strip() or not dossier.demandeur_prenom.strip():
            return None, "nom ou prénom du demandeur absent"
        record["prenoms"] = cls.normalize_name(dossier.demandeur_prenom)
        record["nom"] = cls.normalize_name(dossier.demandeur_nom)
        record["date_validation_demande"] = dossier.date_traitement
        record["instructeur"] = dossier.email_instructeur
        record["date_depot_demande"] = dossier.date_depot
        record["numero_dossier"] = str(dossier.number)
        record["legacy"] = False
        return record, None

    def siren_exists(self, siren: str) -> bool:
        results = (
            self.recherche_client.get("/search", params={"q": siren, "minimal": "true"})
            .json()
            .get("results", [])
        )
        return any(r["siren"] == siren for r in results)

    def get_integrated_dossier_numbers(self) -> set[str]:
        df = self.grist_table.read()
        if "numero_dossier" not in df.columns:
            raise ValueError(
                f"Grist table {self.grist_table.table_id} is missing the column "
                "numero_dossier."
            )
        return set(df["numero_dossier"].str.strip())

    def process_accepted_dossiers(self) -> dict[str, list]:
        """
        Copy each accepted dossier to Grist, then archive it, one dossier at a
        time: a dossier in error doesn't block the others, and a dossier is
        never archived unless it has been copied to Grist.

        Returns, without personal data, in XCom:
        - to_review: the dossiers left for a manual review for an expected
          reason (unknown SIREN)
        - invalid: the dossiers whose champs could not be read, with the reason
        - failed: the dossiers where the copy or archiving failed
        - truncated_texts: the dossiers added with a truncated text
        """
        to_review: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        to_integrate: list[tuple[Dossier, dict[str, Any]]] = []

        for dossier in self.demarche.accepted_dossiers():
            record, reason = self.parse_dossier(dossier)
            issues = invalid
            if record is not None:
                issues = to_review
                if not self.siren_exists(record["siren"]):
                    reason = "SIREN introuvable"
            if reason is not None:
                logger.warning(
                    f"Dossier {dossier.number}: left for a manual review ({reason})"
                )
                issues.append({"number": dossier.number, "reason": reason})
            else:
                to_integrate.append((dossier, record))

        logger.info(
            f"{len(to_integrate) + len(to_review) + len(invalid)} accepted dossiers "
            f"not archived: {len(to_integrate)} to integrate, {len(to_review)} to "
            f"review manually, {len(invalid)} unreadable."
        )
        result: dict[str, list] = {
            "to_review": to_review,
            "invalid": invalid,
            "failed": [],
            "truncated_texts": [],
        }
        if not to_integrate:
            return result

        # A dossier already in Grist was copied by a run whose archiving failed:
        # only the archiving is left to do.
        integrated = self.get_integrated_dossier_numbers()
        date_ajout = datetime.now(tz=UTC).date().isoformat()
        added = 0
        for dossier, record in to_integrate:
            step = "ajout dans Grist"
            try:
                if str(dossier.number) in integrated:
                    logger.info(
                        f"Dossier {dossier.number}: already in Grist, archiving only"
                    )
                else:
                    record["date_ajout"] = date_ajout
                    self.grist_table.append(
                        [{column: record[column] for column in GRIST_COLUMN_NAMES}]
                    )
                    added += 1
                    logger.info(f"Dossier {dossier.number}: added to Grist")
                    if record["truncated"]:
                        logger.warning(
                            f"Dossier {dossier.number}: motif truncated to "
                            f"{MOTIF_MAX_LENGTH} characters"
                        )
                        result["truncated_texts"].append(dossier.number)
                step = "archivage"
                dossier.archive()
                logger.info(f"Dossier {dossier.number}: archived")
            except Exception as e:
                logger.error(f"Dossier {dossier.number}, {step} failed: {e}")
                result["failed"].append({"number": dossier.number, "reason": step})

        logger.info(
            f"{added} dossiers added to Grist, {len(result['failed'])} in error."
        )
        if added:
            get_current_context()["ti"].xcom_push(
                key=Notification.notification_xcom_key,
                value=f"{added} dirigeant(s) ajouté(s) dans Grist",
            )
        return result

    def dossier_link(self, number: int) -> str:
        return f'<a href="{self.demarche.dossier_url(number)}">dossier {number}</a>'

    def dossier_items(self, dossiers: list[dict[str, Any]]) -> str:
        return "".join(
            f"<li>{self.dossier_link(d['number'])} : {d['reason']}</li>"
            for d in dossiers
        )

    def notify_dossiers_with_issues(self, result: dict[str, list]) -> None:
        """
        Report on Tchap the dossiers process_accepted_dossiers could not process.

        Expected cases (unknown SIREN, truncated motif) are handled by hand
        in Démarche Numérique: they are sent to Tchap and the task succeeds.
        Unreadable dossiers (form changed, missing champ) and copy or archiving
        errors make the task fail, the failure callback sending the whole report.
        """
        errors = []
        expected = []
        if invalid := result["invalid"]:
            errors.append(
                f"<p>{len(invalid)} demande(s) acceptée(s) illisible(s), non "
                f"intégrée(s) ni archivée(s) :</p>"
                f"<ul>{self.dossier_items(invalid)}</ul>"
            )
        if failed := result["failed"]:
            errors.append(
                f"<p>{len(failed)} demande(s) en erreur, réessayée(s) au prochain "
                f"run (détail dans les logs) :</p>"
                f"<ul>{self.dossier_items(failed)}</ul>"
            )
        if to_review := result["to_review"]:
            expected.append(
                f"<p>{len(to_review)} demande(s) acceptée(s) non intégrée(s) ni "
                f"archivée(s), à traiter manuellement :</p>"
                f"<ul>{self.dossier_items(to_review)}</ul>"
            )
        if truncated := result["truncated_texts"]:
            items = "".join(f"<li>{self.dossier_link(n)}</li>" for n in truncated)
            expected.append(
                f"<p>{len(truncated)} dossier(s) avec un texte tronqué à {MOTIF_MAX_LENGTH} "
                f"caractères dans Grist, texte complet dans le dossier :</p>"
                f"<ul>{items}</ul>"
            )
        logger.info(
            f"{len(to_review)} dossiers to review manually, {len(invalid)} "
            f"unreadable, {len(failed)} in error, {len(truncated)} with a "
            "truncated motif."
        )
        context = get_current_context()
        if errors:
            message = "".join(errors + expected)
            context["ti"].xcom_push(
                key=Notification.notification_xcom_key, value=message
            )
            raise ValueError(message)
        if not expected:
            logger.info("Nothing to notify.")
            return
        message = (
            f"<p>{Notification.Status.WARNING.value} airflow : "
            f"{context['dag'].dag_id}</p>" + "".join(expected)
        )
        logger.info(f"Tchap message: {message}")
        tchap.send_message(message)
