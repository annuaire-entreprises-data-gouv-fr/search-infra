import logging
from datetime import UTC, datetime, timedelta

from airflow.sdk import get_current_context

from data_pipelines_annuaire.config import TCHAP_BOT_SUPPORT_ROOM_ID
from data_pipelines_annuaire.helpers import Notification, tchap
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.config import (
    GRIST_COLUMN_NAMES,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.demarche_numerique import (
    DemarcheNumeriqueProcessor,
)

logger = logging.getLogger(__name__)

NAMES_CONFIRMATION_DELAY_DAYS = 4
NON_LEGACY_OPTIONAL_COLUMNS = {"motif", "prenoms", "date_de_naissance"}
NON_LEGACY_REQUIRED_COLUMNS = [
    column for column in GRIST_COLUMN_NAMES if column not in NON_LEGACY_OPTIONAL_COLUMNS
]
# Legacy rows come from the former Siren protégés list and had no dossier
LEGACY_REQUIRED_COLUMNS = ["siren", "date_ajout", "legacy"]


def notify_names_not_verified() -> None:
    """
    Send a notification on the support Tchap channel all the validated dossiers
    added to Grist at least N days ago whose nom and prénoms are still not
    verified by an instructeur.
    """
    processor = DemarcheNumeriqueProcessor()
    df = processor.grist_table.read()
    oldest_date_ajout = (
        datetime.now(tz=UTC).date() - timedelta(days=NAMES_CONFIRMATION_DELAY_DAYS)
    ).isoformat()
    # Legacy rows don't need to be verified
    not_verified = df[
        (df["siren"].str.strip() != "")
        & (df["legacy"].str.strip().str.lower() == "false")
        & (df["ortographe_verifiee"].str.strip().str.lower() == "false")
        & (df["date_ajout"].str.strip() <= oldest_date_ajout)
    ].sort_values("date_ajout")
    logger.info(
        f"{len(not_verified)} dossiers with names not verified for at least "
        f"{NAMES_CONFIRMATION_DELAY_DAYS} days."
    )
    header = f"airflow : {get_current_context()['dag'].dag_id}"
    if not_verified.empty:
        tchap.send_message(
            f"<p>{Notification.Status.SUCCESS.value} {header}</p>"
            f"<p>Tous les dossiers ajoutés dans Grist depuis au moins "
            f"{NAMES_CONFIRMATION_DELAY_DAYS} jours ont été vérifiés.</p>",
            "notice",
            room_id=TCHAP_BOT_SUPPORT_ROOM_ID,
        )
        return
    items = "".join(
        f"<li>{processor.dossier_link(int(number))} (ajouté le {date_ajout})</li>"
        for number, date_ajout in zip(
            not_verified["numero_dossier"].str.strip(),
            not_verified["date_ajout"].str.strip(),
        )
    )
    message = (
        f"<p>{Notification.Status.WARNING.value} {header}</p>"
        f"<p>{len(not_verified)} dossier(s) ajouté(s) dans Grist depuis au moins "
        f"{NAMES_CONFIRMATION_DELAY_DAYS} jours sans confirmation par "
        f"l'instructeur que les nom et prénoms correspondent à ceux de "
        f"l'Annuaire :</p>"
        f"<ul>{items}</ul>"
    )
    tchap.send_message(message, room_id=TCHAP_BOT_SUPPORT_ROOM_ID)


def notify_missing_fields() -> None:
    """
    Send a notification on the support Tchap channel listing the Grist rows
    with a siren but an empty required column.
    """
    processor = DemarcheNumeriqueProcessor()
    df = processor.grist_table.read()
    items = []
    # The CSV export has the same order of row than Grist
    for line, row in df.iterrows():
        if not row["siren"].strip():
            continue
        is_legacy = row["legacy"].strip().lower() == "true"
        required = LEGACY_REQUIRED_COLUMNS if is_legacy else NON_LEGACY_REQUIRED_COLUMNS
        missing = [column for column in required if not row[column].strip()]
        if not missing:
            continue
        labels = [f"ligne {int(str(line)) + 1}", f"siren {row['siren'].strip()}"]
        number = row["numero_dossier"].strip()
        if number.isdigit():
            labels.append(processor.dossier_link(int(number)))
        items.append(f"<li>{', '.join(labels)} : {', '.join(missing)}</li>")
    logger.info(f"{len(items)} Grist rows with missing required fields.")
    header = f"airflow : {get_current_context()['dag'].dag_id}"
    if not items:
        tchap.send_message(
            f"<p>{Notification.Status.SUCCESS.value} {header}</p>"
            f"<p>Toutes les lignes de Grist sont bien remplies.</p>",
            "notice",
            room_id=TCHAP_BOT_SUPPORT_ROOM_ID,
        )
        return
    message = (
        f"<p>{Notification.Status.WARNING.value} {header}</p>"
        f"<p>{len(items)} ligne(s) de Grist avec des champs obligatoires manquants :</p>"
        f"<ul>{''.join(items)}</ul>"
    )
    tchap.send_message(message, room_id=TCHAP_BOT_SUPPORT_ROOM_ID)
