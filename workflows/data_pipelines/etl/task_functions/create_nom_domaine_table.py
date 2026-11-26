import logging

from airflow.sdk import task

from data_pipelines_annuaire.config import (
    RNE_DATABASE_LOCATION,
    SIRENE_DATABASE_LOCATION,
)
from data_pipelines_annuaire.helpers.sqlite_client import SqliteClient
from data_pipelines_annuaire.workflows.data_pipelines.etl.data_fetch_clean.nom_domaine import (
    clean_noms_de_domaine,
)

logger = logging.getLogger(__name__)


@task
def create_nom_domaine_table():
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.connect_to_another_db(RNE_DATABASE_LOCATION, "db_rne")
    rows = sqlite_client.execute(
        "SELECT siren, nom_domaine FROM db_rne.nom_domaine"
    ).fetchall()
    sqlite_client.detach_database("db_rne")
    noms_de_domaine = clean_noms_de_domaine(rows)
    cleaned_count = sum(
        nom_domaine is not None for _, _, nom_domaine in noms_de_domaine
    )
    logger.info(
        f"{cleaned_count} RNE entries out of {len(rows)} have a cleaned domain name"
    )

    sqlite_client.drop_table("nom_domaine")
    sqlite_client.execute(
        "CREATE TABLE nom_domaine (siren TEXT, nom_domaine_original TEXT, nom_domaine TEXT)"
    )
    sqlite_client.execute_many(
        "INSERT INTO nom_domaine (siren, nom_domaine_original, nom_domaine) VALUES (?, ?, ?)",
        noms_de_domaine,
    )
    sqlite_client.execute("CREATE INDEX idx_siren_nom_domaine ON nom_domaine (siren)")
    sqlite_client.commit_and_close_conn()
