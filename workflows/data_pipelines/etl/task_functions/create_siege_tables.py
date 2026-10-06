import logging
import sqlite3

from airflow.sdk import task

from data_pipelines_annuaire.config import (
    RNE_DATABASE_LOCATION,
    SIRENE_DATABASE_LOCATION,
)
from data_pipelines_annuaire.helpers import SqliteClient
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.queries.siege import (
    create_index_siege_etablissement,
    update_siege_etablissement_with_rne_data_query,
    update_siege_fields_in_etablissement,
)

logger = logging.getLogger(__name__)


@task
def update_siege_fields_in_etablissement_table():
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(update_siege_fields_in_etablissement)
    sqlite_client.execute(create_index_siege_etablissement)
    for row in sqlite_client.execute(
        "SELECT COUNT(*) FROM etablissement WHERE est_siege = 'true'"
    ):
        logger.info(f"************ {row} etablissements are marked as siege!")
    for row in sqlite_client.execute(
        "SELECT COUNT(*) FROM etablissement WHERE ancien_siege = 'true'"
    ):
        logger.info(f"************ {row} etablissements are marked as ancien siege!")
    sqlite_client.commit_and_close_conn()


@task
def add_rne_data_to_siege_etablissement():
    # Connect to the first database
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)

    # Attach the RNE database
    sqlite_client_siren.connect_to_another_db(RNE_DATABASE_LOCATION, "db_rne")

    try:
        sqlite_client_siren.execute(update_siege_etablissement_with_rne_data_query)

        sqlite_client_siren.db_conn.commit()
        sqlite_client_siren.detach_database("db_rne")
        sqlite_client_siren.commit_and_close_conn()

    except sqlite3.IntegrityError as e:
        # Log the error and problematic siren values
        logger.error(f"IntegrityError: {e}")
        problematic_sirens = e.args[0].split(": ")[1].split(", ")
        logger.error(f"Problematic Sirens: {problematic_sirens}")

    except Exception as error:
        # Handle other exceptions if needed
        logger.error(f"An unexpected error occurred: {error}")
        raise
