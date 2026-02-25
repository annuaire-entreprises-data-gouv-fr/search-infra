import logging
import sqlite3

from airflow.sdk import task

from data_pipelines_annuaire.config import (
    RNE_DATABASE_LOCATION,
    SIRENE_DATABASE_LOCATION,
)
from data_pipelines_annuaire.helpers import SqliteClient
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.helpers import (
    create_index,
    create_table_model,
    get_table_count,
)
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.queries.siege import (
    create_table_ancien_siege_query,
    create_table_siege_query,
    delete_current_siege_from_ancien_siege_query,
    populate_ancien_siege_from_historique_query,
    populate_table_siege_query,
    update_est_siege_in_etablissement,
    update_siege_table_fields_with_rne_data_query,
)


@task
def create_siege_table():
    table_name = "siege"
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=create_table_siege_query,
        create_index_func=create_index,
        index_name=f"index_{table_name}_siren",
        index_column="siren",
    )
    sqlite_client.execute(
        create_index(f"index_{table_name}_siret", table_name, "siret")
    )
    sqlite_client.execute(populate_table_siege_query)
    sqlite_client.execute(update_est_siege_in_etablissement)
    for row in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {row} total records have been added "
            f"to the {table_name} table!"
        )
    sqlite_client.commit_and_close_conn()


@task
def create_ancien_siege_table():
    """Populate ancien_siege from historique_unite_legale where date_fin_periode IS NOT NULL."""
    table_name = "ancien_siege"
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=create_table_ancien_siege_query,
        create_index_func=create_index,
        index_name=f"index_{table_name}_siret",
        index_column="siret",
    )
    # Populate ancien_siege from historique_unite_legale (which was enriched with flux)
    # Only keep historical sieges (date_fin_periode IS NOT NULL)
    sqlite_client.execute(populate_ancien_siege_from_historique_query)
    # And remove any ancien siege that may have become siege again since then
    sqlite_client.execute(delete_current_siege_from_ancien_siege_query)

    for row in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {row} total records have been added "
            f"to the {table_name} table!"
        )
    sqlite_client.commit_and_close_conn()


@task
def add_rne_data_to_siege_table():
    # Connect to the first database
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)

    # Attach the RNE database
    sqlite_client_siren.connect_to_another_db(RNE_DATABASE_LOCATION, "db_rne")

    try:
        # Update existing rows in main siege table based on siren from rne.siege
        sqlite_client_siren.execute(update_siege_table_fields_with_rne_data_query)

        sqlite_client_siren.db_conn.commit()
        sqlite_client_siren.detach_database("db_rne")
        sqlite_client_siren.commit_and_close_conn()

    except sqlite3.IntegrityError as e:
        # Log the error and problematic siren values
        logging.error(f"IntegrityError: {e}")
        problematic_sirens = e.args[0].split(": ")[1].split(", ")
        logging.error(f"Problematic Sirens: {problematic_sirens}")

    except Exception as error:
        # Handle other exceptions if needed
        logging.error(f"An unexpected error occurred: {error}")
        raise error
