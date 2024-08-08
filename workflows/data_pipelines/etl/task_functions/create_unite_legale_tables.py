import logging
import sqlite3
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.etl.data_fetch_clean.unite_legale\
    import (
    preprocess_historique_unite_legale_data,
    preprocess_unite_legale_data,
    process_anciens_sieges_flux,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.queries.unite_legale\
    import (
    create_table_date_fermeture_unite_legale_query,
    create_table_flux_unite_legale_query,
    create_table_historique_unite_legale_query,
    create_table_anciens_sieges_query,
    create_table_unite_legale_query,
    delete_current_siege_from_anciens_sieges_query,
    insert_date_fermeture_unite_legale_query,
    replace_table_unite_legale_query,
    insert_remaining_rne_data_into_main_table_query,
    update_main_table_fields_with_rne_data_query,
)
# fmt: on
from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.helpers import (
    create_index,
    create_table_model,
    create_unique_index,
    execute_query,
    get_table_count,
)
from dag_datalake_sirene.config import AIRFLOW_ETL_DATA_DIR
from dag_datalake_sirene.config import (
    SIRENE_DATABASE_LOCATION,
    RNE_DATABASE_LOCATION,
)


def create_table(query, table_name, index, sirene_file_type):
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=query,
        create_index_func=create_unique_index,
        index_name=index,
        index_column="siren",
    )
    for df_unite_legale in preprocess_unite_legale_data(
        AIRFLOW_ETL_DATA_DIR, sirene_file_type
    ):
        df_unite_legale.to_sql(
            table_name, sqlite_client.db_conn, if_exists="append", index=False
        )
        for row in sqlite_client.execute(get_table_count(table_name)):
            logging.debug(
                f"************ {row} total records have been added "
                f"to the {table_name} table!"
            )

    del df_unite_legale

    for count_unites_legales in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {count_unites_legales} total records have been added to the "
            f"{table_name} table!"
        )
    sqlite_client.commit_and_close_conn()
    return count_unites_legales[0]


def create_unite_legale_table(**kwargs):
    counts = create_table(
        create_table_unite_legale_query,
        "unite_legale",
        "index_siren",
        "stock",
    )
    kwargs["ti"].xcom_push(key="count_unites_legales", value=counts)


def create_flux_unite_legale_table(**kwargs):
    counts = create_table(
        create_table_flux_unite_legale_query,
        "flux_unite_legale",
        "index_flux_siren",
        "flux",
    )
    kwargs["ti"].xcom_push(key="count_flux_unites_legales", value=counts)


def add_ancien_siege_flux_data(**kwargs):
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)

    table_name = "anciens_sieges"

    for df_unite_legale in process_anciens_sieges_flux(AIRFLOW_ETL_DATA_DIR):
        df_unite_legale.to_sql(
            table_name, sqlite_client.db_conn, if_exists="append", index=False
        )
        for row in sqlite_client.execute(get_table_count(table_name)):
            logging.info(
                f"************ {row} total records have been added "
                f"to the {table_name} table!"
            )
    del df_unite_legale
    sqlite_client.execute(delete_current_siege_from_anciens_sieges_query)
    for row in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {row} final records have been added "
            f"to the {table_name} table!"
        )
    sqlite_client.commit_and_close_conn()


def replace_unite_legale_table():
    return execute_query(
        query=replace_table_unite_legale_query,
    )


def add_rne_data_to_unite_legale_table(**kwargs):

    try:
        # Connect to the main database (SIRENE)
        sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)

        # Attach the RNE database
        sqlite_client_siren.connect_to_another_db(RNE_DATABASE_LOCATION, "db_rne")

        sqlite_client_siren.execute(update_main_table_fields_with_rne_data_query)
        # (handling duplicates with INSERT OR IGNORE)
        sqlite_client_siren.execute(insert_remaining_rne_data_into_main_table_query)

        # Commit changes before detaching
        sqlite_client_siren.db_conn.commit()
        sqlite_client_siren.detach_database("db_rne")
        sqlite_client_siren.commit_and_close_conn()

    except sqlite3.IntegrityError as e:
        # Log the error and problematic siren values
        logging.error(f"IntegrityError: {e}")
        problematic_sirens = e.args[0].split(": ")[1].split(", ")
        logging.error(f"Problematic Sirens: {problematic_sirens}")
        raise e

    except Exception as e:
        # Handle other exceptions if needed
        logging.error(f"An unexpected error occurred: {e}")
        raise e


def create_historique_unite_legale_tables(**kwargs):

    sqlite_client = create_table_model(
        table_name="anciens_sieges",
        create_table_query=create_table_anciens_sieges_query,
        create_index_func=create_index,
        index_name="index_anciens_sieges",
        index_column="siret",
    )
    sqlite_client.commit_and_close_conn()

    table_name = "historique_unite_legale"
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=create_table_historique_unite_legale_query,
        create_index_func=create_index,
        index_name="index_historique_siren",
        index_column="siren",
    )

    for (
        df_hist_unite_legale,
        df_anciens_sieges,
    ) in preprocess_historique_unite_legale_data(
        AIRFLOW_ETL_DATA_DIR,
    ):
        df_hist_unite_legale.to_sql(
            table_name, sqlite_client.db_conn, if_exists="append", index=False
        )

        df_anciens_sieges.to_sql(
            "anciens_sieges",
            sqlite_client.db_conn,
            if_exists="append",
            index=False,
        )

        for row in sqlite_client.execute(get_table_count(table_name)):
            logging.debug(
                f"************ {row} total records have been added "
                f"to the {table_name} table!"
            )
        for row in sqlite_client.execute(get_table_count("anciens_sieges")):
            logging.debug(
                f"************ {row} total records have been added "
                f"to the anciens_sieges table!"
            )

    del df_hist_unite_legale

    for count_unites_legales in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {count_unites_legales} total records have been added to the "
            f"{table_name} table!"
        )
    sqlite_client.commit_and_close_conn()
    kwargs["ti"].xcom_push(
        key="count_historique_unites_legales", value=count_unites_legales
    )


def create_date_fermeture_unite_legale_table(**kwargs):
    table_name = "date_fermeture_unite_legale"
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=create_table_date_fermeture_unite_legale_query,
        create_index_func=create_unique_index,
        index_name="index_date_fermeture_siren",
        index_column="siren",
    )

    for count_unites_legales in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {count_unites_legales} total records have been added to the "
            f"{table_name} table!"
        )
    sqlite_client.commit_and_close_conn()
    kwargs["ti"].xcom_push(
        key="count_date_fermeture_unites_legales", value=count_unites_legales
    )


def insert_date_fermeture_unite_legale(**kwargs):
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(insert_date_fermeture_unite_legale_query)
    sqlite_client.commit_and_close_conn()
