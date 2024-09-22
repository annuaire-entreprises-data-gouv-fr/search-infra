import logging
import os
import shutil


from helpers.sqlite_client import SqliteClient

from helpers.settings import Settings


def create_sqlite_database():
    if os.path.exists(Settings.AIRFLOW_ETL_DATA_DIR) and os.path.isdir(Settings.AIRFLOW_ETL_DATA_DIR):
        shutil.rmtree(Settings.AIRFLOW_ETL_DATA_DIR)
    os.makedirs(os.path.dirname(Settings.AIRFLOW_ETL_DATA_DIR), exist_ok=True)
    if os.path.exists(Settings.SIRENE_DATABASE_LOCATION):
        os.remove(Settings.SIRENE_DATABASE_LOCATION)
        logging.info(
            f"******************** Existing database removed from "
            f"{Settings.SIRENE_DATABASE_LOCATION}"
        )
    logging.info("******************* Creating database! *******************")
    sqlite_client = SqliteClient(Settings.SIRENE_DATABASE_LOCATION)
    sqlite_client.commit_and_close_conn()
