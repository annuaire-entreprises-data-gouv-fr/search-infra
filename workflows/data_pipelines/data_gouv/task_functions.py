from datetime import datetime
import gzip
import re
import shutil
import logging
import os
import pandas as pd
from dag_datalake_sirene.config import (
    SIRENE_MINIO_DATA_PATH,
    AIRFLOW_DATAGOUV_DATA_DIR,
)
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient
from dag_datalake_sirene.workflows.data_pipelines.data_gouv.queries import (
    etab_fields_to_select,
    ul_fields_to_select,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.data_enrichment import (
    format_adresse_complete,
)

from dag_datalake_sirene.helpers.tchap import send_message


def send_notification_failure_tchap(context):
    send_message("\U0001F534 Données :" "\nFail DAG de publication!!!!")


current_date = datetime.now().date()


def get_latest_database(**kwargs):
    database_files = minio_client.get_files_from_prefix(
        prefix=SIRENE_MINIO_DATA_PATH,
    )

    if not database_files:
        raise Exception(f"No database files were found in : {SIRENE_MINIO_DATA_PATH}")

    # Extract dates from the db file names and sort them
    dates = sorted(re.findall(r"sirene_(\d{4}-\d{2}-\d{2})", " ".join(database_files)))

    if dates:
        last_date = dates[-1]
        logging.info(f"***** Last database saved: {last_date}")
        minio_client.get_files(
            list_files=[
                {
                    "source_path": SIRENE_MINIO_DATA_PATH,
                    "source_name": f"sirene_{last_date}.db.gz",
                    "dest_path": AIRFLOW_DATAGOUV_DATA_DIR,
                    "dest_name": "sirene.db.gz",
                }
            ],
        )
        # Unzip database file
        db_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}sirene.db"
        with gzip.open(f"{db_path}.gz", "rb") as f_in:
            with open(db_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(f"{db_path}.gz")

    else:
        raise Exception(
            f"No dates in database files were found : {SIRENE_MINIO_DATA_PATH}"
        )


def fill_ul_file():
    chunk_size = 100000
    sqlite_client = SqliteClient(AIRFLOW_DATAGOUV_DATA_DIR + "sirene.db")

    ul_parquet_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}unites_legales.parquet"
    # Use the first chunk to create the parquet file
    first_chunk = True
    for chunk in pd.read_sql_query(
        ul_fields_to_select, sqlite_client.db_conn, chunksize=chunk_size
    ):
        """
        chunk["nom_complet"] = format_nom_complet(
            chunk["nom"],
            chunk["nom_usage"],
            chunk["nom_raison_sociale"],
            chunk["prenom"],
        )
        """
        if first_chunk:
            chunk.to_parquet(ul_parquet_path, index=False)
            first_chunk = False
        else:
            chunk.to_parquet(ul_parquet_path, index=False, append=True)

    sqlite_client.commit_and_close_conn()


def fill_etab_file():
    sqlite_client = SqliteClient(AIRFLOW_DATAGOUV_DATA_DIR + "sirene.db")
    df_etab = pd.read_sql_query(etab_fields_to_select, sqlite_client.db_conn)
    df_etab

    df_etab["adresse"] = format_adresse_complete(
        df_etab["complement_adresse"],
        df_etab["numero_voie"],
        df_etab["indice_repetition"],
        df_etab["type_voie"],
        df_etab["libelle_voie"],
        df_etab["libelle_commune"],
        df_etab["libelle_cedex"],
        df_etab["distribution_speciale"],
        df_etab["code_postal"],
        df_etab["cedex"],
        df_etab["commune"],
        df_etab["libelle_commune_etranger"],
        df_etab["libelle_pays_etranger"],
    )

    etab_parquet_path = f"{AIRFLOW_DATAGOUV_DATA_DIR}etablissements.parquet"
    df_etab.to_parquet(etab_parquet_path, index=False)

    # del df_etab
