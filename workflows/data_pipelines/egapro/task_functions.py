import pandas as pd
import logging

from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.config import (
    EGAPRO_TMP_FOLDER,
    RESOURCE_ID_EGAPRO,
    URL_EGAPRO,
)
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.helpers.utils import (
    fetch_and_store_last_modified_metadata,
)


def preprocess_egapro_data(ti):
    df_egapro = pd.read_excel(
        URL_EGAPRO,
        dtype=str,
        engine="openpyxl",
    )
    df_egapro = df_egapro.drop_duplicates(subset=["SIREN"], keep="first")
    df_egapro = df_egapro[["SIREN"]]
    df_egapro["egapro_renseignee"] = True
    df_egapro = df_egapro.rename(columns={"SIREN": "siren"})
    df_egapro.to_csv(f"{EGAPRO_TMP_FOLDER}egapro.csv", index=False)
    ti.xcom_push(key="nb_siren_egapro", value=str(df_egapro["siren"].nunique()))
    del df_egapro


def save_date_last_modified():
    fetch_and_store_last_modified_metadata(RESOURCE_ID_EGAPRO, EGAPRO_TMP_FOLDER)


def send_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": EGAPRO_TMP_FOLDER,
                "source_name": "egapro.csv",
                "dest_path": "egapro/new/",
                "dest_name": "egapro.csv",
            },
            {
                "source_path": EGAPRO_TMP_FOLDER,
                "source_name": "metadata.json",
                "dest_path": "egapro/new/",
                "dest_name": "metadata.json",
            },
        ],
    )


def compare_files_minio():
    are_files_identical = minio_client.compare_files(
        file_path_1="egapro/new/",
        file_name_2="egapro.csv",
        file_path_2="egapro/latest/",
        file_name_1="egapro.csv",
    )

    if are_files_identical:
        return False

    if are_files_identical is None:
        logging.info("First time in this Minio env. Creating")

    minio_client.send_files(
        list_files=[
            {
                "source_path": EGAPRO_TMP_FOLDER,
                "source_name": "egapro.csv",
                "dest_path": "egapro/latest/",
                "dest_name": "egapro.csv",
            },
            {
                "source_path": EGAPRO_TMP_FOLDER,
                "source_name": "metadata.json",
                "dest_path": "egapro/latest/",
                "dest_name": "metadata.json",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siren = ti.xcom_pull(key="nb_siren_egapro", task_ids="process_egapro")
    send_message(
        f"\U0001F7E2 Données Egapro mises à jour.\n"
        f"- {nb_siren} unités légales représentées."
    )
