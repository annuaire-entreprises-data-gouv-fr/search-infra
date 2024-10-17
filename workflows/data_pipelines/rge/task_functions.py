import pandas as pd
import logging
import requests
import os

from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.config import (
    RGE_TMP_FOLDER,
    URL_RGE,
)
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.helpers.utils import get_date_last_modified, save_to_metadata
from typing import List


def preprocess_rge_data(ti):
    r = requests.get(URL_RGE, allow_redirects=True)
    data = r.json()
    list_rge: List[str] = []
    list_rge = list_rge + data["results"]
    cpt = 0
    while "next" in data:
        cpt = cpt + 1
        r = requests.get(data["next"])
        data = r.json()
        list_rge = list_rge + data["results"]
    df_rge = pd.DataFrame(list_rge)
    df_rge = df_rge[df_rge["siret"].notna()]
    df_list_rge = (
        df_rge.groupby(["siret"])["code_qualification"]
        .apply(list)
        .reset_index(name="liste_rge")
    )
    df_list_rge = df_list_rge[["siret", "liste_rge"]]
    df_list_rge["liste_rge"] = df_list_rge["liste_rge"].astype(str)

    df_list_rge.to_csv(f"{RGE_TMP_FOLDER}rge.csv", index=False)
    ti.xcom_push(key="nb_siret_rge", value=str(df_rge["siret"].nunique()))

    del df_rge
    del df_list_rge


def save_date_last_modified():
    date_last_modified = get_date_last_modified(url=URL_RGE)
    metadata_path = os.path.join(RGE_TMP_FOLDER, "metadata.json")

    # Save the 'last_modified' date to the metadata file
    save_to_metadata(metadata_path, "last_modified", date_last_modified)

    logging.info(f"Last modified date saved successfully to {metadata_path}")


def send_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": RGE_TMP_FOLDER,
                "source_name": "rge.csv",
                "dest_path": "rge/new/",
                "dest_name": "rge.csv",
            },
            {
                "source_path": RGE_TMP_FOLDER,
                "source_name": "metadata.json",
                "dest_path": "rge/new/",
                "dest_name": "metadata.json",
            },
        ],
    )


def compare_files_minio():
    is_same = minio_client.compare_files(
        file_path_1="rge/new/",
        file_name_2="rge.csv",
        file_path_2="rge/latest/",
        file_name_1="rge.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    minio_client.send_files(
        list_files=[
            {
                "source_path": RGE_TMP_FOLDER,
                "source_name": "rge.csv",
                "dest_path": "rge/latest/",
                "dest_name": "rge.csv",
            },
            {
                "source_path": RGE_TMP_FOLDER,
                "source_name": "metadata.json",
                "dest_path": "rge/latest/",
                "dest_name": "metadata.json",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siret = ti.xcom_pull(key="nb_siret_rge", task_ids="preprocess_rge_data")
    send_message(
        f"\U0001F7E2 Données RGE mises à jour.\n"
        f"- {nb_siret} établissements représentées."
    )
