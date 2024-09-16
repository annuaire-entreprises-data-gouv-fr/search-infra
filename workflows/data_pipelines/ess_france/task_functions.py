import pandas as pd
import logging

from helpers.minio_helpers import minio_client
from config import (
    ESS_TMP_FOLDER,
    URL_ESS_FRANCE,
)
from helpers.tchap import send_message


def preprocess_ess_france_data(ti):
    df_ess = pd.read_csv(URL_ESS_FRANCE, dtype=str)
    df_ess["SIREN"] = df_ess["SIREN"].str.zfill(9)
    df_ess.rename(columns={"SIREN": "siren"}, inplace=True)
    df_ess["est_ess_france"] = True
    df_ess = df_ess[["siren", "est_ess_france"]]

    df_ess.to_csv(f"{ESS_TMP_FOLDER}ess_france.csv", index=False)
    ti.xcom_push(key="nb_siren_ess", value=str(df_ess["siren"].nunique()))


def send_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": ESS_TMP_FOLDER,
                "source_name": "ess_france.csv",
                "dest_path": "ess/new/",
                "dest_name": "ess_france.csv",
            },
        ],
    )


def compare_files_minio():
    is_same = minio_client.compare_files(
        file_path_1="ess/new/",
        file_name_2="ess_france.csv",
        file_path_2="ess/latest/",
        file_name_1="ess_france.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    minio_client.send_files(
        list_files=[
            {
                "source_path": ESS_TMP_FOLDER,
                "source_name": "ess_france.csv",
                "dest_path": "ess/latest/",
                "dest_name": "ess_france.csv",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siren = ti.xcom_pull(key="nb_siren_ess", task_ids="preprocess_ess_data")
    send_message(
        f"\U0001F7E2 Données ESS France mises à jour.\n"
        f"- {nb_siren} unités légales représentées."
    )
