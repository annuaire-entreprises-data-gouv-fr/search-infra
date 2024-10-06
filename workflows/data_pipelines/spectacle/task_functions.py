import pandas as pd
import logging
import requests

from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.utils import fetch_and_store_last_modified_metadata
from dag_datalake_sirene.config import (
    RESSOURCE_ID_ENTREPRENEUR_SPECTACLE,
    SPECTACLE_TMP_FOLDER,
    URL_ENTREPRENEUR_SPECTACLE,
)
from dag_datalake_sirene.helpers.tchap import send_message


def preprocess_spectacle_data(ti):
    r = requests.get(URL_ENTREPRENEUR_SPECTACLE)
    with open(SPECTACLE_TMP_FOLDER + "spectacle-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_spectacle = pd.read_csv(
        SPECTACLE_TMP_FOLDER + "spectacle-download.csv", dtype=str, sep=";"
    )
    df_spectacle["siren"] = df_spectacle[
        "SIRET (personne morale) / SIREN (personne physique)"
    ].str[:9]
    df_spectacle.rename(
        columns={"Statut du récépissé": "statut_du_recepisse"}, inplace=True
    )
    df_spectacle = df_spectacle[["siren", "statut_du_recepisse"]]
    df_spectacle["statut_du_recepisse"] = df_spectacle["statut_du_recepisse"].apply(
        lambda x: "valide" if x == "Valide" else "invalide"
    )

    df_spectacle = df_spectacle[df_spectacle["siren"].notna()]
    df_spectacle_clean = (
        df_spectacle.groupby("siren")["statut_du_recepisse"].unique().reset_index()
    )
    # If at least one of `statut` values is valid, then the value we keep is `valide
    df_spectacle_clean["statut_entrepreneur_spectacle"] = df_spectacle_clean[
        "statut_du_recepisse"
    ].apply(lambda list_statuts: "valide" if "valide" in list_statuts else "invalide")
    df_spectacle_clean["est_entrepreneur_spectacle"] = True
    df_spectacle_clean.drop("statut_du_recepisse", axis=1, inplace=True)
    df_spectacle_clean.to_csv(f"{SPECTACLE_TMP_FOLDER}spectacle.csv", index=False)
    ti.xcom_push(
        key="nb_siren_entrepreneur_spectacle",
        value=str(df_spectacle_clean["siren"].nunique()),
    )
    del df_spectacle_clean


def save_date_last_modified():
    fetch_and_store_last_modified_metadata(
        RESSOURCE_ID_ENTREPRENEUR_SPECTACLE, SPECTACLE_TMP_FOLDER
    )


def send_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": SPECTACLE_TMP_FOLDER,
                "source_name": "spectacle.csv",
                "dest_path": "spectacle/new/",
                "dest_name": "spectacle.csv",
            },
            {
                "source_path": SPECTACLE_TMP_FOLDER,
                "source_name": "metadata.json",
                "dest_path": "spectacle/new/",
                "dest_name": "metadata.json",
            },
        ],
    )


def compare_files_minio():
    is_same = minio_client.compare_files(
        file_path_1="spectacle/new/",
        file_name_2="spectacle.csv",
        file_path_2="spectacle/latest/",
        file_name_1="spectacle.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    minio_client.send_files(
        list_files=[
            {
                "source_path": SPECTACLE_TMP_FOLDER,
                "source_name": "spectacle.csv",
                "dest_path": "spectacle/latest/",
                "dest_name": "spectacle.csv",
            },
            {
                "source_path": SPECTACLE_TMP_FOLDER,
                "source_name": "metadata.json",
                "dest_path": "spectacle/latest/",
                "dest_name": "metadata.json",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siren = ti.xcom_pull(
        key="nb_siren_entrepreneur_spectacle", task_ids="preprocess_spectacle_data"
    )
    send_message(
        f"\U0001F7E2 Données Entrepreneur spectacle mises à jour.\n"
        f"- {nb_siren} unités légales représentées."
    )
