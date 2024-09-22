import pandas as pd
import logging
import requests

from helpers.minio_helpers import minio_client
from helpers.settings import Settings
from helpers.tchap import send_message


def preprocess_finess_data(ti):
    r = requests.get(Settings.URL_FINESS)
    with open(Settings.FINESS_TMP_FOLDER + "finess-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_finess = pd.read_csv(
        Settings.FINESS_TMP_FOLDER + "finess-download.csv",
        dtype=str,
        sep=";",
        encoding="Latin-1",
        skiprows=1,
        header=None,
    )
    df_finess = df_finess[[1, 18, 22]]
    df_finess = df_finess.rename(
        columns={1: "finess", 18: "cat_etablissement", 22: "siret"}
    )
    # df_finess["siren"] = df_finess["siren"].str[:9]
    df_finess = df_finess[df_finess["siret"].notna()]
    df_list_finess = (
        df_finess.groupby(["siret"])["finess"]
        .apply(list)
        .reset_index(name="liste_finess")
    )
    df_list_finess = df_list_finess[["siret", "liste_finess"]]
    df_list_finess["liste_finess"] = df_list_finess["liste_finess"].astype(str)
    df_list_finess.to_csv(f"{Settings.FINESS_TMP_FOLDER}finess.csv", index=False)
    ti.xcom_push(
        key="nb_siret_finess",
        value=str(df_list_finess["siret"].nunique()),
    )
    del df_finess
    del df_list_finess


def send_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": Settings.FINESS_TMP_FOLDER,
                "source_name": "finess.csv",
                "dest_path": "finess/new/",
                "dest_name": "finess.csv",
            },
        ],
    )


def compare_files_minio():
    is_same = minio_client.compare_files(
        file_path_1="finess/new/",
        file_name_2="finess.csv",
        file_path_2="finess/latest/",
        file_name_1="finess.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    minio_client.send_files(
        list_files=[
            {
                "source_path": Settings.FINESS_TMP_FOLDER,
                "source_name": "finess.csv",
                "dest_path": "finess/latest/",
                "dest_name": "finess.csv",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siret = ti.xcom_pull(key="nb_siret_finess", task_ids="preprocess_finess_data")
    send_message(
        f"\U0001F7E2 Données Finess mises à jour.\n"
        f"- {nb_siret} établissements représentées."
    )
