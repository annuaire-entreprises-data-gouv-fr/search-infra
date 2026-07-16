import json
import logging
from datetime import datetime

import pandas as pd
import requests
from airflow.sdk import get_current_context
from requests.exceptions import HTTPError

from data_pipelines_annuaire.config import (
    METADATA_CC_OBJECT_STORAGE_PATH,
    METADATA_CC_TMP_FOLDER,
)
from data_pipelines_annuaire.helpers.notification import Notification
from data_pipelines_annuaire.helpers.object_storage import File, ObjectStorageClient
from data_pipelines_annuaire.helpers.utils import fetch_hyperlink_from_page

# Datasets
URL_CC_DARES = "https://travail-emploi.gouv.fr/conventions-collectives-nomenclatures"
URL_CC_KALI = "https://www.data.gouv.fr/datasets/r/02b67492-5243-44e8-8dd1-0cb3f90f35ff"
# Stable prefix of the DARES file name, used to find its download link on the page
FILE_CC_DATE = "Dares_Suivi_Historique_convention_collective_"


def is_metadata_not_updated() -> bool:
    last_run_date = ObjectStorageClient().get_date_last_modified(
        f"{METADATA_CC_OBJECT_STORAGE_PATH}cc_kali.json"
    )
    if last_run_date is not None:
        last_run_date = datetime.fromisoformat(last_run_date)
        if (
            last_run_date.month == datetime.now().month
            and last_run_date.year == datetime.now().year
        ):
            logging.info("Metadata was already updated for the current month.")
            return False
        else:
            logging.info("Metadata needs to be updated for the current month.")
            return True

    return True


def create_metadata_convention_collective_json():
    # The file name and its remote folder change every few month and are not always
    # consistent so we parse the downloadable link from the page instead of
    # guessing the URL
    file_downloaded = False
    current_url_cc_dares = URL_CC_DARES
    try:
        current_url_cc_dares = fetch_hyperlink_from_page(
            URL_CC_DARES, FILE_CC_DATE, match_on="href"
        )
        r = requests.get(current_url_cc_dares, allow_redirects=True)
        # travail-emploi.gouv.fr sometimes answers 200 with an HTML page
        # instead of the file, so r.ok is not enough: check the xlsx zip
        # signature to be sure we actually downloaded a spreadsheet
        file_downloaded = r.ok and r.content[:4] == b"PK\x03\x04"
    except (ValueError, requests.exceptions.RequestException) as e:
        logging.warning(f"Failed to fetch the CC Dares file: {e}")

    if not file_downloaded:
        # The file is sometimes unavailable, this is expected but
        # we need to be informed to act upon it if it has been too long
        last_run_date = ObjectStorageClient().get_date_last_modified(
            f"{METADATA_CC_OBJECT_STORAGE_PATH}cc_kali.json"
        )
        if last_run_date is not None:
            date_diff = datetime.now() - datetime.fromisoformat(last_run_date)
            error_message = f"\u26a0\ufe0f Le fichier CC du DARES n'est pas disponible depuis {date_diff.days} jours."
        else:
            error_message = "\u26a0\ufe0f Le fichier CC du DARES n'est pas disponible."
        logging.warning(error_message)
        ti = get_current_context()["ti"]
        ti.xcom_push(key=Notification.notification_xcom_key, value=error_message)
        raise HTTPError(f"{error_message}: {current_url_cc_dares}")

    with open(METADATA_CC_TMP_FOLDER + "dares-download.xlsx", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)
    df_dares = pd.read_excel(
        METADATA_CC_TMP_FOLDER + "dares-download.xlsx",
        sheet_name="Conventions de branche",
        dtype=str,
        header=0,
        engine="openpyxl",
    )
    # DARES pads IDCC to 5 digits (e.g. 00176) while Kali doesn't (e.g. 176)
    # We have to strip leading zeros so both dataframes can be merged
    df_dares["IDCC"] = df_dares["IDCC"].str.lstrip("0")
    df_dares = df_dares.rename(columns={"Libellé": "titre de la convention"})
    # Get Kali list
    r = requests.get(URL_CC_KALI, allow_redirects=True)
    with open(METADATA_CC_TMP_FOLDER + "kali-download.xlsx", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)
    df_kali = pd.read_excel(
        METADATA_CC_TMP_FOLDER + "kali-download.xlsx",
        header=0,
        skiprows=3,
        dtype=str,
        engine="openpyxl",
    )
    # Sort by date
    df_kali = df_kali.sort_values(by="DEBUT", ascending=False)
    df_kali = df_kali.drop_duplicates(subset="ID", keep="first")
    df_kali = df_kali.drop(columns=["Unnamed: 0"], errors="ignore")

    merged_df = pd.merge(df_dares, df_kali, left_on="IDCC", right_on="IDCC", how="left")
    merged_df.columns = merged_df.columns.str.lower()
    merged_df = merged_df.drop(columns=["titre"], errors="ignore")
    merged_df = merged_df.where(pd.notna(merged_df), None)
    merged_df.rename(columns={"id": "id_kali"}, inplace=True)
    merged_df.set_index("idcc", inplace=True)

    columns_to_keep = [
        "titre de la convention",
        "id_kali",
        "cc_ti",
        "nature",
        "etat",
        "debut",
        "fin",
        "url",
    ]
    merged_df = merged_df[columns_to_keep]

    metadata_dict = merged_df.to_dict(orient="index")
    metadata_json = {str(key): value for key, value in metadata_dict.items()}

    with open(METADATA_CC_TMP_FOLDER + "metadata-cc-kali.json", "w") as json_file:
        json.dump(metadata_json, json_file)


def upload_json_to_object_storage():
    ObjectStorageClient().send_files(
        list_files=[
            File(
                source_path=METADATA_CC_TMP_FOLDER,
                source_name="metadata-cc-kali.json",
                dest_path=METADATA_CC_OBJECT_STORAGE_PATH,
                dest_name="cc_kali.json",
                content_type=None,
            )
        ],
    )
