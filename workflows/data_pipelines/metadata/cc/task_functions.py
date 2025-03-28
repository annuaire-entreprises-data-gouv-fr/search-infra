import json
import logging
from datetime import datetime

import pandas as pd
import requests
from airflow.operators.python import get_current_context
from requests.exceptions import HTTPError

from dag_datalake_sirene.config import (
    FILE_CC_DATE,
    METADATA_CC_MINIO_PATH,
    METADATA_CC_TMP_FOLDER,
    URL_CC_DARES,
    URL_CC_KALI,
)
from dag_datalake_sirene.helpers.minio_helpers import File, MinIOClient
from dag_datalake_sirene.helpers.notification import Notification
from dag_datalake_sirene.helpers.utils import get_previous_months


def get_month_year_french():
    # Mapping of month numbers to French month names in lowercase
    month_mapping = {
        1: "janvier",
        2: "février",
        3: "mars",
        4: "avril",
        5: "mai",
        6: "juin",
        7: "juillet",
        8: "aout",
        9: "septembre",
        10: "octobre",
        11: "novembre",
        12: "décembre",
    }

    current_date = datetime.now()
    month_number = current_date.month
    month_name_french = month_mapping.get(month_number, "unknown").title()

    year_last_two_digits = str(current_date.year)[-2:]
    # Format the result as 'monthYear'
    result = f"{month_name_french}{year_last_two_digits}"
    return result


def is_metadata_not_updated() -> bool:
    last_run_date = MinIOClient().get_date_last_modified(
        f"{METADATA_CC_MINIO_PATH}cc_kali.json"
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
    context = get_current_context()
    ti = context["ti"]

    current_cc_dares_file = f"{FILE_CC_DATE}{get_month_year_french()}.xlsx"
    months = get_previous_months(lookback=3)
    logging.info(months)
    # We try to fetch the file from previous folder months because sometimes
    # the file is not uploaded to the right folder
    for month in months:
        current_url_cc_dares = f"{URL_CC_DARES}/{month}/{current_cc_dares_file}"
        logging.info(f"CC Dares URL: {current_url_cc_dares}")

        r = requests.get(current_url_cc_dares, allow_redirects=True)
        if r.ok:
            break

    if not r.ok:
        # The file is often unavailable, this is expected but
        # we need to be informed to act upon it if it has been too long
        last_run_date = MinIOClient().get_date_last_modified(
            f"{METADATA_CC_MINIO_PATH}cc_kali.json"
        )
        if last_run_date is not None:
            date_diff = datetime.now() - datetime.fromisoformat(last_run_date)
            error_message = f"\u26a0\ufe0f {r.status_code}: Le fichier CC du DARES n'est pas disponible depuis {date_diff.days} jours."
        else:
            error_message = f"\u26a0\ufe0f {r.status_code}: Le fichier CC du DARES n'est pas disponible."
        logging.warning(error_message)
        ti.xcom_push(key=Notification.notification_xcom_key, value=error_message)
        raise HTTPError(f"{error_message}: {current_url_cc_dares}")

    with open(METADATA_CC_TMP_FOLDER + "dares-download.xlsx", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)
    df_dares = pd.read_excel(
        METADATA_CC_TMP_FOLDER + "dares-download.xlsx",
        dtype=str,
        header=0,
        skiprows=3,
        engine="openpyxl",
    )
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


def upload_json_to_minio():
    MinIOClient().send_files(
        list_files=[
            File(
                source_path=METADATA_CC_TMP_FOLDER,
                source_name="metadata-cc-kali.json",
                dest_path=METADATA_CC_MINIO_PATH,
                dest_name="cc_kali.json",
                content_type=None,
            )
        ],
    )
