import pandas as pd
import requests
import logging
import json
from datetime import datetime
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.helpers.s3_helpers import s3_client
from dag_datalake_sirene.config import (
    URL_CC_DARES,
    URL_CC_KALI,
    METADATA_CC_MINIO_PATH,
    METADATA_CC_TMP_FOLDER,
)


def get_month_year_french():
    # Mapping of month numbers to French month names in lowercase
    month_mapping = {
        1: "janvier",
        2: "fevrier",
        3: "mars",
        4: "avril",
        5: "mai",
        6: "juin",
        7: "juillet",
        8: "aout",
        9: "septembre",
        10: "octobre",
        11: "novembre",
        12: "decembre",
    }

    current_date = datetime.now()
    month_number = current_date.month
    month_name_french = month_mapping.get(month_number, "unknown")

    year_last_two_digits = str(current_date.year)[-2:]
    # Format the result as 'monthYear'
    result = f"{month_name_french}{year_last_two_digits}"
    return result


def create_metadata_concollective_json():
    current_cc_dares_extension = f"{get_month_year_french()}.xlsx"
    current_url_cc_dares = URL_CC_DARES + current_cc_dares_extension
    logging.info(f"Current CC Dares URL: {current_url_cc_dares}")
    r = requests.get(current_url_cc_dares, allow_redirects=True)
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


def upload_json_file_to_minio():
    s3_client.send_files(
        list_files=[
            {
                "source_path": METADATA_CC_TMP_FOLDER,
                "source_name": "metadata-cc-kali.json",
                "dest_path": METADATA_CC_MINIO_PATH,
                "dest_name": "cc_kali.json",
            }
        ],
    )


def send_notification_success_tchap():
    send_message(
        f"\U0001F7E2 Données :"
        f"\nMetadata Conventions Collectives mise à jour sur Minio "
        f"- Bucket {s3_client.bucket}."
    )


def send_notification_failure_tchap(context):
    send_message("\U0001F534 Données :" "\nFail DAG Metadata CC!!!!")
