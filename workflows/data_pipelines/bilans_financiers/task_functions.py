import pandas as pd
import logging
from datetime import datetime

from dag_datalake_sirene.config import (
    BILANS_FINANCIERS_TMP_FOLDER,
)
from dag_datalake_sirene.helpers.datagouv import get_resource
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.helpers.utils import get_fiscal_year

from dag_datalake_sirene.helpers.s3_helpers import s3_client


def download_bilans_financiers():
    get_resource(
        resource_id="9d213815-1649-4527-9eb4-427146ef2e5b",
        file_to_store={
            "dest_path": BILANS_FINANCIERS_TMP_FOLDER,
            "dest_name": "bilans_entreprises.csv",
        },
    )
    logging.info("download done!")


def process_bilans_financiers(ti):
    fields = [
        "siren",
        "chiffre_d_affaires",
        "resultat_net",
        "date_cloture_exercice",
        "type_bilan",
    ]
    df_bilan = pd.read_csv(
        f"{BILANS_FINANCIERS_TMP_FOLDER}bilans_entreprises.csv",
        dtype=str,
        sep=";",
        usecols=fields,
        parse_dates=[
            "date_cloture_exercice"
        ],  # Convert 'date_cloture_exercice' to datetime
    )
    # Get the current fiscal year
    current_fiscal_year = get_fiscal_year(datetime.now())

    # Filter out rows with fiscal years greater than the current fiscal year
    df_bilan["annee_cloture_exercice"] = df_bilan["date_cloture_exercice"].apply(
        get_fiscal_year
    )
    df_bilan = df_bilan[df_bilan["annee_cloture_exercice"] <= current_fiscal_year]

    # Rename columns and keep relevant columns
    df_bilan = df_bilan.rename(columns={"chiffre_d_affaires": "ca"})
    df_bilan = df_bilan[
        [
            "siren",
            "ca",
            "date_cloture_exercice",
            "resultat_net",
            "type_bilan",
            "annee_cloture_exercice",
        ]
    ]

    # Drop duplicates based on siren, fiscal year, and type_bilan
    df_bilan = df_bilan.drop_duplicates(
        subset=["siren", "annee_cloture_exercice", "type_bilan"], keep="last"
    )

    # Filter out rows with 'type_bilan' value different than 'K' if the corresponding
    # 'siren' exists in at least one row with 'type_bilan' 'K'
    siren_with_K = df_bilan[df_bilan["type_bilan"] == "K"]["siren"].unique()
    df_bilan = df_bilan[
        ~df_bilan["siren"].isin(siren_with_K) | (df_bilan["type_bilan"] == "K")
    ].reset_index(drop=True)

    # Sort values by siren, fiscal year, and type_bilan, and then keep the first
    # occurrence of each siren (C takes precedant over S alphabetically as well)
    df_bilan = df_bilan.sort_values(
        ["siren", "annee_cloture_exercice", "type_bilan"], ascending=[True, False, True]
    )
    df_bilan = df_bilan.drop_duplicates(subset=["siren"], keep="first")

    # Convert columns to appropriate data types
    df_bilan["ca"] = df_bilan["ca"].astype(float)
    df_bilan["resultat_net"] = df_bilan["resultat_net"].astype(float)

    # Keep only the relevant columns and log the first 3 rows of the resulting DataFrame
    df_bilan = df_bilan[
        [
            "siren",
            "ca",
            "date_cloture_exercice",
            "resultat_net",
            "annee_cloture_exercice",
        ]
    ]

    df_bilan.to_csv(
        f"{BILANS_FINANCIERS_TMP_FOLDER}synthese_bilans.csv",
        index=False,
    )

    ti.xcom_push(key="nb_siren", value=str(df_bilan.shape[0]))


def send_file_to_minio():
    s3_client.send_files(
        list_files=[
            {
                "source_path": BILANS_FINANCIERS_TMP_FOLDER,
                "source_name": "synthese_bilans.csv",
                "dest_path": "bilans_financiers/new/",
                "dest_name": "synthese_bilans.csv",
            },
        ],
    )


def compare_files_minio():
    is_same = s3_client.compare_files(
        file_path_1="bilans_financiers/new/",
        file_name_2="synthese_bilans.csv",
        file_path_2="bilans_financiers/latest/",
        file_name_1="synthese_bilans.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    s3_client.send_files(
        list_files=[
            {
                "source_path": BILANS_FINANCIERS_TMP_FOLDER,
                "source_name": "synthese_bilans.csv",
                "dest_path": "bilans_financiers/latest/",
                "dest_name": "synthese_bilans.csv",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siren = ti.xcom_pull(key="nb_siren", task_ids="process_bilans_financiers")
    send_message(
        f"\U0001F7E2 Données Bilans Financiers mises à jour.\n"
        f"- {nb_siren} unités légales référencés\n"
    )
