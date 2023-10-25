import os
import json
from datetime import datetime, timedelta
import re
import logging
from dag_datalake_sirene.data_pipelines.utils.mattermost import send_message
from dag_datalake_sirene.data_pipelines.utils.minio_functions import (
    get_files_from_prefix,
    send_files,
)
from dag_datalake_sirene.data_pipelines.rne.flux.rne_api import ApiRNEClient
from dag_datalake_sirene.data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET,
    MINIO_USER,
    MINIO_PASSWORD,
)


TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/flux/"
DATADIR = f"{TMP_FOLDER}data"
DEFAULT_START_DATE = "2023-07-01"
MINIO_DATA_PATH = "rne/flux/data/"


def get_last_json_file_date():
    json_daily_flux_files = get_files_from_prefix(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET,
        MINIO_USER=MINIO_USER,
        MINIO_PASSWORD=MINIO_PASSWORD,
        prefix=MINIO_DATA_PATH,
    )

    if not json_daily_flux_files:
        return None

    # Extract dates from the JSON file names and sort them
    dates = sorted(
        re.findall(r"rne_flux_(\d{4}-\d{2}-\d{2})", " ".join(json_daily_flux_files))
    )

    if dates:
        last_date = dates[-1]
        logging.info(f"***** Last date saved: {last_date}")
        return last_date
    else:
        return None


def compute_start_date():
    last_json_date = get_last_json_file_date()

    if last_json_date:
        last_date_obj = datetime.strptime(last_json_date, "%Y-%m-%d")
        next_day = last_date_obj + timedelta(days=1)
        start_date = next_day.strftime("%Y-%m-%d")
    else:
        start_date = DEFAULT_START_DATE

    return start_date


def get_and_save_daily_flux_rne(
    start_date: str,
    end_date: str,
):
    """
    Fetches daily flux data from RNE API,
    stores it in a JSON file, and sends the file to a MinIO server.

    Args:
        start_date (str): The start date for data retrieval in the format 'YYYY-MM-DD'.
        end_date (str): The end date for data retrieval in the format 'YYYY-MM-DD'.

    Returns:
        None
    """
    json_file_name = f"rne_flux_{start_date}.json"
    json_file_path = f"{DATADIR}/{json_file_name}"

    if not os.path.exists(DATADIR):
        logging.info(f"********** Creating {DATADIR}")
        os.makedirs(DATADIR)

    last_siren = None  # Initialize last_siren
    page_data = True

    rne_client = ApiRNEClient()

    with open(json_file_path, "a") as json_file:
        logging.info(f"****** Opening file: {json_file_path}")
        while page_data:
            try:
                page_data, last_siren = rne_client.make_api_request(
                    start_date, end_date, last_siren
                )
                if page_data:
                    json.dump(page_data, json_file)
                    json_file.write("\n")  # Add a newline for multiple JSON objects
            except Exception as e:
                # If the API request failed, delete the current
                # JSON file and break the loop
                logging.info(f"****** Deleting file: {json_file_path}")
                os.remove(json_file_path)
                raise Exception(f"Error occurred during the API request: {e}")

    if os.path.exists(json_file_path):
        send_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET,
            MINIO_USER=MINIO_USER,
            MINIO_PASSWORD=MINIO_PASSWORD,
            list_files=[
                {
                    "source_path": f"{DATADIR}/",
                    "source_name": f"{json_file_name}",
                    "dest_path": MINIO_DATA_PATH,
                    "dest_name": f"{json_file_name}",
                },
            ],
        )
        logging.info(f"****** Sent file to MinIO: {json_file_name}")


def get_every_day_flux(**kwargs):
    """
    Fetches daily flux data from the Registre National des Entreprises (RNE) API
    and saves it to JSON files for a range of dates. This function iterates through
    a date range and calls the `get_and_save_daily_flux_rne` function for each day.
    """
    # Get the start and end date
    start_date = compute_start_date()
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logging.info(f"********* Start date: {start_date}")
    logging.info(f"********* End date: {end_date}")

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date_dt:
        start_date_formatted = current_date.strftime("%Y-%m-%d")
        next_day = current_date + timedelta(days=1)
        next_day_formatted = next_day.strftime("%Y-%m-%d")

        get_and_save_daily_flux_rne(start_date_formatted, next_day_formatted)

        current_date = next_day

    kwargs["ti"].xcom_push(key="rne_flux_start_date", value=start_date)
    kwargs["ti"].xcom_push(key="rne_flux_end_date", value=end_date)


def send_notification_mattermost(**kwargs):
    rne_flux_start_date = kwargs["ti"].xcom_pull(
        key="rne_flux_start_date", task_ids="get_every_day_flux"
    )
    rne_flux_end_date = kwargs["ti"].xcom_pull(
        key="rne_flux_end_date", task_ids="get_every_day_flux"
    )
    send_message(
        f"Données flux RNE mise à jour sur Minio "
        f"- Bucket {MINIO_BUCKET} :"
        f"\n - Date début flux : {rne_flux_start_date} "
        f"\n - Date fin flux : {rne_flux_end_date} "
    )
