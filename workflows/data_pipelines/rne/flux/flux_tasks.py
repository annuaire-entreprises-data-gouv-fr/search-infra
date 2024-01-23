import os
import json
from datetime import datetime, timedelta
import re
import logging
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.utils import get_last_line
from dag_datalake_sirene.workflows.data_pipelines.rne.flux.rne_api import ApiRNEClient
from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    RNE_FLUX_DATADIR,
    RNE_MINIO_FLUX_DATA_PATH,
    RNE_DEFAULT_START_DATE,
)


def get_last_json_file_date():
    json_daily_flux_files = minio_client.get_files_from_prefix(
        prefix=RNE_MINIO_FLUX_DATA_PATH,
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


def get_latest_json_file(ti):
    start_date = compute_start_date()
    last_json_file_path = f"{RNE_FLUX_DATADIR}/rne_flux_{start_date}.json"
    minio_client.get_object_minio(
        f"ae/{AIRFLOW_ENV}/{RNE_MINIO_FLUX_DATA_PATH}",
        f"rne_flux_{start_date}.json",
        last_json_file_path,
    )
    logging.info(f"***** Got file: {last_json_file_path}")
    ti.xcom_push(key="last_json_file_path", value=last_json_file_path)
    return last_json_file_path


def get_last_siren(ti):
    """
    Retrieve the last 'siren' value from the latest JSON file
    associated with the RNE flux.

    Parameters:
    - ti (airflow.models.TaskInstance): The Airflow task instance
    for which the latest JSON file needs to be retrieved.

    Returns:
    - str or None: The 'siren' value if found, or None if not present
    in the JSON file.

    Raises:
    - Exception: If no valid JSON is found in the specified file.
    """
    try:
        last_json_file_path = get_latest_json_file(ti)

        last_line = get_last_line(last_json_file_path)

        try:
            if last_line:
                latest_dict = json.loads(last_line)
                latest_company = latest_dict["company"]
                last_siren = latest_company.get("siren")
                if last_siren is not None:
                    logging.info(
                        f"****Last siren in saved file "
                        f"{last_json_file_path}: {last_siren}"
                    )
                else:
                    logging.info("No 'siren' key found in the decoded JSON.")
        except json.JSONDecodeError:
            logging.error("Error decoding JSON. Removing last line and trying again.")

        return last_siren
    except Exception:
        return None


def compute_start_date():
    last_json_date = get_last_json_file_date()

    if last_json_date:
        last_date_obj = datetime.strptime(last_json_date, "%Y-%m-%d")
        start_date = last_date_obj.strftime("%Y-%m-%d")
        logging.info(f"++++++++Start date: {start_date}")
    else:
        start_date = RNE_DEFAULT_START_DATE

    return start_date


def get_and_save_daily_flux_rne(
    start_date: str,
    end_date: str,
    first_exec: bool,
    ti,
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
    json_file_path = f"{RNE_FLUX_DATADIR}/{json_file_name}"

    if not os.path.exists(RNE_FLUX_DATADIR):
        logging.info(f"********** Creating {RNE_FLUX_DATADIR}")
        os.makedirs(RNE_FLUX_DATADIR)

    if first_exec:
        last_siren = get_last_siren(ti)
        logging.info(f"*********{last_siren}")
    else:
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
                    for company in page_data:  # type: ignore
                        json.dump(company, json_file)
                        json_file.write("\n")
            except Exception as e:
                # If exception accures, save uncompleted file
                if os.path.exists(json_file_path):
                    minio_client.send_files(
                        list_files=[
                            {
                                "source_path": f"{RNE_FLUX_DATADIR}/",
                                "source_name": f"{json_file_name}",
                                "dest_path": RNE_MINIO_FLUX_DATA_PATH,
                                "dest_name": f"{json_file_name}",
                            },
                        ],
                    )
                # If the API request failed, delete the current
                # JSON file and break the loop
                logging.info(f"****** Deleting file: {json_file_path}")
                os.remove(json_file_path)
                raise Exception(f"Error occurred during the API request: {e}")

    if os.path.exists(json_file_path):
        minio_client.send_files(
            list_files=[
                {
                    "source_path": f"{RNE_FLUX_DATADIR}/",
                    "source_name": f"{json_file_name}",
                    "dest_path": RNE_MINIO_FLUX_DATA_PATH,
                    "dest_name": f"{json_file_name}",
                },
            ],
        )
        logging.info(f"****** Sent file to MinIO: {json_file_name}")
        logging.info(f"****** Deleting file: {json_file_path}")
        os.remove(json_file_path)


def get_every_day_flux(ti):
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
    first_exec = True
    while current_date <= end_date_dt:
        start_date_formatted = current_date.strftime("%Y-%m-%d")
        next_day = current_date + timedelta(days=1)
        next_day_formatted = next_day.strftime("%Y-%m-%d")
        get_and_save_daily_flux_rne(
            start_date_formatted, next_day_formatted, first_exec, ti
        )
        first_exec = False
        current_date = next_day

    ti.xcom_push(key="rne_flux_start_date", value=start_date)
    ti.xcom_push(key="rne_flux_end_date", value=end_date)


def send_notification_success_tchap(**kwargs):
    rne_flux_start_date = kwargs["ti"].xcom_pull(
        key="rne_flux_start_date", task_ids="get_every_day_flux"
    )
    rne_flux_end_date = kwargs["ti"].xcom_pull(
        key="rne_flux_end_date", task_ids="get_every_day_flux"
    )
    send_message(
        f"\U0001F7E2 Données :"
        f"\nDonnées flux RNE mise à jour sur Minio "
        f"- Bucket {minio_client.bucket}."
        f"\n - Date début flux : {rne_flux_start_date} "
        f"\n - Date fin flux : {rne_flux_end_date} "
    )


def send_notification_failure_tchap(context):
    send_message("\U0001F534 Données :" "\nFail DAG flux RNE!!!!")
