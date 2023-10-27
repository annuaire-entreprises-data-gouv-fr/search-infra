from minio.error import S3Error

from datetime import datetime, timedelta
import os
from minio import Minio
import sqlite3
import json
import re
import logging
from dag_datalake_sirene.utils.minio_helpers import (
    get_files_from_prefix,
    send_files,
    get_files,
)
from dag_datalake_sirene.data_pipelines.rne.database.process_rne import (
    insert_record,
    extract_dirigeants_data,
    create_tables,
)
from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET,
    MINIO_USER,
    MINIO_PASSWORD,
)

from dag_datalake_sirene.utils.tchap import send_message

PATH_MINIO_RNE_DATA = "rne/database/"
LATEST_DATE_FILE = "latest_rne_date.json"
MINIO_FLUX_DATA_PATH = "rne/flux/data/"
MINIO_STOCK_DATA_PATH = "rne/stock/data/"

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/database/"


client = Minio(
    MINIO_URL,
    access_key=MINIO_USER,
    secret_key=MINIO_PASSWORD,
    secure=True,
)

yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_start_date_minio(ti):
    try:
        get_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET,
            MINIO_USER=MINIO_USER,
            MINIO_PASSWORD=MINIO_PASSWORD,
            list_files=[
                {
                    "source_path": PATH_MINIO_RNE_DATA,
                    "source_name": LATEST_DATE_FILE,
                    "dest_path": TMP_FOLDER,
                    "dest_name": LATEST_DATE_FILE,
                }
            ],
        )

        with open(f"{TMP_FOLDER}/latest_rne_date.json") as fp:
            data = json.load(fp)

        start_date = data["latest_date"]
        dt_sd = datetime.strptime(start_date, "%Y-%m-%d")
        start_date = datetime.strftime((dt_sd + timedelta(days=1)), "%Y-%m-%d")
        ti.xcom_push(key="start_date", value=start_date)
    except S3Error as e:
        if e.code == "NoSuchKey":
            logging.info(
                f"The file {PATH_MINIO_RNE_DATA + LATEST_DATE_FILE} "
                f"does not exist in the bucket {MINIO_BUCKET}."
            )
            ti.xcom_push(key="start_date", value=None)
        else:
            raise Exception(
                f"An error occurred while trying to get latest date file: {e}"
            )


def connect_to_db(database_location):
    connection = sqlite3.connect(database_location)
    cursor = connection.cursor()
    return connection, cursor


def get_database_location(**kwargs):
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")
    if start_date:
        return None
    rne_database_location = TMP_FOLDER + f"rne_{start_date}.db"
    return rne_database_location


def create_db(**kwargs):
    rne_database_location = get_database_location(**kwargs)

    if rne_database_location is None:
        return None

    if os.path.exists(rne_database_location):
        os.remove(rne_database_location)

    connection, cursor = connect_to_db(rne_database_location)
    create_tables(cursor)
    connection.commit()
    connection.close()


def get_latest_db(**kwargs):
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")
    if start_date is not None:
        get_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET,
            MINIO_USER=MINIO_USER,
            MINIO_PASSWORD=MINIO_PASSWORD,
            list_files=[
                {
                    "source_path": PATH_MINIO_RNE_DATA,
                    "source_name": f"rne_{start_date}.db",
                    "dest_path": TMP_FOLDER,
                    "dest_name": "rne.db",
                }
            ],
        )


def process_stock_json_files(**kwargs):
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")
    rne_database_location = get_database_location(**kwargs)
    logging.info(f"^^^^^^^^^^^ rne database location : {rne_database_location}")

    # Only process stock files if a date doesn't already exist
    if start_date is not None:
        return None

    json_stock_rne_files = get_files_from_prefix(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET,
        MINIO_USER=MINIO_USER,
        MINIO_PASSWORD=MINIO_PASSWORD,
        prefix=MINIO_STOCK_DATA_PATH,
    )

    if not json_stock_rne_files:
        raise Exception("No RNE stock files found!!!")

    for file_path in json_stock_rne_files:
        logging.info(f"Processing stock file: {file_path}...")
        get_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET,
            MINIO_USER=MINIO_USER,
            MINIO_PASSWORD=MINIO_PASSWORD,
            list_files=[
                {
                    "source_path": "",
                    "source_name": f"{file_path}",
                    "dest_path": "",
                    "dest_name": f"{file_path}",
                }
            ],
        )
        inject_stock_records_into_database(file_path, rne_database_location)
        logging.info(
            f"File {file_path} processed and stock records injected into the database."
        )


def process_flux_json_files(**kwargs):
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")
    rne_database_location = get_database_location(**kwargs)
    logging.info(f"^^^^^^^^^^^ rne database location : {rne_database_location}")

    json_daily_flux_files = get_files_from_prefix(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET,
        MINIO_USER=MINIO_USER,
        MINIO_PASSWORD=MINIO_PASSWORD,
        prefix=MINIO_FLUX_DATA_PATH,
    )

    if not json_daily_flux_files:
        return None

    if start_date is None:
        start_date = "0000-00-00"

    for file_path in sorted(json_daily_flux_files, reverse=True):
        date_match = re.search(r"rne_flux_(\d{4}-\d{2}-\d{2})", file_path)
        if date_match:
            file_date = date_match.group(1)
            if file_date > start_date:
                logging.info(f"Processing file {file_path} with date {file_date}")
                get_files(
                    MINIO_URL=MINIO_URL,
                    MINIO_BUCKET=MINIO_BUCKET,
                    MINIO_USER=MINIO_USER,
                    MINIO_PASSWORD=MINIO_PASSWORD,
                    list_files=[
                        {
                            "source_path": MINIO_FLUX_DATA_PATH,
                            "source_name": f"rne_flux_{file_date}.json",
                            "dest_path": TMP_FOLDER,
                            "dest_name": f"rne_flux_{file_date}.json",
                        }
                    ],
                )
                json_path = f"{TMP_FOLDER}rne_flux_{file_date}.json"
                inject_records_into_database(json_path, rne_database_location)
                logging.info(
                    f"File {json_path} processed and"
                    " records injected into the database."
                )
                os.remove(json_path)

    # Extract dates from the JSON file names and sort them
    dates = sorted(
        re.findall(r"rne_flux_(\d{4}-\d{2}-\d{2})", " ".join(json_daily_flux_files))
    )
    if dates:
        last_date = dates[-1]
        logging.info(f"***** Last date saved: {last_date}")
    else:
        last_date = None
    kwargs["ti"].xcom_push(key="last_date", value=last_date)


def inject_stock_records_into_database(file_path, db_path):
    with open(file_path, "r") as file:
        logging.info(f"Processing stock file: {file_path}")
        try:
            json_data = file.read()
            data = json.loads(json_data)
            logging.info(f"/////////{data[0]}")
            for record in data:
                list_dirigeants_pp, list_dirigeants_pm = extract_dirigeants_data(
                    record, file_type="stock"
                )
                insert_record(
                    list_dirigeants_pp, list_dirigeants_pm, file_path, db_path
                )
        except json.JSONDecodeError as e:
            raise Exception(f"JSONDecodeError: {e} in file {file_path}")


def inject_records_into_database(file_path, db_path):
    with open(file_path, "r") as file:
        logging.info(f"Processing flux file: {file_path}")
        for line in file:
            try:
                data = json.loads(line)
                for record in data:
                    list_dirigeants_pp, list_dirigeants_pm = extract_dirigeants_data(
                        record, file_type="flux"
                    )
                    insert_record(
                        list_dirigeants_pp, list_dirigeants_pm, file_path, db_path
                    )
            except json.JSONDecodeError as e:
                raise Exception(f"JSONDecodeError: {e} in file {file_path}")


def send_to_minio(list_files):
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET,
        MINIO_USER=MINIO_USER,
        MINIO_PASSWORD=MINIO_PASSWORD,
        list_files=list_files,
    )


def upload_db_to_minio(ti):
    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    send_to_minio(
        [
            {
                "source_path": TMP_FOLDER,
                "source_name": f"rne_{start_date}.db",
                "dest_path": PATH_MINIO_RNE_DATA,
                "dest_name": f"rne_{start_date}.db",
            }
        ]
    )


def upload_latest_date_rne_minio(ti):
    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    latest_date = (start_date + timedelta(days=1)).strftime("%Y-%m-%d")
    data = {}
    data["latest_date"] = latest_date
    with open(TMP_FOLDER + "latest_rne_date.json", "w") as write_file:
        json.dump(data, write_file)

    send_to_minio(
        [
            {
                "source_path": TMP_FOLDER,
                "source_name": LATEST_DATE_FILE,
                "dest_path": PATH_MINIO_RNE_DATA,
                "dest_name": LATEST_DATE_FILE,
            }
        ]
    )
    ti.xcom_push(key="latest_date", value=latest_date)


def notification_mattermost(ti):
    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    end_date = ti.xcom_pull(key="end_date", task_ids="upload_latest_date_rne_minio")
    send_message(
        f"Données RNE traitées de {start_date} à {end_date} sur Minio "
        f"- Bucket {MINIO_BUCKET}",
    )
