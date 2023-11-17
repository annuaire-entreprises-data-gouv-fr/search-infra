from minio.error import S3Error
from datetime import datetime, timedelta
import os
import json
import re
import logging
from dag_datalake_sirene.utils.minio_helpers import (
    get_files_from_prefix,
    send_files,
    get_files,
)
from dag_datalake_sirene.data_pipelines.rne.database.process_rne import (
    create_tables,
    get_tables_count,
    inject_records_into_db,
)
from dag_datalake_sirene.data_pipelines.rne.database.db_connexion import (
    connect_to_db,
)
from dag_datalake_sirene.utils.tchap import send_message
from dag_datalake_sirene.config import (
    MINIO_BUCKET,
    MINIO_URL,
    MINIO_USER,
    MINIO_PASSWORD,
    RNE_MINIO_DATA_PATH,
    RNE_LATEST_DATE_FILE,
    RNE_DB_TMP_FOLDER,
    RNE_MINIO_STOCK_DATA_PATH,
    RNE_MINIO_FLUX_DATA_PATH,
)


def get_start_date_minio(**kwargs):
    try:
        get_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET,
            MINIO_USER=MINIO_USER,
            MINIO_PASSWORD=MINIO_PASSWORD,
            list_files=[
                {
                    "source_path": RNE_MINIO_DATA_PATH,
                    "source_name": RNE_LATEST_DATE_FILE,
                    "dest_path": RNE_DB_TMP_FOLDER,
                    "dest_name": RNE_LATEST_DATE_FILE,
                }
            ],
        )

        with open(f"{RNE_DB_TMP_FOLDER}/latest_rne_date.json") as fp:
            data = json.load(fp)

        previous_latest_date = data["latest_date"]
        previous_latest_date = datetime.strptime(previous_latest_date, "%Y-%m-%d")
        start_date = datetime.strftime(previous_latest_date, "%Y-%m-%d")
        kwargs["ti"].xcom_push(key="start_date", value=start_date)
    except S3Error as e:
        if e.code == "NoSuchKey":
            logging.info(
                f"The file {RNE_MINIO_STOCK_DATA_PATH + RNE_LATEST_DATE_FILE} "
                f"does not exist in the bucket {MINIO_BUCKET}."
            )
            kwargs["ti"].xcom_push(key="start_date", value=None)
        else:
            raise Exception(
                f"An error occurred while trying to get latest date file: {e}"
            )


def create_db_path(start_date):
    """
    Create a database path for RNE data.

    Args:
        start_date (str): The start date for the RNE data.

    Returns:
        str or None: The database path if it doesn't already exist, otherwise None.
    """
    # Only return path if start_date does not already exist
    """
    if start_date:
        return None
    """
    rne_database_location = RNE_DB_TMP_FOLDER + f"rne_{start_date}.db"
    return rne_database_location


def create_db(**kwargs):
    """
    Create an RNE database, if it doesn't already exist.

    Args:
        **kwargs: Keyword arguments including 'ti' (TaskInstance) for 'start_date'.

    Returns:
        None: If the database already exists or couldn't be created.
    """
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")

    rne_db_path = create_db_path(start_date)
    kwargs["ti"].xcom_push(key="rne_db_path", value=rne_db_path)
    logging.info(f"***********RNE database path: {rne_db_path}")

    if start_date:
        return None

    if os.path.exists(rne_db_path):
        os.remove(rne_db_path)

    connection, cursor = connect_to_db(rne_db_path)
    create_tables(cursor)

    connection.commit()
    connection.close()


def get_latest_db(**kwargs):
    """
    This function retrieves the RNE database file associated with the
    provided start date from a Minio server and saves it to a
    temporary folder for further processing.
    """
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")
    if start_date is not None:
        previous_latest_date = datetime.strptime(start_date, "%Y-%m-%d")
        previous_start_date = datetime.strftime(
            (previous_latest_date - timedelta(days=1)), "%Y-%m-%d"
        )
        get_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET,
            MINIO_USER=MINIO_USER,
            MINIO_PASSWORD=MINIO_PASSWORD,
            list_files=[
                {
                    "source_path": RNE_MINIO_DATA_PATH,
                    "source_name": f"rne_{previous_start_date}.db",
                    "dest_path": RNE_DB_TMP_FOLDER,
                    "dest_name": f"rne_{start_date}.db",
                }
            ],
        )


def process_stock_json_files(**kwargs):
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")
    rne_db_path = kwargs["ti"].xcom_pull(key="rne_db_path", task_ids="create_db")

    # Only process stock files if a date doesn't already exist
    if start_date is not None:
        return None

    json_stock_rne_files = get_files_from_prefix(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET,
        MINIO_USER=MINIO_USER,
        MINIO_PASSWORD=MINIO_PASSWORD,
        prefix=RNE_MINIO_STOCK_DATA_PATH,
    )

    if not json_stock_rne_files:
        raise Exception("No RNE stock files found!!!")

    for file_path in json_stock_rne_files:
        logging.info(f"*******Processing stock file: {file_path}...")
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
        inject_records_into_db(file_path, rne_db_path, file_type="stock")
        logging.info(
            f"File {file_path} processed and stock records injected into the database."
        )
        os.remove(file_path)
        logging.info(f"******Removed file: {file_path}")


def process_flux_json_files(**kwargs):
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")
    rne_db_path = kwargs["ti"].xcom_pull(key="rne_db_path", task_ids="create_db")

    json_daily_flux_files = get_files_from_prefix(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET,
        MINIO_USER=MINIO_USER,
        MINIO_PASSWORD=MINIO_PASSWORD,
        prefix=RNE_MINIO_FLUX_DATA_PATH,
    )

    if not json_daily_flux_files:
        return None

    if start_date is None:
        start_date = "0000-00-00"

    for file_path in sorted(json_daily_flux_files, reverse=False):
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
                            "source_path": RNE_MINIO_FLUX_DATA_PATH,
                            "source_name": f"rne_flux_{file_date}.json",
                            "dest_path": RNE_DB_TMP_FOLDER,
                            "dest_name": f"rne_flux_{file_date}.json",
                        }
                    ],
                )
                json_path = f"{RNE_DB_TMP_FOLDER}rne_flux_{file_date}.json"
                inject_records_into_db(json_path, rne_db_path, "flux")
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
        last_date_processed = dates[-1]
        logging.info(f"***** Last date saved: {last_date_processed}")
    else:
        last_date_processed = None
    kwargs["ti"].xcom_push(key="last_date_processed", value=last_date_processed)


def check_db_count(ti, min_pp_table_count=12000000, min_pm_table_count=1000000):
    try:
        rne_db_path = ti.xcom_pull(key="rne_db_path", task_ids="create_db")
        count_pp, count_pm = get_tables_count(rne_db_path)

        if count_pp < min_pp_table_count or count_pm < min_pm_table_count:
            raise Exception(
                f"Counts below the minimum threshold: "
                f"count pp : {count_pp}"
                f"count pm : {count_pm}"
            )

    except Exception as e:
        raise Exception(f"An error occurred: {e}")


def send_to_minio(list_files):
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET,
        MINIO_USER=MINIO_USER,
        MINIO_PASSWORD=MINIO_PASSWORD,
        list_files=list_files,
    )


def upload_db_to_minio(**kwargs):
    start_date = kwargs["ti"].xcom_pull(key="start_date", task_ids="get_start_date")
    last_date_processed = kwargs["ti"].xcom_pull(
        key="last_date_processed", task_ids="process_flux_json_files"
    )
    send_to_minio(
        [
            {
                "source_path": RNE_DB_TMP_FOLDER,
                "source_name": f"rne_{start_date}.db",
                "dest_path": RNE_MINIO_DATA_PATH,
                "dest_name": f"rne_{last_date_processed}.db",
            }
        ]
    )


def upload_latest_date_rne_minio(ti):
    """Start date saved is the next day"""
    last_date_processed = ti.xcom_pull(
        key="last_date_processed", task_ids="process_flux_json_files"
    )
    last_date_processed = datetime.strptime(last_date_processed, "%Y-%m-%d")
    latest_date = (last_date_processed + timedelta(days=1)).strftime("%Y-%m-%d")
    data = {}
    data["latest_date"] = latest_date
    with open(RNE_DB_TMP_FOLDER + "latest_rne_date.json", "w") as write_file:
        json.dump(data, write_file)

    send_to_minio(
        [
            {
                "source_path": RNE_DB_TMP_FOLDER,
                "source_name": RNE_LATEST_DATE_FILE,
                "dest_path": RNE_MINIO_DATA_PATH,
                "dest_name": RNE_LATEST_DATE_FILE,
            }
        ]
    )
    ti.xcom_push(key="latest_date", value=latest_date)


def notification_tchap(ti):
    start_date = ti.xcom_pull(key="start_date", task_ids="get_start_date")
    last_date_processed = ti.xcom_pull(
        key="last_date_processed", task_ids="process_flux_json_files"
    )
    send_message(
        f"Données RNE traitées de {start_date} à {last_date_processed} "
        "et stockées sur la base de données sur Minio "
        f"- Bucket {MINIO_BUCKET}",
    )
