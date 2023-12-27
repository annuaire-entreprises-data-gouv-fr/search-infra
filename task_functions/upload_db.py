from datetime import datetime
import re
import logging

from dag_datalake_sirene.config import (
    MINIO_BUCKET,
    MINIO_URL,
    MINIO_USER,
    MINIO_PASSWORD,
    SIRENE_MINIO_DATA_PATH,
    SIRENE_DATABASE_LOCATION,
    AIRFLOW_INDEXING_DATA_DIR,
)
from dag_datalake_sirene.utils.minio_helpers import (
    get_files_from_prefix,
    get_files,
    send_files,
)

current_date = datetime.now().date()


def send_to_minio(list_files):
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET,
        MINIO_USER=MINIO_USER,
        MINIO_PASSWORD=MINIO_PASSWORD,
        list_files=list_files,
    )


def upload_db_to_minio(**kwargs):
    send_to_minio(
        [
            {
                "source_path": SIRENE_DATABASE_LOCATION,
                "source_name": "",
                "dest_path": SIRENE_MINIO_DATA_PATH,
                "dest_name": f"sirene_{current_date}.db",
            }
        ]
    )


def get_latest_database(**kwargs):
    database_files = get_files_from_prefix(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET,
        MINIO_USER=MINIO_USER,
        MINIO_PASSWORD=MINIO_PASSWORD,
        prefix=SIRENE_MINIO_DATA_PATH,
    )

    if not database_files:
        raise Exception(f"No database files were found in : {SIRENE_MINIO_DATA_PATH}")

    # Extract dates from the db file names and sort them
    dates = sorted(re.findall(r"sirene_(\d{4}-\d{2}-\d{2})", " ".join(database_files)))

    if dates:
        last_date = dates[-1]
        logging.info(f"***** Last databse saved: {last_date}")
        get_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET,
            MINIO_USER=MINIO_USER,
            MINIO_PASSWORD=MINIO_PASSWORD,
            list_files=[
                {
                    "source_path": SIRENE_MINIO_DATA_PATH,
                    "source_name": f"sirene_{last_date}.db",
                    "dest_path": AIRFLOW_INDEXING_DATA_DIR,
                    "dest_name": "sirene.db",
                }
            ],
        )

    else:
        raise Exception(
            f"No dates in databse files were found : {SIRENE_MINIO_DATA_PATH}"
        )
