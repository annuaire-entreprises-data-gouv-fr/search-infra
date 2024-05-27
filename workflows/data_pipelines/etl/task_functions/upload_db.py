from datetime import datetime
import gzip
import logging
import shutil
import os

from dag_datalake_sirene.config import (
    SIRENE_MINIO_DATA_PATH,
    AIRFLOW_ETL_DATA_DIR,
)
from dag_datalake_sirene.helpers.s3_helpers import s3_client

current_date = datetime.now().date()


def send_to_minio(list_files):
    s3_client.send_files(
        list_files=list_files,
    )


def upload_db_to_minio(**kwargs):
    # Zip database
    database_file_path = os.path.join(AIRFLOW_ETL_DATA_DIR, "sirene.db")

    with open(database_file_path, "rb") as f_in:
        with gzip.open(f"{database_file_path}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    send_to_minio(
        [
            {
                "source_path": AIRFLOW_ETL_DATA_DIR,
                "source_name": "sirene.db.gz",
                "dest_path": SIRENE_MINIO_DATA_PATH,
                "dest_name": f"sirene_{current_date}.db.gz",
            }
        ]
    )
    # Delete the local database file after uploading to Minio
    if os.path.exists(database_file_path):
        os.remove(database_file_path)
        os.remove(f"{database_file_path}.gz")
        logging.info(f"Database deleted! {database_file_path}")
    else:
        logging.warning(f"Warning: Database file '{database_file_path}' not found.")
