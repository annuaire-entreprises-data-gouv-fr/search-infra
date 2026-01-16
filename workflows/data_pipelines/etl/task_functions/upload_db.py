import gzip
import logging
import os
import shutil
from datetime import datetime

from airflow.decorators import task

from data_pipelines_annuaire.config import (
    AIRFLOW_ETL_DATA_DIR,
    SIRENE_OBJECT_STORAGE_DATA_PATH,
)
from data_pipelines_annuaire.helpers.object_storage import ObjectStorageClient

current_date = datetime.now().date()


def send_to_object_storage(list_files):
    ObjectStorageClient().send_files(
        list_files=list_files,
    )


@task
def upload_db_to_object_storage(**kwargs):
    # Zip database
    database_file_path = os.path.join(AIRFLOW_ETL_DATA_DIR, "sirene.db")

    with open(database_file_path, "rb") as f_in:
        with gzip.open(f"{database_file_path}.gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    send_to_object_storage(
        [
            {
                "source_path": AIRFLOW_ETL_DATA_DIR,
                "source_name": "sirene.db.gz",
                "dest_path": SIRENE_OBJECT_STORAGE_DATA_PATH,
                "dest_name": f"sirene_{current_date}.db.gz",
            }
        ]
    )
    # Delete the local database file after uploading to object storage
    if os.path.exists(database_file_path):
        os.remove(database_file_path)
        os.remove(f"{database_file_path}.gz")
        logging.info(f"Database deleted! {database_file_path}")
    else:
        logging.warning(f"Warning: Database file '{database_file_path}' not found.")
