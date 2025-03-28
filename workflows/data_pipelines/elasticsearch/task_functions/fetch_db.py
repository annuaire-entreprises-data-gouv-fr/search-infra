import gzip
import logging
import os
import re
import shutil
from datetime import datetime

from dag_datalake_sirene.config import (
    AIRFLOW_ELK_DATA_DIR,
    SIRENE_MINIO_DATA_PATH,
)
from dag_datalake_sirene.helpers.minio_helpers import MinIOClient

current_date = datetime.now().date()


def get_latest_database(**kwargs):
    minio_client = MinIOClient()
    database_files = minio_client.get_files_from_prefix(
        prefix=SIRENE_MINIO_DATA_PATH,
    )

    if not database_files:
        raise Exception(f"No database files were found in : {SIRENE_MINIO_DATA_PATH}")

    # Extract dates from the db file names and sort them
    dates = sorted(re.findall(r"sirene_(\d{4}-\d{2}-\d{2})", " ".join(database_files)))

    if dates:
        last_date = dates[-1]
        logging.info(f"***** Last database saved: {last_date}")
        minio_client.get_files(
            list_files=[
                {
                    "source_path": SIRENE_MINIO_DATA_PATH,
                    "source_name": f"sirene_{last_date}.db.gz",
                    "dest_path": AIRFLOW_ELK_DATA_DIR,
                    "dest_name": "sirene.db.gz",
                }
            ],
        )
        # Unzip database file
        db_path = f"{AIRFLOW_ELK_DATA_DIR}sirene.db"
        with gzip.open(f"{db_path}.gz", "rb") as f_in:
            with open(db_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(f"{db_path}.gz")

    else:
        raise Exception(
            f"No dates in database files were found : {SIRENE_MINIO_DATA_PATH}"
        )
