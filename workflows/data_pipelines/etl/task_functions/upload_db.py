from datetime import datetime

from dag_datalake_sirene.config import (
    SIRENE_MINIO_DATA_PATH,
    AIRFLOW_ETL_DATA_DIR,
)
from dag_datalake_sirene.helpers.minio_helpers import minio_client

current_date = datetime.now().date()


def send_to_minio(list_files):
    minio_client.send_files(
        list_files=list_files,
    )


def upload_db_to_minio(**kwargs):
    send_to_minio(
        [
            {
                "source_path": AIRFLOW_ETL_DATA_DIR,
                "source_name": "sirene.db",
                "dest_path": SIRENE_MINIO_DATA_PATH,
                "dest_name": f"test_sirene_{current_date}.db",
            }
        ]
    )
