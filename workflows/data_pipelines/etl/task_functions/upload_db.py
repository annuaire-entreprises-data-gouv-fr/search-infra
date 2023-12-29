from datetime import datetime

from dag_datalake_sirene.config import (
    MINIO_BUCKET,
    MINIO_URL,
    MINIO_USER,
    MINIO_PASSWORD,
    SIRENE_MINIO_DATA_PATH,
    AIRFLOW_ETL_DATA_DIR,
)
from dag_datalake_sirene.helpers.minio_helpers import send_files

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
                "source_path": AIRFLOW_ETL_DATA_DIR,
                "source_name": "sirene.db",
                "dest_path": SIRENE_MINIO_DATA_PATH,
                "dest_name": f"sirene_{current_date}.db",
            }
        ]
    )
