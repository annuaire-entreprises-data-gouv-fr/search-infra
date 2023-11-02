from minio import Minio
from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_USER,
    MINIO_PASSWORD,
)

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
