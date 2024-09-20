import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_RGE
from dag_datalake_sirene.helpers.utils import get_last_modified


def preprocess_rge_data(data_dir, **kwargs):
    df_rge = pd.read_csv(URL_MINIO_RGE, dtype=str)
    # Get the last modified date of the CSV file
    last_modified = get_last_modified(URL_MINIO_RGE)
    kwargs["ti"].xcom_push(key="rge_last_modified", value=last_modified)

    return df_rge
