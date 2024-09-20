import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_FINESS
from dag_datalake_sirene.helpers.utils import get_last_modified


def preprocess_finess_data(data_dir, **kwargs):
    df_finess = pd.read_csv(URL_MINIO_FINESS, dtype=str)
    # Get the last modified date of the CSV file
    last_modified = get_last_modified(URL_MINIO_FINESS)
    kwargs["ti"].xcom_push(key="finess_last_modified", value=last_modified)

    return df_finess
