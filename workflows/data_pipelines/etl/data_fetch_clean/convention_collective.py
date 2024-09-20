import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_CONVENTION_COLLECTIVE
from dag_datalake_sirene.helpers.utils import get_last_modified


def preprocess_convcollective_data(data_dir, **kwargs):
    df_cc = pd.read_csv(
        URL_MINIO_CONVENTION_COLLECTIVE,
        dtype=str,
    )
    # Get the last modified date of the CSV file
    last_modified = get_last_modified(URL_MINIO_CONVENTION_COLLECTIVE)
    kwargs["ti"].xcom_push(
        key="convention_collective_last_modified", value=last_modified
    )
    return df_cc
