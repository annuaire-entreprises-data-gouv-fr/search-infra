import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_ENTREPRENEUR_SPECTACLE
from dag_datalake_sirene.helpers.utils import get_last_modified


def preprocess_spectacle_data(data_dir, **kwargs):
    df_spectacle = pd.read_csv(
        URL_MINIO_ENTREPRENEUR_SPECTACLE,
        dtype={
            "siren": "object",
            "statut_entrepreneur_spectacle": "object",
            "est_entrepreneur_spectacle": "bool",
        },
    )
    # Get the last modified date of the CSV file
    last_modified = get_last_modified(URL_MINIO_ENTREPRENEUR_SPECTACLE)
    kwargs["ti"].xcom_push(
        key="entrepreneur_spectacle_last_modified", value=last_modified
    )

    return df_spectacle
