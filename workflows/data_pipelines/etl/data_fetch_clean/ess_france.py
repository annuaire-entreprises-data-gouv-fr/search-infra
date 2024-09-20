import pandas as pd
from dag_datalake_sirene.helpers.utils import get_last_modified
from dag_datalake_sirene.config import URL_MINIO_ESS_FRANCE


def preprocess_ess_france_data(data_dir, **kwargs):
    df_ess = pd.read_csv(
        URL_MINIO_ESS_FRANCE,
        dtype={
            "siren": "object",
            "est_ess_france": "bool",
        },
    )
    # Get the last modified date of the CSV file
    last_modified = get_last_modified(URL_MINIO_ESS_FRANCE)
    kwargs["ti"].xcom_push(key="ess_france_last_modified", value=last_modified)
    return df_ess
