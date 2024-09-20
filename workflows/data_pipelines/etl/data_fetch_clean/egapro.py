import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_EGAPRO
from dag_datalake_sirene.helpers.utils import get_last_modified


def preprocess_egapro_data(data_dir, **kwargs):
    df_egapro = pd.read_csv(
        URL_MINIO_EGAPRO, dtype={"siren": "object", "egapro_renseignee": "bool"}
    )
    # Get the last modified date of the CSV file
    last_modified = get_last_modified(URL_MINIO_EGAPRO)
    kwargs["ti"].xcom_push(key="egapro_last_modified", value=last_modified)
    return df_egapro
