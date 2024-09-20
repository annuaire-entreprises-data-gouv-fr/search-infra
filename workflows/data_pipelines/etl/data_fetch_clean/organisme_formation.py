import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_ORGANISME_FORMATION
from dag_datalake_sirene.helpers.utils import get_last_modified


def preprocess_organisme_formation_data(data_dir, **kwargs):
    df_organisme_formation = pd.read_csv(
        URL_MINIO_ORGANISME_FORMATION,
        dtype={
            "siren": "object",
            "liste_id_organisme_formation": "object",
            "est_qualiopi": "bool",
        },
    )
    # Get the last modified date of the CSV file
    last_modified = get_last_modified(URL_MINIO_ORGANISME_FORMATION)
    kwargs["ti"].xcom_push(key="organisme_formation_last_modified", value=last_modified)
    return df_organisme_formation
